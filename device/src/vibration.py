"""Stateful vibration detection independent of the MSA311 hardware driver."""

from dataclasses import dataclass
from collections import deque
import math
import statistics
from typing import Generic, TypeVar


TimestampT = TypeVar("TimestampT")

SECONDS_PER_DAY = 86400.0

# A median needs a few observations before it means anything. Only reached with an
# unusually short warmup; the production warmup collects hundreds.
_MIN_BASELINE_SAMPLES = 4

# Ambient histogram: log-spaced bins of window energy relative to the running
# median, from a tenth of normal to a thousand times it. 40 bins per decade puts
# neighbouring bins 12% apart in energy, which is finer than any threshold choice
# needs, in a few hundred bytes.
_HISTOGRAM_LOW = -1.0
_HISTOGRAM_HIGH = 3.0
_HISTOGRAM_BINS_PER_DECADE = 40

# (window seconds, energy ratio above the running median that trips it).
#
# Two separate constraints, measured against 133k real quiet windows (the
# pre-trigger buffers of 788 recorded events on this device):
#
#  - Sensor noise must never trip it. Robust spread of the quiet window energy is
#    1.4-1.8x what a white-noise model predicts (the noise is mildly correlated),
#    which puts the noise ceiling at ratio 2.6 (128 ms) and 2.0 (512 ms) for one
#    crossing per hundred days. Anything above that is noise-proof.
#
#  - Ambient movement is a continuum, and above the noise ceiling it is what sets
#    the rate: ratio 2.6 reports ~6700 faint tremors a day, 3.5 ~675, 4.5 ~67,
#    5.3 ~20, 6.4 ~5. None of these are noise -- this is a significance choice,
#    not a false-alarm budget, so the event rate is a property of the room and not
#    of the detector. An empty flat should produce none of them.
#
# The defaults report roughly the strongest twenty movements a day, about twice the
# energy the noise could ever reach. Turn `margin` up to hear only firm movement,
# down to hear everything that is not noise.
DEFAULT_WINDOWS = (
    (0.128, 5.3),
    (0.512, 3.1),
)


@dataclass(frozen=True)
class Detection:
    """Result of processing one accelerometer sample."""

    value: float
    ready: bool
    just_calibrated: bool = False
    over_threshold: bool = False
    active: bool = False
    event_started: bool = False
    event_ended: bool = False


class EventBuffer(Generic[TimestampT]):
    """Hold recent idle samples for pre-trigger event context."""

    def __init__(self, pre_trigger_samples: int) -> None:
        if pre_trigger_samples <= 0:
            raise ValueError("pre_trigger_samples must be positive")
        self._samples: deque[tuple[float, TimestampT]] = deque(
            maxlen=pre_trigger_samples
        )

    def append(self, value: float, timestamp: TimestampT) -> None:
        self._samples.append((value, timestamp))

    def flush_with_zero(self) -> list[tuple[float, TimestampT]]:
        """Return buffered context with a leading zero, then empty the buffer."""
        if not self._samples:
            return []
        output = [(0.0, self._samples[0][1]), *list(self._samples)[1:]]
        self._samples.clear()
        return output

    def clear(self) -> None:
        self._samples.clear()


@dataclass(frozen=True)
class AmbientWindow:
    """Measured ambient energy distribution for one window length."""

    seconds: float
    observations: int
    sigma: float
    threshold_ratio: float
    peak_ratio: float
    quantiles: dict[float, float | None]
    rate_ratios: dict[float, float | None]


@dataclass(frozen=True)
class AmbientReport:
    """What the sensor has actually been seeing, for threshold calibration.

    The event counters cover the interval since the previous report, so a sequence
    of reports is a diurnal activity profile. The energy histograms are cumulative
    since startup instead, because resolving how often ambient movement reaches a
    given level needs every observation it can get.
    """

    seconds: float
    events: int
    duty_cycle: float
    windows: list[AmbientWindow]


def format_ambient(report: AmbientReport) -> list[str]:
    """Render an AmbientReport as log lines.

    `peak` and the percentiles are window energy as a multiple of its own running
    median, the same units as `threshold`, so they read against each other
    directly. The trailing figures are what the threshold would have to be to fire
    at a given rate, measured rather than extrapolated; "n/a" means not enough
    observations have accumulated to see that far into the tail.
    """
    def ratio(value: float | None) -> str:
        return "n/a" if value is None else f"{value:.2f}"

    lines = [
        f"Vibration ambient over {report.seconds:.0f}s: {report.events} events,"
        f" {100 * report.duty_cycle:.3f}% of the time in an event"
    ]
    for window in report.windows:
        quantiles = " ".join(
            f"p{100 * q:g}={ratio(window.quantiles[q])}"
            for q in sorted(window.quantiles)
        )
        rates = " ".join(
            f"{rate:g}/day={ratio(window.rate_ratios[rate])}"
            for rate in sorted(window.rate_ratios, reverse=True)
        )
        lines.append(
            f"  {window.seconds * 1000:.0f}ms n={window.observations}"
            f" sigma={window.sigma:.5f} threshold={window.threshold_ratio:.2f}"
            f" peak={window.peak_ratio:.2f} | {quantiles} | would need {rates}"
        )
    return lines


class _EnergyHistogram:
    """Counts of window energy relative to the running median, in log-spaced bins."""

    def __init__(self) -> None:
        span = _HISTOGRAM_HIGH - _HISTOGRAM_LOW
        self._bins = [0] * round(span * _HISTOGRAM_BINS_PER_DECADE)
        self._total = 0
        self._peak = 0.0

    def add(self, ratio: float) -> None:
        if ratio <= 0.0:
            return
        self._peak = max(self._peak, ratio)
        index = int(
            (math.log10(ratio) - _HISTOGRAM_LOW) * _HISTOGRAM_BINS_PER_DECADE
        )
        self._bins[min(max(index, 0), len(self._bins) - 1)] += 1
        self._total += 1

    @property
    def observations(self) -> int:
        return self._total

    @property
    def peak(self) -> float:
        return self._peak

    def _upper_edge(self, index: int) -> float:
        return 10.0 ** (_HISTOGRAM_LOW + (index + 1) / _HISTOGRAM_BINS_PER_DECADE)

    def quantile(self, q: float) -> float | None:
        # Refuse to name a quantile no observation has reached yet: a p99.99 drawn
        # from thirty samples is just the largest of the thirty wearing a label.
        if min(q, 1.0 - q) * self._total < 1.0:
            return None
        target = q * self._total
        cumulative = 0
        for index, count in enumerate(self._bins):
            cumulative += count
            if cumulative >= target:
                return self._upper_edge(index)
        return self._upper_edge(len(self._bins) - 1)

    def ratio_for_rate(self, events_per_day: float, tests_per_day: float) -> float | None:
        """The ratio a threshold would need to fire `events_per_day` times.

        None until enough observations have accumulated to see that far into the
        tail, rather than inventing a number from one lucky sample.
        """
        return self.quantile(1.0 - events_per_day / tests_per_day)


def _chi_square_median(degrees_of_freedom: int) -> float:
    """Wilson-Hilferty median of a chi-square distribution.

    Used only to report an interpretable per-axis sigma: under quiet conditions a
    window's energy is sigma^2 times chi-square with three degrees of freedom per
    sample. Accurate to better than 0.01% at the sizes used here, and the reported
    sigma agrees to within 0.5% across every window length on real data.
    """
    return degrees_of_freedom * (1.0 - 2.0 / (9.0 * degrees_of_freedom)) ** 3


class _Scale:
    """One window length of the energy detector.

    Holds a sliding sum of squared deviations (the window's energy) plus a long
    ring of periodically sampled energies whose median is the quiet reference.
    """

    def __init__(self, samples: int, ratio: float, baseline_capacity: int) -> None:
        self.samples = samples
        self.ratio = ratio
        self._window: deque[float] = deque(maxlen=samples)
        self._energy = 0.0
        self._baseline: deque[float] = deque(maxlen=baseline_capacity)
        self._median: float | None = None
        self.histogram = _EnergyHistogram()
        self._since_observation = 0

    def push(self, square: float) -> None:
        if len(self._window) == self.samples:
            self._energy -= self._window[0]  # about to be evicted by append
        self._window.append(square)
        self._energy += square

        # Record one histogram observation per full window turnover. Sampling every
        # sample instead would just count the same overlapping window repeatedly and
        # overstate how much independent evidence the tail rests on.
        self._since_observation += 1
        if self._since_observation >= self.samples and self.full and self._median:
            self._since_observation = 0
            self.histogram.add(self._energy / self._median)

    @property
    def full(self) -> bool:
        return len(self._window) == self.samples

    @property
    def energy(self) -> float:
        return self._energy

    def observe_baseline(self) -> None:
        """Record the current energy as a quiet-reference observation."""
        if not self.full:
            return
        self._baseline.append(self._energy)
        self._median = statistics.median(self._baseline)

    @property
    def armed(self) -> bool:
        return self.full and len(self._baseline) >= _MIN_BASELINE_SAMPLES

    @property
    def median_energy(self) -> float | None:
        return self._median

    @property
    def threshold(self) -> float | None:
        return None if self._median is None else self._median * self.ratio

    @property
    def over_threshold(self) -> bool:
        threshold = self.threshold
        return threshold is not None and self.full and self._energy > threshold


class VibrationDetector:
    """Detect a sustained rise in movement energy above the ambient noise floor.

    Two properties matter, and the obvious per-sample design gets both wrong.

    *Energy, not outliers.* A threshold on a single sample is tested once per
    sample - 10.8 million times a day at 125 Hz - so even a genuine 5-sigma line is
    crossed a couple of hundred times a day by noise alone. Measured on this
    device: 309 such events a day, containing one or two samples above the noise
    and spread evenly across every hour including the ones the occupant was asleep
    for, which is a noise floor rather than movement. Vibration instead raises the
    energy of a whole window, so a window is tested once per window rather than
    once per sample. That crushes the noise variance, which buys both a much lower
    threshold and far fewer crossings: the defaults trip at 1.8-2.3x the noise
    amplitude where the old design needed 4.96x.

    *Robust, never gated.* The quiet reference is the median of each window's
    energy over several minutes, fed by every sample. Events are brief relative to
    that window, so they cannot move a median. Excluding them instead - the
    intuitive way to keep events out of the noise model - makes the estimate
    self-confirming: it only ever sees samples below its own threshold, so a
    threshold left below the ambient level is a stable fixed point. Past about a
    2x step in ambient level the old design received zero samples and never
    recovered until the process restarted.
    """

    def __init__(
        self,
        *,
        sample_hz: float,
        windows: tuple[tuple[float, float], ...] = DEFAULT_WINDOWS,
        baseline_window_seconds: float = 300.0,
        baseline_sample_hz: float = 2.0,
        margin: float = 1.0,
        baseline_tau_seconds: float = 2.0,
        tail_seconds: float = 1.0,
        warmup_seconds: float = 60.0,
    ) -> None:
        if min(
            sample_hz,
            baseline_window_seconds,
            baseline_sample_hz,
            margin,
            baseline_tau_seconds,
            tail_seconds,
            warmup_seconds,
        ) <= 0:
            raise ValueError("detector rates, durations and margin must be positive")
        if not windows or min(min(w) for w in windows) <= 0:
            raise ValueError("windows must be a non-empty tuple of positive pairs")

        self.sample_hz = sample_hz
        self.baseline_tau_seconds = baseline_tau_seconds
        self.tail_seconds = tail_seconds
        self.warmup_seconds = warmup_seconds

        capacity = max(
            _MIN_BASELINE_SAMPLES, round(baseline_window_seconds * baseline_sample_hz)
        )
        self._baseline_period = 1.0 / baseline_sample_hz
        self._scales = [
            _Scale(max(2, round(seconds * sample_hz)), ratio * margin, capacity)
            for seconds, ratio in windows
        ]

        self._resting: tuple[float, float, float] | None = None
        self._started_at: float | None = None
        self._last_sample_at: float | None = None
        self._next_baseline_at = 0.0
        self._last_trigger_at: float | None = None
        self._ready = False
        self._active = False

        self._report_at: float | None = None
        self._report_events = 0
        self._report_active = 0
        self._report_samples = 0

    @property
    def ready(self) -> bool:
        return self._ready

    @property
    def resting(self) -> tuple[float, float, float] | None:
        """The slowly tracked resting XYZ vector (gravity plus orientation)."""
        return self._resting

    @property
    def noise_sigma(self) -> float:
        """Per-axis noise standard deviation implied by the longest window."""
        scale = max(self._scales, key=lambda s: s.samples)
        median = scale.median_energy
        if not median:
            return 0.0
        return math.sqrt(median / _chi_square_median(3 * scale.samples))

    def ambient_report(self, now: float) -> AmbientReport:
        """Summarise what the sensor has been seeing, and reset the event counters.

        The thresholds in DEFAULT_WINDOWS were extrapolated from a biased sample:
        the only ambient data available was the second preceding each recorded
        trigger, which is both a tiny fraction of the day and selected for being
        eventful. This report measures the real distribution over the whole day, so
        the ratios can be set from it instead.
        """
        windows = []
        for scale in self._scales:
            seconds = scale.samples / self.sample_hz
            tests_per_day = self.sample_hz * SECONDS_PER_DAY / scale.samples
            median = scale.median_energy
            sigma = (
                0.0 if not median
                else math.sqrt(median / _chi_square_median(3 * scale.samples))
            )
            windows.append(AmbientWindow(
                seconds=seconds,
                observations=scale.histogram.observations,
                sigma=sigma,
                threshold_ratio=scale.ratio,
                peak_ratio=scale.histogram.peak,
                quantiles={
                    q: scale.histogram.quantile(q)
                    for q in (0.5, 0.99, 0.999, 0.9999)
                },
                rate_ratios={
                    rate: scale.histogram.ratio_for_rate(rate, tests_per_day)
                    for rate in (100.0, 20.0, 5.0)
                },
            ))

        elapsed = 0.0 if self._report_at is None else now - self._report_at
        report = AmbientReport(
            seconds=elapsed,
            events=self._report_events,
            duty_cycle=self._report_active / max(self._report_samples, 1),
            windows=windows,
        )
        self._report_at = now
        self._report_events = 0
        self._report_active = 0
        self._report_samples = 0
        return report

    def sensitivity(self) -> list[tuple[float, float]]:
        """Per window: (seconds, per-axis amplitude in m/s^2 that trips it)."""
        out = []
        for scale in self._scales:
            threshold = scale.threshold
            amplitude = (
                0.0 if threshold is None else math.sqrt(threshold / (3 * scale.samples))
            )
            out.append((scale.samples / self.sample_hz, amplitude))
        return out

    def process(self, sample: tuple[float, float, float], now: float) -> Detection:
        x, y, z = sample

        if self._resting is None:
            # Seed the resting vector from the first sample; the EMA below removes
            # the one-sample seeding error within a few time constants, long before
            # the warmup ends.
            self._resting = (x, y, z)
            self._started_at = now
            self._last_sample_at = now
            self._next_baseline_at = now
            return Detection(value=0.0, ready=False)

        assert self._started_at is not None
        assert self._last_sample_at is not None
        rx, ry, rz = self._resting
        dx, dy, dz = x - rx, y - ry, z - rz
        value = math.sqrt(dx * dx + dy * dy + dz * dz)

        dt = max(now - self._last_sample_at, 0.0)
        self._last_sample_at = now

        # Every sample feeds every window, event or not. Immunity to events comes
        # from the median, not from leaving them out.
        for scale in self._scales:
            scale.push(value * value)
        if now >= self._next_baseline_at:
            self._next_baseline_at = now + self._baseline_period
            for scale in self._scales:
                scale.observe_baseline()

        # Orientation/drift follows every sample. Oscillating vibration averages out,
        # while a device placed in a new orientation eventually becomes quiet again.
        resting_alpha = 1.0 - math.exp(-dt / self.baseline_tau_seconds)
        self._resting = (
            rx + resting_alpha * dx,
            ry + resting_alpha * dy,
            rz + resting_alpha * dz,
        )

        if not self._ready:
            if now - self._started_at < self.warmup_seconds or not all(
                scale.armed for scale in self._scales
            ):
                return Detection(value=value, ready=False)
            self._ready = True
            self._report_at = now
            return Detection(value=value, ready=False, just_calibrated=True)

        over_threshold = any(scale.over_threshold for scale in self._scales)
        was_active = self._active
        tail_live = (
            was_active
            and self._last_trigger_at is not None
            and now - self._last_trigger_at <= self.tail_seconds
        )

        event_ended = was_active and not tail_live and not over_threshold
        event_started = not was_active and over_threshold
        if over_threshold:
            self._last_trigger_at = now
        self._active = over_threshold or tail_live

        self._report_samples += 1
        self._report_active += self._active
        self._report_events += event_started

        return Detection(
            value=value,
            ready=True,
            over_threshold=over_threshold,
            active=self._active,
            event_started=event_started,
            event_ended=event_ended,
        )
