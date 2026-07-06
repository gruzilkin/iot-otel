"""Stateful vibration detection independent of the MSA311 hardware driver."""

from dataclasses import dataclass
from collections import deque
import math
import statistics
from typing import Generic, TypeVar


TimestampT = TypeVar("TimestampT")


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


class VibrationDetector:
    """Detect unusually large changes from a slowly moving resting vector.

    The detector is silent while collecting its initial calibration samples. After
    calibration, the resting XYZ vector follows every sample, while noise statistics
    only follow non-event samples. Keeping those estimators separate lets a changed
    orientation become the new rest position without teaching real vibration to the
    trigger threshold.
    """

    def __init__(
        self,
        *,
        calibration_seconds: float,
        baseline_tau_seconds: float,
        stats_tau_seconds: float,
        tail_seconds: float,
        sigma_multiplier: float,
    ) -> None:
        if min(
            calibration_seconds,
            baseline_tau_seconds,
            stats_tau_seconds,
            tail_seconds,
            sigma_multiplier,
        ) <= 0:
            raise ValueError("detector durations and sigma multiplier must be positive")

        self.calibration_seconds = calibration_seconds
        self.baseline_tau_seconds = baseline_tau_seconds
        self.stats_tau_seconds = stats_tau_seconds
        self.tail_seconds = tail_seconds
        self.sigma_multiplier = sigma_multiplier

        self._calibration_started: float | None = None
        self._calibration_samples: list[tuple[float, float, float]] = []
        self._baseline: tuple[float, float, float] | None = None
        self._noise_mean = 0.0
        self._noise_variance = 0.0
        self._last_sample_at: float | None = None
        self._last_trigger_at: float | None = None
        self._active = False

    @property
    def ready(self) -> bool:
        return self._baseline is not None

    @property
    def baseline(self) -> tuple[float, float, float] | None:
        return self._baseline

    @property
    def noise_mean(self) -> float:
        return self._noise_mean

    @property
    def noise_sigma(self) -> float:
        return math.sqrt(max(self._noise_variance, 0.0))

    @property
    def threshold(self) -> float:
        return self._noise_mean + self.sigma_multiplier * self.noise_sigma

    def process(self, sample: tuple[float, float, float], now: float) -> Detection:
        x, y, z = sample

        if self._calibration_started is None:
            self._calibration_started = now

        if not self.ready:
            self._calibration_samples.append((x, y, z))
            if now - self._calibration_started < self.calibration_seconds:
                return Detection(value=0.0, ready=False)
            self._finish_calibration(now)
            return Detection(value=0.0, ready=False, just_calibrated=True)

        assert self._baseline is not None
        assert self._last_sample_at is not None
        bx, by, bz = self._baseline
        dx, dy, dz = x - bx, y - by, z - bz
        value = math.sqrt(dx * dx + dy * dy + dz * dz)

        threshold = self.threshold
        over_threshold = value > threshold
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

        dt = max(now - self._last_sample_at, 0.0)
        self._last_sample_at = now

        # Orientation/drift follows every sample. Oscillating vibration averages out,
        # while a device placed in a new orientation eventually becomes quiet again.
        baseline_alpha = 1.0 - math.exp(-dt / self.baseline_tau_seconds)
        self._baseline = (
            bx + baseline_alpha * dx,
            by + baseline_alpha * dy,
            bz + baseline_alpha * dz,
        )

        # Freeze the noise model for a complete event. Adding event samples would widen
        # sigma and allow a sustained real vibration to hide itself from the detector.
        if not self._active and not over_threshold:
            stats_alpha = 1.0 - math.exp(-dt / self.stats_tau_seconds)
            delta = value - self._noise_mean
            self._noise_mean += stats_alpha * delta
            self._noise_variance = (1.0 - stats_alpha) * (
                self._noise_variance + stats_alpha * delta * delta
            )

        return Detection(
            value=value,
            ready=True,
            over_threshold=over_threshold,
            active=self._active,
            event_started=event_started,
            event_ended=event_ended,
        )

    def _finish_calibration(self, now: float) -> None:
        count = len(self._calibration_samples)
        bx = sum(sample[0] for sample in self._calibration_samples) / count
        by = sum(sample[1] for sample in self._calibration_samples) / count
        bz = sum(sample[2] for sample in self._calibration_samples) / count
        self._baseline = (bx, by, bz)

        residuals = [
            math.sqrt((x - bx) ** 2 + (y - by) ** 2 + (z - bz) ** 2)
            for x, y, z in self._calibration_samples
        ]
        self._noise_mean = statistics.fmean(residuals)
        self._noise_variance = statistics.fmean(
            (value - self._noise_mean) ** 2 for value in residuals
        )
        self._calibration_samples.clear()
        self._last_sample_at = now
