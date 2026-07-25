import random
import unittest

from vibration import EventBuffer, VibrationDetector, format_ambient


SAMPLE_HZ = 100.0
SIGMA = 0.02       # per-axis noise, m/s^2 (the real MSA311 measures ~0.021)
GRAVITY = 9.806


def detector(**overrides):
    # Short windows and a fast baseline sampler keep the tests quick; the shape of
    # the maths is identical to the 125 Hz production settings.
    settings = {
        "sample_hz": SAMPLE_HZ,
        "windows": ((0.08, 5.3), (0.16, 3.1)),
        "baseline_window_seconds": 10.0,
        "baseline_sample_hz": 20.0,
        "warmup_seconds": 2.0,
        "tail_seconds": 0.2,
        "baseline_tau_seconds": 2.0,
    }
    settings.update(overrides)
    return VibrationDetector(**settings)


class Stream:
    """Feed a detector a continuous, reproducible sample stream."""

    def __init__(self, subject, seed=1):
        self.subject = subject
        self.random = random.Random(seed)
        self.now = 0.0
        self.period = 1.0 / SAMPLE_HZ

    def run(self, seconds, sigma=SIGMA):
        results = []
        end = self.now + seconds
        while self.now < end:
            noise = self.random.gauss
            sample = (noise(0.0, sigma), noise(0.0, sigma), GRAVITY + noise(0.0, sigma))
            results.append(self.subject.process(sample, self.now))
            self.now += self.period
        return results

    def displace(self, magnitude):
        """One single sample displaced by `magnitude` m/s^2 away from rest."""
        result = self.subject.process((magnitude, 0.0, GRAVITY), self.now)
        self.now += self.period
        return result


class VibrationDetectorTest(unittest.TestCase):
    def test_warmup_is_silent_and_arms_exactly_once(self):
        subject = detector()
        results = Stream(subject).run(4.0)

        armed = [index for index, r in enumerate(results) if r.just_calibrated]
        self.assertEqual(len(armed), 1)
        self.assertFalse(any(r.ready for r in results[: armed[0] + 1]))
        self.assertFalse(any(r.active for r in results[: armed[0] + 1]))
        self.assertTrue(results[-1].ready)
        self.assertTrue(subject.ready)
        self.assertAlmostEqual(subject.noise_sigma, SIGMA, delta=0.3 * SIGMA)

    def test_quiet_noise_produces_no_events(self):
        subject = detector()
        stream = Stream(subject)
        stream.run(4.0)

        results = stream.run(120.0)

        self.assertEqual(sum(r.event_started for r in results), 0)
        self.assertEqual(sum(r.active for r in results), 0)

    def test_single_sample_outlier_is_not_an_event(self):
        """A lone 6-sigma sample is noise, not vibration.

        The previous per-sample design tripped at 4.96 sigma, which at 125 Hz is
        crossed a couple of hundred times a day; 57% of the events it recorded in
        the field contained exactly one sample above the noise.
        """
        subject = detector()
        stream = Stream(subject)
        stream.run(4.0)

        spike = stream.displace(6.0 * SIGMA)
        after = stream.run(1.0)

        self.assertFalse(spike.over_threshold)
        self.assertFalse(spike.event_started)
        self.assertEqual(sum(r.event_started for r in after), 0)

    def test_sustained_low_amplitude_vibration_is_an_event(self):
        """2.5x the noise amplitude, sustained, is an event.

        The old 4.96-sigma per-sample line would have ignored this entirely, so the
        windowed test is both quieter and more sensitive.
        """
        subject = detector()
        stream = Stream(subject)
        stream.run(4.0)

        results = stream.run(0.5, sigma=2.5 * SIGMA)

        self.assertTrue(any(r.event_started for r in results))
        self.assertTrue(any(r.active for r in results))

    def test_tail_extends_from_last_trigger_then_event_ends(self):
        subject = detector()
        stream = Stream(subject)
        stream.run(4.0)

        during = stream.run(0.5, sigma=5.0 * SIGMA)
        after = stream.run(3.0)

        self.assertTrue(any(r.event_started for r in during))
        self.assertEqual(sum(r.event_started for r in after), 0)
        self.assertTrue(any(r.event_ended for r in after))
        self.assertFalse(after[-1].active)

    def test_recovers_when_ambient_noise_rises_permanently(self):
        """The noise reference must keep adapting while an event is in progress.

        Gating the reference on "not currently in an event" makes it self-confirming:
        once triggering is continuous it sees nothing, so a threshold left below the
        ambient level is a stable fixed point and the device reports one unbroken
        event until it restarts.
        """
        subject = detector()
        stream = Stream(subject)
        stream.run(4.0)
        quiet_sigma = subject.noise_sigma

        onset = stream.run(2.0, sigma=5.0 * SIGMA)
        settled = stream.run(60.0, sigma=5.0 * SIGMA)

        self.assertTrue(any(r.event_started for r in onset))
        self.assertGreater(subject.noise_sigma, 3.0 * quiet_sigma)
        self.assertFalse(settled[-1].active)
        tail = settled[-500:]
        self.assertLess(sum(r.active for r in tail) / len(tail), 0.05)

    def test_resting_vector_follows_a_new_orientation(self):
        subject = detector(baseline_tau_seconds=0.1)
        stream = Stream(subject)
        stream.run(4.0)

        for _ in range(100):
            stream.displace(1.0)

        self.assertIsNotNone(subject.resting)
        self.assertGreater(subject.resting[0], 0.9)

    def test_sensitivity_improves_with_window_length(self):
        subject = detector()
        Stream(subject).run(4.0)

        windows = subject.sensitivity()

        self.assertEqual([w for w, _ in windows], [0.08, 0.16])
        self.assertGreater(windows[0][1], windows[1][1])
        # Every window trips well below the 4.96-sigma per-sample line it replaces.
        for _, amplitude in windows:
            self.assertLess(amplitude, 4.963 * SIGMA)

    def test_ambient_report_measures_the_distribution_and_resets_counters(self):
        subject = detector()
        stream = Stream(subject)
        stream.run(4.0)
        stream.run(0.5, sigma=5.0 * SIGMA)
        stream.run(30.0)

        report = subject.ambient_report(stream.now)

        self.assertGreater(report.seconds, 30.0)
        self.assertEqual(report.events, 1)
        self.assertGreater(report.duty_cycle, 0.0)
        for window in report.windows:
            # The median of "energy over its own median" must be about 1.
            self.assertAlmostEqual(window.quantiles[0.5], 1.0, delta=0.15)
            self.assertGreater(window.observations, 0)
            self.assertAlmostEqual(window.sigma, SIGMA, delta=0.3 * SIGMA)
            # The burst is in the histogram, so the peak sits above the threshold.
            self.assertGreater(window.peak_ratio, window.threshold_ratio)

        # Counters cover the interval, so a fresh report starts from zero...
        again = subject.ambient_report(stream.now)
        self.assertEqual(again.events, 0)
        # ...while the histograms are cumulative and keep every observation.
        self.assertEqual(
            [w.observations for w in again.windows],
            [w.observations for w in report.windows],
        )

    def test_ambient_report_declines_to_extrapolate_rare_rates(self):
        subject = detector()
        stream = Stream(subject)
        stream.run(4.0)

        report = subject.ambient_report(stream.now)

        # Seconds of data cannot say how often a once-per-day level is reached.
        for window in report.windows:
            self.assertIsNone(window.rate_ratios[5.0])
            self.assertIsNone(window.quantiles[0.9999])

    def test_format_ambient_renders_one_line_per_window(self):
        subject = detector()
        stream = Stream(subject)
        stream.run(4.0)

        lines = format_ambient(subject.ambient_report(stream.now))

        self.assertEqual(len(lines), 1 + len(subject.sensitivity()))
        self.assertIn("Vibration ambient over", lines[0])
        self.assertIn("80ms", lines[1])
        self.assertIn("threshold=5.30", lines[1])
        self.assertIn("p50=", lines[1])
        self.assertIn("n/a", lines[1])  # a few seconds cannot resolve 5/day

    def test_rejects_invalid_arguments(self):
        with self.assertRaises(ValueError):
            detector(sample_hz=0)
        with self.assertRaises(ValueError):
            detector(tail_seconds=-1.0)
        with self.assertRaises(ValueError):
            detector(windows=())
        with self.assertRaises(ValueError):
            detector(windows=((0.1, -0.2),))


class EventBufferTest(unittest.TestCase):
    def test_adds_pretrigger_context_and_zero_boundary(self):
        buffer = EventBuffer[str](pre_trigger_samples=3)
        buffer.append(0.01, "t1")
        buffer.append(0.01, "t2")
        buffer.append(0.01, "t3")

        self.assertEqual(
            buffer.flush_with_zero(),
            [(0.0, "t1"), (0.01, "t2"), (0.01, "t3")],
        )
        self.assertEqual(buffer.flush_with_zero(), [])

    def test_rejects_non_positive_capacity(self):
        with self.assertRaises(ValueError):
            EventBuffer(0)


if __name__ == "__main__":
    unittest.main()
