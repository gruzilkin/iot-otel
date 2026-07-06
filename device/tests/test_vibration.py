import math
import unittest

from vibration import EventBuffer, VibrationDetector


def detector(**overrides):
    settings = {
        "calibration_seconds": 1.0,
        "baseline_tau_seconds": 2.0,
        "stats_tau_seconds": 60.0,
        "tail_seconds": 1.0,
        "sigma_multiplier": 5.0,
    }
    settings.update(overrides)
    return VibrationDetector(**settings)


def calibrate(subject):
    # Stationary samples with a small, repeatable noise distribution.
    for index in range(11):
        noise = (-0.02, -0.01, 0.0, 0.01, 0.02)[index % 5]
        result = subject.process((noise, 0.0, 9.806), index / 10)
    return result


class VibrationDetectorTest(unittest.TestCase):
    def test_calibration_is_silent_and_builds_five_sigma_threshold(self):
        subject = detector()
        result = calibrate(subject)

        self.assertFalse(result.ready)
        self.assertTrue(result.just_calibrated)
        self.assertTrue(subject.ready)
        self.assertGreater(subject.noise_sigma, 0.0)
        self.assertAlmostEqual(
            subject.threshold,
            subject.noise_mean + 5.0 * subject.noise_sigma,
        )

    def test_trigger_starts_event_and_tail_extends_from_last_trigger(self):
        subject = detector(baseline_tau_seconds=1000.0)
        calibrate(subject)

        first = subject.process((1.0, 0.0, 9.806), 1.1)
        second = subject.process((-1.0, 0.0, 9.806), 1.8)
        tail = subject.process((0.0, 0.0, 9.806), 2.7)
        ended = subject.process((0.0, 0.0, 9.806), 2.81)

        self.assertTrue(first.event_started)
        self.assertTrue(first.active)
        self.assertTrue(second.over_threshold)
        self.assertTrue(tail.active)
        self.assertTrue(ended.event_ended)
        self.assertFalse(ended.active)

    def test_event_buffer_adds_pretrigger_context_and_zero_boundaries(self):
        buffer = EventBuffer[str](pre_trigger_samples=3)
        buffer.append(0.01, "t1")
        buffer.append(0.01, "t2")
        buffer.append(0.01, "t3")

        self.assertEqual(
            buffer.flush_with_zero(),
            [(0.0, "t1"), (0.01, "t2"), (0.01, "t3")],
        )
        self.assertEqual(buffer.flush_with_zero(), [])

    def test_noise_statistics_are_frozen_for_complete_event(self):
        subject = detector(baseline_tau_seconds=1000.0)
        calibrate(subject)
        mean = subject.noise_mean
        sigma = subject.noise_sigma

        subject.process((1.0, 0.0, 9.806), 1.1)
        subject.process((0.0, 0.0, 9.806), 1.5)
        subject.process((0.0, 0.0, 9.806), 2.0)

        self.assertEqual(subject.noise_mean, mean)
        self.assertEqual(subject.noise_sigma, sigma)

    def test_resting_vector_adapts_even_during_event(self):
        subject = detector(baseline_tau_seconds=0.1)
        calibrate(subject)

        for index in range(1, 11):
            subject.process((1.0, 0.0, 9.806), 1.0 + index / 10)

        self.assertIsNotNone(subject.baseline)
        self.assertGreater(subject.baseline[0], 0.99)
        self.assertTrue(math.isfinite(subject.threshold))


if __name__ == "__main__":
    unittest.main()
