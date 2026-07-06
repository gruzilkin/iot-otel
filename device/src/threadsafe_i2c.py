"""Thread-safe Linux I2C bus wrapper for Blinka worker-thread access."""

from adafruit_extended_bus import ExtendedI2C


class ThreadSafeExtendedI2C(ExtendedI2C):
    """Use ExtendedI2C's real RLock for CircuitPython bus locking.

    Blinka's inherited ``try_lock`` uses a cooperative boolean. That is sufficient
    when every sensor runs on one thread, but slow sensors run in worker threads in
    this application so they do not stall accelerometer sampling. ExtendedI2C already
    creates an RLock; these overrides make I2CDevice contexts actually use it.
    """

    def try_lock(self):
        return self._lock.acquire(blocking=False)

    def unlock(self):
        self._lock.release()
