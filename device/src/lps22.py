"""LPS22 pressure sensor with BDU (block data update) enabled.

The sensor rewrites its three pressure output bytes from each conversion (75 Hz)
asynchronously to bus reads, and the Adafruit driver leaves BDU (CTRL_REG1 bit 1)
disabled, so a conversion can land between the bytes of the driver's single
3-byte read. Whenever ambient pressure sits on a 16 hPa raw boundary (1008.00 hPa
is exactly 0x3F0000 x 1/4096 hPa) that torn read returns a clean +/-16 hPa
one-sample spike. BDU holds the output registers steady until a full read
completes.
"""
import adafruit_lps2x
from adafruit_register.i2c_bit import RWBit

_LPS22_CTRL_REG1 = 0x10


class LPS22(adafruit_lps2x.LPS22):
    """LPS22 that guarantees coherent multi-byte pressure reads."""

    block_data_update = RWBit(_LPS22_CTRL_REG1, 1)

    def __init__(self, i2c_bus, *args, **kwargs) -> None:
        super().__init__(i2c_bus, *args, **kwargs)
        self.block_data_update = True
