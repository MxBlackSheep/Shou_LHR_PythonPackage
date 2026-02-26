# TeleshakeControl.py
import os
import sys
import time
import serial
from datetime import datetime
from enum import IntEnum
from typing import List, Tuple, Optional

# === Simple file logging (no print monkeypatching) ===
LOG_DIR = r"C:\Python Log"
os.makedirs(LOG_DIR, exist_ok=True)

SCRIPT_NAME = os.path.splitext(os.path.basename(sys.argv[0]))[0]
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = os.path.join(LOG_DIR, f"{SCRIPT_NAME}_{TIMESTAMP}.log")

# Set True if you want console output too (otherwise silent)
MIRROR_TO_CONSOLE = False


def log(msg: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{now}] {msg}\n"
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line)
    if MIRROR_TO_CONSOLE:
        sys.__stdout__.write(line)
        sys.__stdout__.flush()


def log_exc(prefix: str, exc: BaseException) -> None:
    log(f"{prefix}: {type(exc).__name__}: {exc}")


class TeleshakeCommand(IntEnum):
    """Teleshake protocol commands"""
    QUERY_ALL = 0x20
    RESET_ALL = 0x21
    RESET_DEVICE = 0x22
    GET_INFO = 0x23
    GET_LAST_ERROR = 0x25
    START_DEVICE = 0x30
    STOP_DEVICE = 0x31
    GET_CYCLE_TIME = 0x32
    SET_CYCLE_TIME = 0x33


class TeleshakeController:
    """Controller for Teleshake"""

    def __init__(self, com_port: str, device_address: int = 1):
        """
        Initialize Teleshake controller

        Args:
            com_port: COM port (e.g., 'COM6')
            device_address: Device address (1-14, 15 is broadcast)
        """
        self.com_port = com_port
        self.device_address = device_address
        self.serial_port: Optional[serial.Serial] = None
        self.is_connected = False

    # --------------- Low-level helpers ---------------

    def _safe_sleep(self, seconds: float) -> None:
        """Sleep but swallow any unexpected errors (ensuring no raise)."""
        try:
            time.sleep(seconds)
        except Exception as e:
            log(f"Sleep interrupted/failed: {e}")

    def connect(self) -> bool:
        """Establish serial connection safely."""
        try:
            self.serial_port = serial.Serial(
                port=self.com_port,
                baudrate=9600,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                timeout=2.0
            )
            log(f"Connected to {self.com_port}")
            self.is_connected = True
            return True
        except Exception as e:
            log(f"Failed to connect to {self.com_port}: {e}")
            self.serial_port = None
            self.is_connected = False
            return False

    def disconnect(self) -> None:
        """Close serial connection safely."""
        try:
            if self.serial_port and self.serial_port.is_open:
                self.serial_port.close()
                log(f"Disconnected from {self.com_port}")
        except Exception as e:
            log(f"Error while disconnecting: {e}")
        finally:
            self.is_connected = False
            self.serial_port = None

    @staticmethod
    def calculate_checksum(bytes_data: List[int]) -> int:
        """Calculate modulo-256 checksum for first 5 bytes (safe)."""
        try:
            return sum(bytes_data[:5]) % 256
        except Exception:
            # Return a value that won't match to force detection upstream
            return -1

    @staticmethod
    def create_control_byte(
        address: int,
        init_mode: bool = False,
        dirty: bool = True,
        error: bool = False
    ) -> int:
        """
        Create control byte

        Bit structure:
        - Bits 0-3: Device address
        - Bit 4: Error flag
        - Bit 5: Dirty bit
        - Bit 6: Mode (0=normal, 1=init)
        - Bit 7: Length (0=6 bytes)
        """
        try:
            byte = address & 0x0F  # Bits 0-3
            if error:
                byte |= 0x10  # Bit 4
            if dirty:
                byte |= 0x20  # Bit 5
            if init_mode:
                byte |= 0x40  # Bit 6
            # Bit 7 stays 0 for 6-byte telegram
            return byte
        except Exception:
            # Default to address 0, dirty, normal mode
            return 0x20

    @staticmethod
    def speed_to_cycle_time(speed: int) -> Optional[Tuple[int, int, int]]:
        """
        Convert speed (shakes/min) to cycle time in microseconds -> 3 bytes.
        Returns None on error.
        """
        try:
            if speed < 1000:
                log("Invalid speed: must be at least 1000 shakes per minute.")
                return None

            # 60,000,000 microseconds per minute / speed = cycle time
            cycle_time_us = int(60_000_000 / speed)

            if not (0 <= cycle_time_us <= 0xFFFFFF):
                log(f"Speed {speed} results in an out-of-range 24-bit cycle time.")
                return None

            high = (cycle_time_us >> 16) & 0xFF
            mid = (cycle_time_us >> 8) & 0xFF
            low = cycle_time_us & 0xFF
            return high, mid, low
        except Exception as e:
            log(f"Error converting speed to cycle time: {e}")
            return None

    def _serial_write_bytes(self, data: List[int]) -> bool:
        """Write a list of bytes to serial port safely."""
        try:
            if not (self.serial_port and self.serial_port.is_open):
                log("Serial port not open for write.")
                return False
            for b in data:
                self.serial_port.write(bytes([b & 0xFF]))
            return True
        except Exception as e:
            log(f"Serial write failed: {e}")
            return False

    def _serial_read_exact(self, nbytes: int, timeout_s: float = 2.0) -> Optional[List[int]]:
        """Read exactly nbytes or return None on timeout/error."""
        try:
            if not (self.serial_port and self.serial_port.is_open):
                log("Serial port not open for read.")
                return None

            response: List[int] = []
            start = time.time()
            while len(response) < nbytes and (time.time() - start) < timeout_s:
                try:
                    if self.serial_port.in_waiting > 0:
                        chunk = self.serial_port.read(1)
                        if chunk:
                            response.append(chunk[0])
                    else:
                        self._safe_sleep(0.01)
                except Exception as e_inner:
                    log(f"Serial read error: {e_inner}")
                    return None

            if len(response) == nbytes:
                return response

            log(f"Timeout: expected {nbytes} bytes, got {len(response)}")
            return None
        except Exception as e:
            log(f"Serial read failed: {e}")
            return None

    # --------------- High-level protocol ---------------

    def send_command(self, command: int, data: Optional[List[int]] = None) -> Optional[List[int]]:
        """
        Send 6-byte command and receive 6-byte response.
        Returns None on any failure (never raises).
        """
        if not self.is_connected:
            log("Not connected to device")
            return None

        # Prepare data bytes (3 bytes, pad with zeros)
        try:
            if data is None:
                data = [0, 0, 0]
            elif len(data) < 3:
                data = data + [0] * (3 - len(data))
            else:
                data = data[:3]
        except Exception as e:
            log(f"Data preparation failed: {e}")
            return None

        control_byte = self.create_control_byte(self.device_address)
        telegram = [control_byte, command] + data[:3]
        checksum = self.calculate_checksum(telegram)
        telegram.append(checksum & 0xFF)

        log(f"Sending: {' '.join(f'{b:03d}' for b in telegram)}")

        if not self._serial_write_bytes(telegram):
            log("Failed to write command to serial.")
            return None

        self._safe_sleep(0.5)

        response = self._serial_read_exact(6, timeout_s=2.0)
        if not response:
            log("No/Incomplete response from device.")
            return None

        log(f"Received: {' '.join(f'{b:03d}' for b in response)}")

        # Verify checksum
        calc_checksum = self.calculate_checksum(response)
        if calc_checksum != response[5]:
            log(f"Checksum error! Expected {calc_checksum}, got {response[5]}")
            return None

        # Check dirty bit cleared (successful execution)
        try:
            if (response[0] & 0x20) == 0:
                log("Command executed successfully")
            else:
                log("Command may not have been executed (dirty bit still set).")
        except Exception as e:
            log(f"Post-parse response flag check failed: {e}")

        # If error bit is set, attempt to fetch last error (best effort)
        try:
            if response and (response[0] & 0x10):
                log("Device responded with ERROR flag set; attempting GET_LAST_ERROR.")
                _ = self.send_command(TeleshakeCommand.GET_LAST_ERROR)
        except Exception:
            pass

        return response

    def initialize_device(self) -> bool:
        """Initialize device with broadcast QueryAll command (never raises)."""
        log("--- Initializing Device ---")
        try:
            control_byte = self.create_control_byte(0x0F, init_mode=True)  # Broadcast address
            telegram = [control_byte, TeleshakeCommand.QUERY_ALL, 0, 0, 0]
            checksum = self.calculate_checksum(telegram)
            telegram.append(checksum & 0xFF)

            log(f"Sending QueryAll: {' '.join(f'{b:03d}' for b in telegram)}")
            if not self._serial_write_bytes(telegram):
                log("Failed to send initialization broadcast.")
                return False

            self._safe_sleep(0.5)

            resp = self._serial_read_exact(6, timeout_s=1.0)
            if resp:
                log(f"Initialization response: {' '.join(f'{b:03d}' for b in resp)}")
            else:
                log("Initialization: no explicit response (continuing).")
            return True
        except Exception as e:
            log(f"Initialization failed: {e}")
            return False

    def set_speed(self, speed: int) -> bool:
        """Set device speed safely."""
        log(f"--- Setting Speed to {speed} ---")
        parts = self.speed_to_cycle_time(speed)
        if parts is None:
            log("Failed to compute cycle time from speed.")
            return False

        high, mid, low = parts
        log(f"Cycle time bytes: high={high}, mid={mid}, low={low}")

        response = self.send_command(TeleshakeCommand.SET_CYCLE_TIME, [high, mid, low])
        return response is not None

    def start_device(self) -> bool:
        """Start the device safely."""
        log("--- Starting Device ---")
        response = self.send_command(TeleshakeCommand.START_DEVICE)
        return response is not None

    def stop_device(self) -> bool:
        """Stop the device safely."""
        log("--- Stopping Device ---")
        response = self.send_command(TeleshakeCommand.STOP_DEVICE)
        return response is not None

    def shake_for_duration(self, speed: int, duration: float) -> bool:
        """
        Shake at specified speed for given duration.
        Returns False on any failure (never raises).
        """
        log(f"=== Shaking at speed {speed} for {duration} seconds ===")

        if not self.set_speed(speed):
            log("Failed to set speed")
            return False

        if not self.start_device():
            log("Failed to start device")
            return False

        log(f"Shaking for {duration} seconds...")
        self._safe_sleep(duration)

        if not self.stop_device():
            log("Failed to stop device")
            return False

        return True


def main() -> int:
    """Execute the specified shaking sequence with strict exit codes."""
    log(f"Log file: {LOG_FILE}")

    # Configuration
    try:
        com_port = sys.argv[1] if len(sys.argv) > 1 else "COM6"
        log(f"Using COM port: {com_port}")
    except Exception as e:
        log(f"Failed reading arguments: {e}")
        return 1

    controller = TeleshakeController(com_port, device_address=1)

    # Connect to device
    if not controller.connect():
        log("Failed to establish connection")
        return 1

    # Ensure we always attempt to stop/disconnect on exit paths
    try:
        # Initialize device
        if not controller.initialize_device():
            log("Warning: initialization did not confirm; proceeding cautiously.")
        controller._safe_sleep(1)

        log("=" * 60)
        log("STARTING SHAKE SEQUENCE")
        log("=" * 60)

        # Phase 1: 10 short cycles at 10 different pre-generated random speeds
        random_speeds = [1153, 1176, 1088, 1229, 1271, 1185, 1181, 1087, 1210, 1082]
        log(f"PHASE 1: 10 repetitions, 5 seconds each, random speeds={random_speeds}")

        for i, speed in enumerate(random_speeds, start=1):
            log(f"Repetition {i}/10 at speed={speed}")
            ok = controller.shake_for_duration(speed=speed, duration=5)
            if not ok:
                log("Aborting sequence: failed to complete repetition.")
                return 1

            if i < 10:
                log("Waiting 1 seconds before next repetition...")
                controller._safe_sleep(1)

        log("PHASE 1 COMPLETE")
        log("Waiting 2 seconds before Phase 2...")
        controller._safe_sleep(2)

        # Phase 2: Speed 1300 for 30 seconds (unchanged)
        log("PHASE 2: Speed 1300, 30 seconds")
        if not controller.shake_for_duration(speed=1300, duration=30):
            log("Aborting sequence: failed to complete Phase 2.")
            return 1

        log("=" * 60)
        log("SHAKE SEQUENCE COMPLETE")
        log("=" * 60)

        return 0

    except KeyboardInterrupt:
        log("Interrupted by user")
        return 1
    except Exception as e:
        log_exc("Fatal error during execution", e)
        return 1
    finally:
        # Stop & disconnect
        try:
            if controller.is_connected:
                controller.stop_device()
        except Exception as e:
            log(f"Error on stop_device in finally: {e}")
        controller._safe_sleep(0.5)
        try:
            controller.disconnect()
        except Exception as e:
            log(f"Error on disconnect in finally: {e}")


if __name__ == "__main__":
    sys.exit(main())