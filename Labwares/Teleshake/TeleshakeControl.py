# TeleshakeControl.py
import builtins
import logging
import os
import sys
import time
from enum import IntEnum
from typing import List, Tuple, Optional

# pyserial may be missing; handle import defensively
try:
    import serial
except Exception as _e:
    # We can't log yet; fall back to stderr and exit(1)
    sys.stderr.write(f"[FATAL] Failed to import 'serial' (pyserial): {_e}\n")
    sys.exit(1)

# === Logging Setup ===
LOG_DIRECTORY = r"C:\Python Log"
_LOGGER_INITIALIZED = False
_LOG_FILE_PATH: Optional[str] = None
_ORIGINAL_PRINT = builtins.print


def setup_logging() -> str:
    """
    Configure logging to a rotating file and mirror print/stdout to logging.
    Never raises; returns a path or exits(1) if logging can't be set up.
    """
    global _LOGGER_INITIALIZED, _LOG_FILE_PATH

    if _LOGGER_INITIALIZED and _LOG_FILE_PATH:
        return _LOG_FILE_PATH

    try:
        os.makedirs(LOG_DIRECTORY, exist_ok=True)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        log_path = os.path.join(LOG_DIRECTORY, f"Teleshake_{timestamp}.log")

        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

        root_logger = logging.getLogger()
        root_logger.handlers.clear()
        root_logger.setLevel(logging.INFO)
        root_logger.addHandler(file_handler)

        def print_and_log(*args, **kwargs):
            sep = kwargs.get("sep", " ")
            message = sep.join(str(arg) for arg in args)
            target = kwargs.get("file", sys.stdout)

            # Mirror to log
            if target is sys.stderr:
                logging.error(message)
            else:
                logging.info(message)

            # Also print to original stdout/stderr
            _ORIGINAL_PRINT(*args, **kwargs)

        builtins.print = print_and_log

        _LOGGER_INITIALIZED = True
        _LOG_FILE_PATH = log_path
        logging.info("Logging initialized.")
        return log_path
    except Exception as e:
        # Last resort: stderr + hard exit
        sys.stderr.write(f"[FATAL] Unable to initialize logging: {e}\n")
        sys.exit(1)


class TeleshakeCommand(IntEnum):
    """Teleshake protocol commands"""
    QUERY_ALL       = 0x20
    RESET_ALL       = 0x21
    RESET_DEVICE    = 0x22
    GET_INFO        = 0x23
    GET_LAST_ERROR  = 0x25
    START_DEVICE    = 0x30
    STOP_DEVICE     = 0x31
    GET_CYCLE_TIME  = 0x32
    SET_CYCLE_TIME  = 0x33


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

    def _safe_sleep(self, seconds: float):
        """Sleep but swallow any unexpected errors (ensuring no raise)."""
        try:
            time.sleep(seconds)
        except Exception as e:
            print(f"Sleep interrupted/failed: {e}")

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
            print(f"Connected to {self.com_port}")
            self.is_connected = True
            return True
        except Exception as e:
            print(f"Failed to connect to {self.com_port}: {e}")
            self.serial_port = None
            self.is_connected = False
            return False

    def disconnect(self):
        """Close serial connection safely."""
        try:
            if self.serial_port and self.serial_port.is_open:
                self.serial_port.close()
                print(f"Disconnected from {self.com_port}")
        except Exception as e:
            print(f"Error while disconnecting: {e}")
        finally:
            self.is_connected = False
            self.serial_port = None

    @staticmethod
    def calculate_checksum(bytes_data: List[int]) -> int:
        """Calculate modulo-256 checksum for first 5 bytes (safe)."""
        try:
            return sum(bytes_data[:5]) % 256
        except Exception as e:
            print(f"Checksum calculation error: {e}")
            # Return a value that won't match to force detection upstream
            return -1

    @staticmethod
    def create_control_byte(address: int, init_mode: bool = False,
                            dirty: bool = True, error: bool = False) -> int:
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
        except Exception as e:
            print(f"Create control byte failed: {e}")
            # Default to address 0, dirty, normal mode
            return 0x20

    @staticmethod
    def speed_to_cycle_time(speed: int) -> Optional[Tuple[int, int, int]]:
        """
        Convert speed (RPM or shakes/min) to cycle time in microseconds -> 3 bytes.
        Returns None on error.
        """
        try:
            if speed < 1000:
                print("Invalid speed: must be at least 1000 shakes per minute.")
                return None

            # 60,000,000 microseconds per minute / speed = cycle time
            cycle_time_us = int(60_000_000 / speed)

            if not (0 <= cycle_time_us <= 0xFFFFFF):
                print(f"Speed {speed} results in an out-of-range 24-bit cycle time.")
                return None

            high = (cycle_time_us >> 16) & 0xFF
            mid  = (cycle_time_us >> 8)  & 0xFF
            low  =  cycle_time_us        & 0xFF
            return high, mid, low
        except Exception as e:
            print(f"Error converting speed to cycle time: {e}")
            return None

    def _serial_write_bytes(self, data: List[int]) -> bool:
        """Write a list of bytes to serial port safely."""
        try:
            if not (self.serial_port and self.serial_port.is_open):
                print("Serial port not open for write.")
                return False
            for b in data:
                self.serial_port.write(bytes([b & 0xFF]))
            return True
        except Exception as e:
            print(f"Serial write failed: {e}")
            return False

    def _serial_read_exact(self, nbytes: int, timeout_s: float = 2.0) -> Optional[List[int]]:
        """Read exactly nbytes or return None on timeout/error."""
        try:
            if not (self.serial_port and self.serial_port.is_open):
                print("Serial port not open for read.")
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
                    print(f"Serial read error: {e_inner}")
                    return None

            if len(response) == nbytes:
                return response
            else:
                print(f"Timeout: expected {nbytes} bytes, got {len(response)}")
                return None
        except Exception as e:
            print(f"Serial read failed: {e}")
            return None

    # --------------- High-level protocol ---------------

    def send_command(self, command: int, data: Optional[List[int]] = None) -> Optional[List[int]]:
        """
        Send 6-byte command and receive 6-byte response.
        Returns None on any failure (never raises).
        """
        if not self.is_connected:
            print("Not connected to device")
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
            print(f"Data preparation failed: {e}")
            return None

        control_byte = self.create_control_byte(self.device_address)
        telegram = [control_byte, command] + data[:3]
        checksum = self.calculate_checksum(telegram)
        telegram.append(checksum & 0xFF)

        print(f"Sending: {' '.join(f'{b:03d}' for b in telegram)}")

        if not self._serial_write_bytes(telegram):
            print("Failed to write command to serial.")
            return None

        self._safe_sleep(0.5)

        response = self._serial_read_exact(6, timeout_s=2.0)
        if not response:
            print("No/Incomplete response from device.")
            return None

        print(f"Received: {' '.join(f'{b:03d}' for b in response)}")

        # Verify checksum
        calc_checksum = self.calculate_checksum(response)
        if calc_checksum != response[5]:
            print(f"Checksum error! Expected {calc_checksum}, got {response[5]}")
            return None

        # Check if dirty bit was cleared (successful execution)
        try:
            if (response[0] & 0x20) == 0:
                print("Command executed successfully")
            else:
                print("Command may not have been executed (dirty bit still set).")
        except Exception as e:
            print(f"Post-parse response flag check failed: {e}")

        return response

    def initialize_device(self) -> bool:
        """Initialize device with broadcast QueryAll command (never raises)."""
        print("\n--- Initializing Device ---")
        try:
            control_byte = self.create_control_byte(0x0F, init_mode=True)  # Broadcast address
            telegram = [control_byte, TeleshakeCommand.QUERY_ALL, 0, 0, 0]
            checksum = self.calculate_checksum(telegram)
            telegram.append(checksum & 0xFF)

            print(f"Sending QueryAll: {' '.join(f'{b:03d}' for b in telegram)}")
            if not self._serial_write_bytes(telegram):
                print("Failed to send initialization broadcast.")
                return False

            self._safe_sleep(0.5)

            # Read whatever the device returns without assuming exact length here
            # but try to read 6 first; if nothing, still consider init best-effort
            resp = self._serial_read_exact(6, timeout_s=1.0)
            if resp:
                print(f"Initialization response: {' '.join(f'{b:03d}' for b in resp)}")
            else:
                print("Initialization: no explicit response (continuing).")
            return True
        except Exception as e:
            print(f"Initialization failed: {e}")
            return False

    def set_speed(self, speed: int) -> bool:
        """Set device speed safely."""
        print(f"\n--- Setting Speed to {speed} ---")
        parts = self.speed_to_cycle_time(speed)
        if parts is None:
            print("Failed to compute cycle time from speed.")
            return False

        high, mid, low = parts
        print(f"Cycle time bytes: high={high}, mid={mid}, low={low}")

        response = self.send_command(TeleshakeCommand.SET_CYCLE_TIME, [high, mid, low])
        return response is not None

    def start_device(self) -> bool:
        """Start the device safely."""
        print("\n--- Starting Device ---")
        response = self.send_command(TeleshakeCommand.START_DEVICE)
        return response is not None

    def stop_device(self) -> bool:
        """Stop the device safely."""
        print("\n--- Stopping Device ---")
        response = self.send_command(TeleshakeCommand.STOP_DEVICE)
        return response is not None

    def shake_for_duration(self, speed: int, duration: float) -> bool:
        """
        Shake at specified speed for given duration.
        Returns False on any failure (never raises).
        """
        print(f"\n=== Shaking at speed {speed} for {duration} seconds ===")

        # Set speed
        if not self.set_speed(speed):
            print("Failed to set speed")
            return False

        # Start
        if not self.start_device():
            print("Failed to start device")
            return False

        # Wait
        print(f"Shaking for {duration} seconds...")
        self._safe_sleep(duration)

        # Stop
        if not self.stop_device():
            print("Failed to stop device")
            return False

        return True


def main():
    """Execute the specified shaking sequence with strict exit codes."""
    log_path = setup_logging()
    print(f"Log file: {log_path}")

    # Configuration
    try:
        if len(sys.argv) > 1:
            com_port = sys.argv[1]
        else:
            com_port = 'COM6'  # Default from your logs
        print(f"Using COM port: {com_port}")
    except Exception as e:
        print(f"Failed reading arguments: {e}")
        sys.exit(1)

    controller = TeleshakeController(com_port, device_address=1)

    # Connect to device
    if not controller.connect():
        print("Failed to establish connection")
        sys.exit(1)

    # Ensure we always attempt to stop/disconnect on exit paths
    try:
        # Initialize device (best-effort; if it fails, still continue but log)
        if not controller.initialize_device():
            print("Warning: initialization did not confirm; proceeding cautiously.")
        controller._safe_sleep(1)

        print("\n" + "=" * 60)
        print("STARTING SHAKE SEQUENCE")
        print("=" * 60)

        # Phase 1: Speed 1200 for 5 seconds, repeat 10 times
        print("\n### PHASE 1: Speed 1200, 5 seconds x 10 repetitions ###")
        for i in range(10):
            print(f"\n--- Repetition {i + 1}/10 ---")
            ok = controller.shake_for_duration(speed=1200, duration=5)
            if not ok:
                print("Aborting sequence: failed to complete repetition.")
                sys.exit(1)

            if i < 9:
                print("Waiting 2 seconds before next repetition...")
                controller._safe_sleep(2)

        print("\n### PHASE 1 COMPLETE ###")
        print("Waiting 5 seconds before Phase 2...")
        controller._safe_sleep(5)

        # Phase 2: Speed 1300 for 30 seconds
        print("\n### PHASE 2: Speed 1300, 30 seconds ###")
        if not controller.shake_for_duration(speed=1300, duration=30):
            print("Aborting sequence: failed to complete Phase 2.")
            sys.exit(1)

        print("\n" + "=" * 60)
        print("SHAKE SEQUENCE COMPLETE")
        print("=" * 60)

        # If we reached here, consider it a success
        sys.exit(0)

    except KeyboardInterrupt:
        print("\n\nInterrupted by user - stopping device...")
        # Treat as a controlled failure for automation purposes
        sys.exit(1)
    except Exception as e:
        print(f"\nFatal error during execution: {e}")
        sys.exit(1)
    finally:
        # Best effort stop & disconnect (never raise)
        try:
            controller.stop_device()
        except Exception as e:
            print(f"Error on stop_device in finally: {e}")
        controller._safe_sleep(1)
        try:
            controller.disconnect()
        except Exception as e:
            print(f"Error on disconnect in finally: {e}")


if __name__ == "__main__":
    main()
