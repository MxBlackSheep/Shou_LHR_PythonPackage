import builtins
import logging
import os
import sys
import time
from enum import IntEnum
from typing import List, Tuple, Optional

import serial

LOG_DIRECTORY = r"C:\Python Log"
_LOGGER_INITIALIZED = False
_LOG_FILE_PATH: Optional[str] = None
_ORIGINAL_PRINT = builtins.print


def setup_logging() -> str:
    """Configure logging to file and mirror standard output."""
    global _LOGGER_INITIALIZED, _LOG_FILE_PATH

    if _LOGGER_INITIALIZED and _LOG_FILE_PATH:
        return _LOG_FILE_PATH

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

        if target is sys.stderr:
            logging.error(message)
        else:
            logging.info(message)

        _ORIGINAL_PRINT(*args, **kwargs)

    builtins.print = print_and_log

    _LOGGER_INITIALIZED = True
    _LOG_FILE_PATH = log_path
    logging.info("Logging initialized.")
    return log_path


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
    """Controller for H+P Labortechnik Teleshake device"""

    def __init__(self, com_port: str, device_address: int = 1):
        """
        Initialize Teleshake controller

        Args:
            com_port: COM port (e.g., 'COM6')
            device_address: Device address (1-14, 15 is broadcast)
        """
        self.com_port = com_port
        self.device_address = device_address
        self.serial_port = None
        self.is_connected = False

    def connect(self) -> bool:
        """Establish serial connection"""
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
        except serial.SerialException as e:
            print(f"Failed to connect to {self.com_port}: {e}")
            return False

    def disconnect(self):
        """Close serial connection"""
        if self.serial_port and self.serial_port.is_open:
            self.serial_port.close()
            print(f"Disconnected from {self.com_port}")
            self.is_connected = False

    def calculate_checksum(self, bytes_data: List[int]) -> int:
        """Calculate modulo-256 checksum for first 5 bytes"""
        return sum(bytes_data[:5]) % 256

    def create_control_byte(self, address: int, init_mode: bool = False,
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
        byte = address & 0x0F  # Bits 0-3
        if error:
            byte |= 0x10  # Bit 4
        if dirty:
            byte |= 0x20  # Bit 5
        if init_mode:
            byte |= 0x40  # Bit 6
        # Bit 7 stays 0 for 6-byte telegram
        return byte

    def speed_to_cycle_time(self, speed: int) -> Tuple[int, int, int]:
        """
        Convert speed (RPM or shakes/min) to cycle time in microseconds

        Args:
            speed: Speed in RPM or shakes per minute

        Returns:
            Tuple of (high_byte, mid_byte, low_byte) for 24-bit cycle time
        """
        # Convert speed to cycle time in microseconds
        # Assuming speed is in shakes/minute or RPM
        if speed < 1000:
            raise ValueError("Speed must be at least 1000 shakes per minute.")

        # 60,000,000 microseconds per minute / speed = cycle time
        cycle_time_us = int(60_000_000 / speed)

        if cycle_time_us > 0xFFFFFF:
            raise ValueError(
                f"Speed {speed} results in a cycle time exceeding the 24-bit limit."
            )

        # Split into 3 bytes (24-bit value)
        high_byte = (cycle_time_us >> 16) & 0xFF
        mid_byte = (cycle_time_us >> 8) & 0xFF
        low_byte = cycle_time_us & 0xFF

        return high_byte, mid_byte, low_byte

    def send_command(self, command: int, data: List[int] = None) -> Optional[List[int]]:
        """
        Send 6-byte command and receive response

        Args:
            command: Command byte
            data: Optional 3 bytes of data [data2, data1, data0]

        Returns:
            Response bytes or None if error
        """
        if not self.is_connected:
            print("Not connected to device")
            return None

        # Prepare data bytes
        if data is None:
            data = [0, 0, 0]
        elif len(data) < 3:
            data = data + [0] * (3 - len(data))

        # Create telegram
        control_byte = self.create_control_byte(self.device_address)
        telegram = [control_byte, command] + data[:3]
        checksum = self.calculate_checksum(telegram)
        telegram.append(checksum)

        # Send command
        print(f"Sending: {' '.join(f'{b:03d}' for b in telegram)}")
        for byte in telegram:
            self.serial_port.write(bytes([byte]))

        # Wait for response
        time.sleep(0.5)

        # Read response (6 bytes)
        response = []
        bytes_to_read = 6
        timeout_start = time.time()

        while len(response) < bytes_to_read and time.time() - timeout_start < 2:
            if self.serial_port.in_waiting > 0:
                byte_data = self.serial_port.read(1)
                if byte_data:
                    response.append(byte_data[0])

        if len(response) == 6:
            print(f"Received: {' '.join(f'{b:03d}' for b in response)}")

            # Verify checksum
            calc_checksum = self.calculate_checksum(response)
            if calc_checksum != response[5]:
                print(f"Checksum error! Expected {calc_checksum}, got {response[5]}")
                return None

            # Check if dirty bit was cleared (successful execution)
            if response[0] & 0x20 == 0:
                print("Command executed successfully")
            else:
                print("Command may not have been executed")

            return response
        else:
            print(f"Incomplete response: received {len(response)} bytes")
            return None

    def initialize_device(self) -> bool:
        """Initialize device with QueryAll command"""
        print("\n--- Initializing Device ---")
        control_byte = self.create_control_byte(0x0F, init_mode=True)  # Broadcast address
        telegram = [control_byte, TeleshakeCommand.QUERY_ALL, 0, 0, 0]
        checksum = self.calculate_checksum(telegram)
        telegram.append(checksum)

        print(f"Sending QueryAll: {' '.join(f'{b:03d}' for b in telegram)}")
        for byte in telegram:
            self.serial_port.write(bytes([byte]))

        time.sleep(0.5)

        # Read response
        response = []
        while self.serial_port.in_waiting > 0:
            byte_data = self.serial_port.read(1)
            if byte_data:
                response.append(byte_data[0])

        if response:
            print(f"Initialization response: {' '.join(f'{b:03d}' for b in response)}")
            return True
        return False

    def set_speed(self, speed: int) -> bool:
        """
        Set device speed

        Args:
            speed: Speed in RPM or shakes/minute

        Returns:
            True if successful
        """
        print(f"\n--- Setting Speed to {speed} ---")
        try:
            high, mid, low = self.speed_to_cycle_time(speed)
        except ValueError as exc:
            print(f"Invalid speed: {exc}")
            return False
        print(f"Cycle time bytes: high={high}, mid={mid}, low={low}")

        response = self.send_command(TeleshakeCommand.SET_CYCLE_TIME, [high, mid, low])
        return response is not None

    def start_device(self) -> bool:
        """Start the device"""
        print("\n--- Starting Device ---")
        response = self.send_command(TeleshakeCommand.START_DEVICE)
        return response is not None

    def stop_device(self) -> bool:
        """Stop the device"""
        print("\n--- Stopping Device ---")
        response = self.send_command(TeleshakeCommand.STOP_DEVICE)
        return response is not None

    def shake_for_duration(self, speed: int, duration: float):
        """
        Shake at specified speed for given duration

        Args:
            speed: Speed in RPM or shakes/minute
            duration: Duration in seconds
        """
        print(f"\n=== Shaking at speed {speed} for {duration} seconds ===")

        # Set speed
        if not self.set_speed(speed):
            print("Failed to set speed")
            return False

        # Start shaking
        if not self.start_device():
            print("Failed to start device")
            return False

        # Wait for specified duration
        print(f"Shaking for {duration} seconds...")
        time.sleep(duration)

        # Stop shaking
        if not self.stop_device():
            print("Failed to stop device")
            return False

        return True


def main():
    """Execute the specified shaking sequence"""

    log_path = setup_logging()
    print(f"Log file: {log_path}")

    # Configuration
    if len(sys.argv) > 1:
        com_port = sys.argv[1]
    else:
        com_port = 'COM6'  # Default from your logs

    print(f"Using COM port: {com_port}")

    # Create controller
    controller = TeleshakeController(com_port, device_address=1)

    # Connect to device
    if not controller.connect():
        print("Failed to establish connection")
        return

    try:
        # Initialize device
        controller.initialize_device()
        time.sleep(1)

        print("\n" + "=" * 60)
        print("STARTING SHAKE SEQUENCE")
        print("=" * 60)

        # Sequence 1: Speed 1200 for 5 seconds, repeat 10 times
        print("\n### PHASE 1: Speed 1200, 5 seconds x 10 repetitions ###")
        for i in range(10):
            print(f"\n--- Repetition {i + 1}/10 ---")
            if not controller.shake_for_duration(speed=1200, duration=5):
                print("Aborting sequence: failed to complete repetition.")
                return

            if i < 9:  # Don't wait after last repetition
                print("Waiting 2 seconds before next repetition...")
                time.sleep(2)

        print("\n### PHASE 1 COMPLETE ###")
        print("Waiting 5 seconds before Phase 2...")
        time.sleep(5)

        # Sequence 2: Speed 1300 for 30 seconds
        print("\n### PHASE 2: Speed 1300, 30 seconds ###")
        if not controller.shake_for_duration(speed=1300, duration=30):
            print("Aborting sequence: failed to complete Phase 2.")
            return

        print("\n" + "=" * 60)
        print("SHAKE SEQUENCE COMPLETE")
        print("=" * 60)

    except KeyboardInterrupt:
        print("\n\nInterrupted by user - stopping device...")
        controller.stop_device()
    except Exception as e:
        print(f"\nError during execution: {e}")
        controller.stop_device()
    finally:
        # Ensure device is stopped and connection closed
        controller.stop_device()
        time.sleep(1)
        controller.disconnect()


if __name__ == "__main__":
    main()
