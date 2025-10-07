import sys
import serial
import time


def main():
    if len(sys.argv) < 4:
        print("Usage: python sendbytes.py response_file COMport# byte1 byte2 ... [>> output_file]")
        return

    # Check if output redirection is included
    if '>>' in sys.argv:
        redirection_index = sys.argv.index('>>')
        output_file = sys.argv[redirection_index + 1]
        sys.argv = sys.argv[:redirection_index]
    else:
        output_file = None

    response_file = sys.argv[1]
    try:
        com_port = int(sys.argv[2]) - 1
    except ValueError:
        print("Invalid COM port number. Please provide a valid integer.")
        return

    try:
        bytes_to_send = [int(byte) for byte in sys.argv[3:] if byte.isdigit()]
    except ValueError as e:
        print(f"Invalid byte value. Please provide valid integers for bytes. Error: {e}")
        return

    print(f"Response file: {response_file}")
    print(f"COM port: {com_port + 1}")
    print(f"Bytes to send: {bytes_to_send}")
    if output_file:
        print(f"Output will be appended to: {output_file}")

    try:
        ser = serial.Serial(
            port=f'COM{com_port + 1}',
            baudrate=9600,
            bytesize=serial.EIGHTBITS,
            parity=serial.PARITY_NONE,
            stopbits=serial.STOPBITS_ONE,
            timeout=1
        )
    except serial.SerialException as e:
        print(f"Cannot open COM port: {e}")
        return

    with open(response_file, 'w') as file:
        try:
            # Send bytes
            for byte in bytes_to_send:
                ser.write(byte.to_bytes(1, 'little'))
                print(f"Sent byte {byte}")

            # Give some time for the response to be ready
            # If the _teleshake.txt is empty this is the place to add more delays
            time.sleep(0.5)

            # Read response
            start_time = time.time()
            while True:
                bytes_in_buffer = ser.in_waiting
                if bytes_in_buffer > 0:
                    response = ser.read(bytes_in_buffer)
                    for byte in response:
                        print(f"{byte:03}", end=' ')
                        file.write(f"{byte:03} ")
                    print("\nReceived bytes:", bytes_in_buffer)
                else:
                    time.sleep(0.1)

                # Exit loop after 5 seconds of no new data
                if time.time() - start_time > 5:
                    break

        except Exception as e:
            print(f"Error during serial communication: {e}")
        finally:
            ser.close()
            print("Closed serial port.")

    if output_file:
        try:
            with open(output_file, 'a') as out_file:
                with open(response_file, 'r') as res_file:
                    data_to_append = res_file.read()
                    out_file.write(data_to_append)
                    print(f"Appended data to {output_file}: {data_to_append}")
        except Exception as e:
            print(f"Error during file operations: {e}")


if __name__ == "__main__":
    main()
