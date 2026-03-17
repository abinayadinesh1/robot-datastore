# /// script
# dependencies = [
#   "bbos",
# ]
# [tool.uv.sources]
# bbos = { path = "/home/bracketbot/BracketBotOS", editable = true }
# ///
from bbos import Writer, Type
import numpy as np
import sys
import tty
import termios
import select
import time

# Configuration
TURN_SPEED = 1.0  # Increased for faster turn response
SPEED = 0.05
COMMAND_TIMEOUT = 0.25  # Stop if no key received within this time

def get_key(timeout=0.02):
    """Non-blocking key read with timeout"""
    if select.select([sys.stdin], [], [], timeout)[0]:
        return sys.stdin.read(1)
    return None

def main():
    print("BracketBotOS WASD Robot Control")
    print("===============================")
    print("Controls:")
    print("  W - Move Forward")
    print("  S - Move Backward")
    print("  A - Turn Left")
    print("  D - Turn Right")
    print("  Q - Quit")
    print("Hold keys to move. Release to stop.")
    print("Ready for input...")

    # Save terminal settings
    old_settings = termios.tcgetattr(sys.stdin)

    try:
        # Set terminal to raw mode
        tty.setraw(sys.stdin.fileno())

        with Writer("drive.ctrl", Type("drive_ctrl")) as writer:
            last_key_time = 0
            active_speed = 0.0
            active_turn = 0.0

            while True:
                key = get_key(0.02)
                now = time.time()

                if key:
                    k = key.lower()
                    if k == 'q' or key == '\x03':  # q or Ctrl+C
                        break

                    last_key_time = now

                    # Set command based on key
                    if k == 'w':
                        active_speed = SPEED
                        active_turn = 0.0
                    elif k == 's':
                        active_speed = -SPEED
                        active_turn = 0.0
                    elif k == 'a':
                        active_speed = 0.0
                        active_turn = TURN_SPEED
                    elif k == 'd':
                        active_speed = 0.0
                        active_turn = -TURN_SPEED

                # Determine what to send
                if now - last_key_time < COMMAND_TIMEOUT:
                    speed = active_speed
                    turn = active_turn
                else:
                    speed = 0.0
                    turn = 0.0

                writer['twist'] = np.array([speed, turn], dtype=np.float32)

    finally:
        # Restore terminal settings
        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)
        print("\nRobot stopped. Goodbye!")

if __name__ == "__main__":
    main()