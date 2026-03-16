"""Initialize Tika Server connections for Podman-based deployment."""

import threading
import requests
from typing import Optional

# Thread-safe port rotation
_port_lock = threading.Lock()
_current_port_index = 0
_tika_ports = [
    9998,
    9999,
    # 10000,
    # 10001,
    # 10002,
    # 10003,
    # 10004,
    # 10005,
]  # 8 Tika servers in Podman


def get_next_tika_port() -> int:
    """Get the next Tika server port in round-robin fashion."""
    global _current_port_index
    with _port_lock:
        port = _tika_ports[_current_port_index % len(_tika_ports)]
        _current_port_index += 1
    return port


def check_tika_health() -> bool:
    """Check if at least one Tika server is running."""
    for port in _tika_ports:
        try:
            response = requests.get(f"http://localhost:{port}/", timeout=2)
            if response.status_code == 200:
                return True
        except Exception:
            continue
    return False


def init_tika_for_windows() -> bool:
    """
    Check Tika Server availability (Podman-based).

    Note: This function now expects Tika servers to be running in Podman
    containers. Start them with:

        podman-compose -f docker/docker-compose-tika.yml up -d
    """
    print("Checking Tika Servers in Podman...")

    if not check_tika_health():
        print("✗ No Tika servers accessible on ports 9998-10005")
        print(
            "  Start them with: podman-compose -f docker/docker-compose-tika.yml up -d"
        )
        return False

    print(f"✓ Tika servers online on ports: {_tika_ports}")
    return True


if __name__ == "__main__":
    success = init_tika_for_windows()
    sys.exit(0 if success else 1)
