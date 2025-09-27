# src/lamport.py
import threading

class LamportClock:
    def __init__(self):
        self._time = 0
        self._lock = threading.Lock()

    def tick(self) -> int:
        with self._lock:
            self._time += 1
            return self._time

    def update(self, received_time: int) -> int:
        with self._lock:
            self._time = max(self._time, int(received_time)) + 1
            return self._time

    def read(self) -> int:
        with self._lock:
            return int(self._time)
