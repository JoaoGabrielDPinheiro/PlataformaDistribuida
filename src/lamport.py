import threading

class LamportClock:
    def __init__(self):
        self._lock = threading.Lock()
        self.time = 0

    def tick(self):
        with self._lock:
            self.time += 1
            return self.time

    def update(self, received_time: int):
        with self._lock:
            self.time = max(self.time, received_time) + 1
            return self.time

    def read(self):
        with self._lock:
            return self.time
