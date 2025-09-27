# src/client.py
import socket
import json
import struct
import config.settings as settings
from src.common import send_json_tcp, recv_json_tcp

class Client:
    def __init__(self, host=None, port=None):
        self.host = host or settings.ORCHESTRATOR_HOST
        self.port = port or settings.ORCHESTRATOR_PORT
        self.token = None

    def _send(self, msg: dict, timeout=5.0):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((self.host, self.port))
        send_json_tcp(s, msg)
        resp = recv_json_tcp(s)
        s.close()
        return resp

    def auth(self, username: str, password: str) -> bool:
        resp = self._send({"type": "auth", "username": username, "password": password})
        if resp and resp.get("ok"):
            self.token = resp.get("token")
            return True
        return False

    def submit_task(self, payload: dict):
        msg = {"type": "submit_task", "token": self.token, "payload": payload}
        return self._send(msg)

    def query(self, task_id: str):
        msg = {"type": "query_task", "token": self.token, "task_id": task_id}
        return self._send(msg)

if __name__ == "__main__":
    c = Client()
    ok = c.auth("alice", "password1")
    print("Auth ok:", ok)
    r = c.submit_task({"duration": 2})
    print("Submit:", r)
    if r and r.get("task_id"):
        import time
        time.sleep(3)
        print("Query:", c.query(r["task_id"]))
