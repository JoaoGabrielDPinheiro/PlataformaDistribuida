import socket
import json
import struct
import time
from config import settings
from logger_setup import get_logger

logger = get_logger("client")

class Client:
    def __init__(self, orch_host=settings.ORCHESTRATOR_HOST, orch_port=settings.ORCHESTRATOR_PORT):
        self.orch_host = orch_host
        self.orch_port = orch_port
        self.token = None

    def auth(self, username: str, password: str):
        payload = {"type":"auth","username":username,"password":password}
        resp = self._send(payload)
        if resp and resp.get("ok"):
            self.token = resp.get("token")
            logger.info("Autenticado. Token obtido.")
            return True
        logger.warning("Falha de autenticação")
        return False

    def submit_task(self, payload: dict):
        if not self.token:
            logger.error("Não autenticado")
            return None
        data = {"type":"submit_task","token":self.token,"payload":payload}
        return self._send(data)

    def query(self, task_id: str):
        if not self.token:
            logger.error("Não autenticado")
            return None
        data = {"type":"query_status","token":self.token,"task_id":task_id}
        return self._send(data)

    def _send(self, obj, timeout=5.0):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(timeout)
            s.connect((self.orch_host, self.orch_port))
            data = json.dumps(obj).encode("utf-8")
            s.sendall(len(data).to_bytes(4,"big"))
            s.sendall(data)
            raw_len = s.recv(4)
            if not raw_len:
                s.close()
                return None
            l = int.from_bytes(raw_len,"big")
            raw = b""
            while len(raw) < l:
                chunk = s.recv(l - len(raw))
                if not chunk:
                    break
                raw += chunk
            s.close()
            if raw:
                return json.loads(raw.decode("utf-8"))
            return None
        except Exception:
            logger.exception("Erro send")
            return None

if __name__ == "__main__":
    c = Client()
    if c.auth("alice","password1"):
        r = c.submit_task({"duration": 3})
        print("submit:", r)
        if r and r.get("task_id"):
            tid = r["task_id"]
            time.sleep(1)
            print("query:", c.query(tid))
