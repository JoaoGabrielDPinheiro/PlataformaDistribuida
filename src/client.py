import socket
import json
import time
from typing import Optional, Dict, Any
from config import settings
from logger_setup import get_logger

logger = get_logger("client")


class ClientError(Exception):
    """Exceção base para erros do Client."""


class AuthenticationError(ClientError):
    """Erro de autenticação."""


class ConnectionError(ClientError):
    """Erro de conexão ou envio de dados."""


class Client:
    AUTH = "auth"
    SUBMIT_TASK = "submit_task"
    QUERY_STATUS = "query_status"

    def __init__(self, orch_host: str = settings.ORCHESTRATOR_HOST,
                 orch_port: int = settings.ORCHESTRATOR_PORT) -> None:
        self.orch_host = orch_host
        self.orch_port = orch_port
        self.token: Optional[str] = None

    def auth(self, username: str, password: str) -> bool:
        payload = {"type": self.AUTH, "username": username, "password": password}
        resp = self._send(payload)
        if resp and resp.get("ok"):
            self.token = resp.get("token")
            logger.info("✅ Autenticado com sucesso. Token obtido.")
            return True
        logger.warning("❌ Falha de autenticação para usuário '%s'", username)
        raise AuthenticationError("Credenciais inválidas ou servidor recusou autenticação.")

    def submit_task(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        self._ensure_authenticated()
        data = {"type": self.SUBMIT_TASK, "token": self.token, "payload": payload}
        return self._send(data)

    def query(self, task_id: str) -> Dict[str, Any]:
        self._ensure_authenticated()
        data = {"type": self.QUERY_STATUS, "token": self.token, "task_id": task_id}
        return self._send(data)

    def _ensure_authenticated(self) -> None:
        if not self.token:
            logger.error("Operação negada: cliente não autenticado.")
            raise AuthenticationError("Cliente precisa estar autenticado antes de continuar.")

    def _send(self, obj: Dict[str, Any], timeout: float = 5.0) -> Optional[Dict[str, Any]]:
        try:
            with socket.create_connection((self.orch_host, self.orch_port), timeout=timeout) as s:
                data = json.dumps(obj).encode("utf-8")
                s.sendall(len(data).to_bytes(4, "big"))
                s.sendall(data)

                raw_len = s.recv(4)
                if not raw_len:
                    logger.error("Resposta vazia do servidor.")
                    return None

                length = int.from_bytes(raw_len, "big")
                raw = b""
                while len(raw) < length:
                    chunk = s.recv(length - len(raw))
                    if not chunk:
                        break
                    raw += chunk

                return json.loads(raw.decode("utf-8")) if raw else None

        except socket.timeout:
            logger.error("⏳ Timeout na comunicação com o servidor.")
            raise ConnectionError("Timeout ao tentar se comunicar com o servidor.")
        except Exception as e:
            logger.exception("Erro de comunicação com o servidor: %s", e)
            raise ConnectionError("Erro inesperado ao enviar dados.")

    # Exemplo de uso
if __name__ == "__main__":
    c = Client()
    try:
        if c.auth("alice", "password1"):
            r = c.submit_task({"duration": 3})
            print("submit:", r)
            if r and r.get("task_id"):
                tid = r["task_id"]
                time.sleep(1)
                print("query:", c.query(tid))
    except ClientError as e:
        logger.error("Falha: %s", e)
