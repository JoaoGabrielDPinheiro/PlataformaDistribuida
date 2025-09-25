import socket
import threading
import json
import time
import uuid
from config import settings
from lamport import LamportClock
from logger_setup import get_logger
from common import recvall

logger = get_logger("worker")

class Worker:
    def __init__(self, host="127.0.0.1", port=0):
        self.id = str(uuid.uuid4())[:8]
        self.host = host
        self.port = port  # 0 -> OS escolhe
        self.server = None
        self.lamport = LamportClock()
        self.load = 0
        self.running = True
        self.orch_host = settings.ORCHESTRATOR_HOST
        self.orch_port = settings.ORCHESTRATOR_PORT
        self.fail_simulate = False

    def start(self):
        t = threading.Thread(target=self._tcp_server, daemon=True)
        t.start()
        time.sleep(0.1)
        self._register()
        t2 = threading.Thread(target=self._heartbeat_loop, daemon=True)
        t2.start()
        logger.info(f"Worker {self.id} iniciado")

    def _tcp_server(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((self.host, self.port))
        s.listen(5)
        self.server = s
        self.port = s.getsockname()[1]
        while self.running:
            try:
                conn, addr = s.accept()
                threading.Thread(target=self._handle_conn, args=(conn, addr), daemon=True).start()
            except Exception:
                logger.exception("Erro accept worker")

    def _handle_conn(self, conn, addr):
        try:
            raw_len = recvall(conn, 4)
            if not raw_len:
                conn.close()
                return
            length = int.from_bytes(raw_len, "big")
            raw = recvall(conn, length)
            msg = json.loads(raw.decode("utf-8"))
            typ = msg.get("type")
            if typ == "execute_task":
                resp = self._execute_task(msg)
                self._send_response(conn, resp)
            else:
                self._send_response(conn, {"ok": False, "error": "unknown"})
        except Exception:
            logger.exception("Erro handle_conn worker")
        finally:
            conn.close()

    def _send_response(self, conn, obj):
        data = json.dumps(obj).encode("utf-8")
        conn.sendall(len(data).to_bytes(4, "big"))
        conn.sendall(data)

    def _register(self):
        try:
            import socket, struct
            payload = {"type":"register_worker","worker_id":self.id,"host":self.host,"port":self.port}
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(3.0)
            s.connect((self.orch_host, self.orch_port))
            data = json.dumps(payload).encode("utf-8")
            s.sendall(struct.pack("!I", len(data)))
            s.sendall(data)
            raw_len = recvall(s, 4)
            if raw_len:
                l = int.from_bytes(raw_len, "big")
                raw = recvall(s, l)
                resp = json.loads(raw.decode("utf-8"))
                logger.info(f"Registro respondido: {resp}")
            s.close()
        except Exception:
            logger.exception("Falha ao registrar worker")

    def _heartbeat_loop(self):
        while self.running:
            try:
                if self.fail_simulate:
                    time.sleep(settings.HEARTBEAT_INTERVAL)
                    continue
                payload = {"type":"heartbeat","worker_id":self.id,"load":self.load,"host":self.host,"port":self.port,"lamport": self.lamport.tick()}
                import socket, struct
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(2.0)
                s.connect((self.orch_host, self.orch_port))
                data = json.dumps(payload).encode("utf-8")
                s.sendall(struct.pack("!I", len(data)))
                s.sendall(data)
                raw_len = recvall(s, 4)
                if raw_len:
                    l = int.from_bytes(raw_len, "big")
                    raw = recvall(s, l)
                    resp = json.loads(raw.decode("utf-8"))
                    if resp.get("ok"):
                        # update lamport
                        self.lamport.update(resp.get("lamport", 0))
                s.close()
            except Exception:
                logger.debug("Heartbeat falhou")
            time.sleep(settings.HEARTBEAT_INTERVAL)

    def _execute_task(self, msg):
        task_id = msg.get("task_id")
        payload = msg.get("payload")
        lam = msg.get("lamport", 0)
        self.lamport.update(lam)
        self.load += 1
        logger.info(f"Executando tarefa {task_id} payload={payload}")
        # simula processamento
        try:
            work_time = payload.get("duration", 2)
            # se payload pedir "crash": simular falha
            if payload.get("crash"):
                logger.warning("Simulando falha no worker")
                # parar heartbeats para simular falha
                self.fail_simulate = True
                raise RuntimeError("Simulated crash")
            time.sleep(work_time)
            result = {"ok": True, "task_id": task_id, "result": f"Processed by {self.id}"}
            logger.info(f"Tarefa {task_id} concluída")
        except Exception as e:
            logger.exception("Erro executando tarefa")
            result = {"ok": False, "task_id": task_id, "error": str(e)}
        finally:
            self.load = max(0, self.load - 1)
        return result

if __name__ == "__main__":
    w = Worker()
    w.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Worker encerrando...")
