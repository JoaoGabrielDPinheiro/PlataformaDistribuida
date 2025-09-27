# src/worker.py
import threading
import socket
import time
import json
import struct
import argparse
import uuid
import traceback

import config.settings as settings
from src.common import send_json_tcp, recv_json_tcp
from src.logger_setup import get_logger

logger = get_logger("Worker")

class Worker:
    def __init__(self, host="127.0.0.1", port=0, worker_id=None):
        self.host = host
        self.port = port  # 0 = escolher porta
        self.id = worker_id or str(uuid.uuid4())[:8]
        self.lamport = 0
        self.load = 0
        self.running = True
        self.server = None
        self.fail_simulate = False

    def start(self):
        # iniciar servidor em thread
        t = threading.Thread(target=self._server_loop, daemon=True)
        t.start()
        # aguardar porta definida
        timeout = time.time() + 3.0
        while (self.port == 0 or self.server is None) and time.time() < timeout:
            time.sleep(0.05)
        # registrar
        self._register()
        # iniciar heartbeat
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        logger.info(f"Worker {self.id} iniciado e registrado (porta {self.port})")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Worker encerrando...")

    def _server_loop(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.host, self.port))
        s.listen(10)
        self.server = s
        self.port = s.getsockname()[1]
        while True:
            try:
                conn, addr = s.accept()
                threading.Thread(target=self._handle_conn, args=(conn, addr), daemon=True).start()
            except Exception:
                logger.exception("Erro accept worker")

    def _handle_conn(self, conn: socket.socket, addr):
        try:
            msg = recv_json_tcp(conn)
            if not msg:
                conn.close()
                return
            typ = msg.get("type")
            if typ == "execute_task":
                resp = self._execute_task(msg)
                # enviar resposta no mesmo socket (sincrono)
                send_json_tcp(conn, resp)
            else:
                send_json_tcp(conn, {"ok": False, "error": "unknown_type"})
        except Exception:
            logger.exception("Erro handle_conn worker")
        finally:
            try:
                conn.close()
            except:
                pass

    def _register(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(3.0)
            s.connect((settings.ORCHESTRATOR_HOST, settings.ORCHESTRATOR_PORT))
            payload = {"type": "register_worker", "worker_id": self.id, "host": self.host, "port": self.port}
            send_json_tcp(s, payload)
            resp = recv_json_tcp(s)
            s.close()
            logger.info(f"Registro resposta: {resp}")
        except Exception:
            logger.exception("Falha ao registrar worker")

    def _heartbeat_loop(self):
        while True:
            try:
                if self.fail_simulate:
                    # simular falha: não envia heartbeat
                    time.sleep(settings.HEARTBEAT_INTERVAL)
                    continue
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(3.0)
                s.connect((settings.ORCHESTRATOR_HOST, settings.ORCHESTRATOR_PORT))
                payload = {"type": "heartbeat", "worker_id": self.id, "load": self.load, "lamport": self.lamport}
                send_json_tcp(s, payload)
                resp = recv_json_tcp(s)
                if resp and resp.get("lamport") is not None:
                    # não usamos Lamport completo aqui, só guardamos se quiser
                    self.lamport = max(self.lamport, int(resp.get("lamport", 0)))
                s.close()
            except Exception:
                # falha em conectar heartbeat é aceitável se orchestrator estiver offline
                pass
            time.sleep(settings.HEARTBEAT_INTERVAL)

    def _execute_task(self, msg: dict):
        tid = msg.get("task_id")
        payload = msg.get("payload") or {}
        self.load += 1
        logger.info(f"Worker {self.id} executando tarefa {tid} payload={payload}")
        try:
            # simular crash se solicitado
            if payload.get("crash"):
                logger.warning("Worker simulando crash (parando heartbeats).")
                self.fail_simulate = True
                raise RuntimeError("Simulated crash")
            duration = float(payload.get("duration", 1))
            time.sleep(duration)
            result = {"ok": True, "task_id": tid, "worker_id": self.id, "result": f"Processed by {self.id}"}
            logger.info(f"Worker {self.id} completou tarefa {tid}")
            return result
        except Exception as e:
            logger.exception("Erro executando tarefa")
            return {"ok": False, "task_id": tid, "worker_id": self.id, "error": str(e)}
        finally:
            # apenas ajustar load se não simulou crash permanente
            if not self.fail_simulate:
                self.load = max(0, self.load - 1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--id", default=None)
    args = parser.parse_args()
    w = Worker(host=args.host, port=args.port, worker_id=args.id)
    w.start()
