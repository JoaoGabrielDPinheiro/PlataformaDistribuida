# src/orchestrator.py
import socket
import threading
import json
import time
import uuid
import struct
import os

import config.settings as settings
from src.common import recvall, send_json_tcp, recv_json_tcp, send_json_udp, create_multicast_sender
from src.lamport import LamportClock
from src.auth import SimpleAuth
from src.logger_setup import get_logger

logger = get_logger("Orchestrator")

class Orchestrator:
    def __init__(self, host=None, port=None, initial_state=None):
        self.host = host or settings.ORCHESTRATOR_HOST
        self.port = port or settings.ORCHESTRATOR_PORT
        self.lamport = LamportClock()
        self.auth = SimpleAuth()
        self.workers = {}  # worker_id -> {host, port, last_hb, load, status}
        self.tasks = {}    # task_id -> {client, payload, status, assigned_worker, result, timestamps}
        self.lock = threading.Lock()
        self.running = True

        # restore if provided (from backup)
        if initial_state:
            try:
                with self.lock:
                    self.workers = initial_state.get("workers", {}) or {}
                    self.tasks = initial_state.get("tasks", {}) or {}
                    self.lamport.update(initial_state.get("lamport", 0) or 0)
                logger.info("Orchestrator: estado inicial carregado (do backup).")
            except Exception:
                logger.exception("Erro ao carregar estado inicial")

        # tenta carregar checkpoint do disco
        self._load_checkpoint_on_startup()

    def _load_checkpoint_on_startup(self):
        if os.path.exists(settings.STATE_CHECKPOINT_FILE):
            try:
                with open(settings.STATE_CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                    st = json.load(f)
                with self.lock:
                    self.workers = st.get("workers", {}) or {}
                    self.tasks = st.get("tasks", {}) or {}
                    self.lamport.update(st.get("lamport", 0) or 0)
                logger.info("Orchestrator: checkpoint carregado do disco.")
            except Exception:
                logger.exception("Erro ao carregar checkpoint")

    def start(self):
        # threads auxiliares
        threading.Thread(target=self._heartbeat_checker_loop, daemon=True).start()
        threading.Thread(target=self._state_multicast_loop, daemon=True).start()

        # servidor TCP principal
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(50)
        logger.info(f"Orchestrator iniciado em {self.host}:{self.port}")

        try:
            while self.running:
                conn, addr = server.accept()
                threading.Thread(target=self._handle_conn, args=(conn, addr), daemon=True).start()
        except KeyboardInterrupt:
            logger.info("Orchestrator encerrando (KeyboardInterrupt).")
        except Exception:
            logger.exception("Erro no servidor principal")
        finally:
            server.close()

    def _handle_conn(self, conn: socket.socket, addr):
        try:
            msg = recv_json_tcp(conn)
            if not msg:
                return
            typ = msg.get("type")
            # aceitar login/auth tanto 'auth' quanto 'login'
            if typ in ("auth", "login"):
                token = self.auth.login(msg.get("username"), msg.get("password"))
                send_json_tcp(conn, {"ok": bool(token), "token": token})
            elif typ == "register_worker":
                wid = msg.get("worker_id") or msg.get("id") or str(uuid.uuid4())
                host = msg.get("host") or addr[0]
                port = int(msg.get("port"))
                with self.lock:
                    self.workers[wid] = {"host": host, "port": port, "last_hb": time.time(), "load": 0, "status": "active"}
                    self.lamport.tick()
                logger.info(f"Worker registrado: {wid} @ {host}:{port}")
                self._checkpoint_state()
                send_json_tcp(conn, {"ok": True, "worker_id": wid})
            elif typ == "heartbeat":
                wid = msg.get("worker_id") or msg.get("id")
                with self.lock:
                    if wid in self.workers:
                        self.workers[wid]["last_hb"] = time.time()
                        self.workers[wid]["load"] = int(msg.get("load", self.workers[wid].get("load", 0)))
                        self.lamport.update(msg.get("lamport", 0) or 0)
                send_json_tcp(conn, {"ok": True, "lamport": self.lamport.read()})
            elif typ == "submit_task":
                token = msg.get("token")
                if not self.auth.validate(token):
                    send_json_tcp(conn, {"ok": False, "error": "unauthenticated"})
                    return
                payload = msg.get("payload", {})
                task_id = str(uuid.uuid4())
                with self.lock:
                    self.tasks[task_id] = {
                        "client": self.auth.user_of(token),
                        "payload": payload,
                        "status": "pending",
                        "assigned_worker": None,
                        "result": None,
                        "created_at": time.time()
                    }
                    self.lamport.tick()
                # tentar atribuir imediatamente
                assigned = self._assign_task(task_id)
                self._checkpoint_state()
                send_json_tcp(conn, {"ok": True, "task_id": task_id, "assigned": bool(assigned)})
            elif typ in ("query_task", "query_status"):
                token = msg.get("token")
                if not self.auth.validate(token):
                    send_json_tcp(conn, {"ok": False, "error": "unauthenticated"})
                    return
                tid = msg.get("task_id")
                with self.lock:
                    t = self.tasks.get(tid)
                if t:
                    send_json_tcp(conn, {"ok": True, "task": t})
                else:
                    send_json_tcp(conn, {"ok": False, "error": "task_not_found"})
            elif typ == "task_result":
                # worker pode enviar resultado de forma assíncrona
                tid = msg.get("task_id")
                with self.lock:
                    if tid in self.tasks:
                        self.tasks[tid]["status"] = "done" if msg.get("ok", True) else "failed"
                        self.tasks[tid]["result"] = msg.get("result") or msg.get("error")
                        self.tasks[tid]["finished_at"] = time.time()
                        # reduzir carga se worker known
                        wid = msg.get("worker_id")
                        if wid and wid in self.workers:
                            self.workers[wid]["load"] = max(0, int(self.workers[wid].get("load", 1)) - 1)
                        self._checkpoint_state()
                send_json_tcp(conn, {"ok": True})
            else:
                send_json_tcp(conn, {"ok": False, "error": "unknown_type"})
        except Exception:
            logger.exception("Erro ao tratar conexão")
        finally:
            try:
                conn.close()
            except:
                pass

    def _assign_task(self, task_id):
        """Escolhe worker com menor carga (Least Load) e envia a tarefa (bloqueante - aguarda resposta)."""
        with self.lock:
            # filtrar ativos
            alive = {wid: w for wid, w in self.workers.items() if w.get("status") == "active"}
            if not alive:
                logger.info("Nenhum worker disponível para atribuir tarefa")
                return False
            # escolher por menor carga
            chosen_id, chosen_meta = min(alive.items(), key=lambda it: int(it[1].get("load", 0)))
            # marcar tentative
            self.tasks[task_id]["assigned_worker"] = chosen_id
            self.tasks[task_id]["status"] = "assigned"
            # incrementar carga estimada
            self.workers[chosen_id]["load"] = int(self.workers[chosen_id].get("load", 0)) + 1
            self.lamport.tick()
        # enviar tarefa via TCP e aguardar resposta (síncrono)
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(10.0)
            s.connect((chosen_meta["host"], int(chosen_meta["port"])))
            payload = {
                "type": "execute_task",
                "task_id": task_id,
                "payload": self.tasks[task_id]["payload"],
                "lamport": self.lamport.read()
            }
            send_json_tcp(s, payload)
            # esperar resposta do worker no mesmo socket
            resp = recv_json_tcp(s)
            s.close()
            with self.lock:
                if resp and resp.get("ok"):
                    self.tasks[task_id]["status"] = "done"
                    self.tasks[task_id]["result"] = resp.get("result")
                    self.tasks[task_id]["finished_at"] = time.time()
                    # decrementar carga
                    self.workers[chosen_id]["load"] = max(0, int(self.workers[chosen_id].get("load", 1)) - 1)
                    logger.info(f"Tarefa {task_id} concluída pelo worker {chosen_id}")
                else:
                    # falha no worker => marcar pendente e reduzir carga
                    logger.warning(f"Tarefa {task_id} retornou erro do worker {chosen_id} ou resp nula")
                    self.tasks[task_id]["status"] = "pending"
                    self.tasks[task_id]["assigned_worker"] = None
                    self.workers[chosen_id]["status"] = "failed"
                    self.workers[chosen_id]["load"] = max(0, int(self.workers[chosen_id].get("load", 1)) - 1)
            self._checkpoint_state()
            return True
        except Exception:
            logger.exception(f"Erro ao enviar tarefa {task_id} para worker {chosen_id}")
            with self.lock:
                # rollback marcações
                self.tasks[task_id]["status"] = "pending"
                self.tasks[task_id]["assigned_worker"] = None
                if chosen_id in self.workers:
                    self.workers[chosen_id]["status"] = "failed"
                    self.workers[chosen_id]["load"] = max(0, int(self.workers[chosen_id].get("load", 1)) - 1)
            self._checkpoint_state()
            return False

    def _heartbeat_checker_loop(self):
        while True:
            time.sleep(1.0)
            now = time.time()
            dead = []
            with self.lock:
                for wid, meta in list(self.workers.items()):
                    if now - meta.get("last_hb", 0) > settings.WORKER_TIMEOUT:
                        logger.warning(f"Worker {wid} considerado falho (timeout).")
                        meta["status"] = "failed"
                        dead.append(wid)
                # reatribuir tarefas de workers falhos
                for tid, t in self.tasks.items():
                    wid = t.get("assigned_worker")
                    if wid in dead and t.get("status") != "done":
                        t["status"] = "pending"
                        t["assigned_worker"] = None
            # tentar redistribuir pendentes
            with self.lock:
                pendings = [tid for tid, t in self.tasks.items() if t["status"] == "pending"]
            for tid in pendings:
                self._assign_task(tid)
            # salvar checkpoint
            self._checkpoint_state()

    def _state_multicast_loop(self):
        sock = create_multicast_sender(ttl=1)
        group_addr = (settings.BACKUP_MULTICAST_GROUP, settings.BACKUP_MULTICAST_PORT)
        while True:
            try:
                state = {"workers": self.workers, "tasks": self.tasks, "lamport": self.lamport.read(), "timestamp": time.time()}
                send_json_udp(sock, {"type": "state_sync", "state": state}, group_addr)
            except Exception:
                logger.exception("Erro no envio multicast do estado")
            time.sleep(1.0)

    def _checkpoint_state(self):
        try:
            state = {"workers": self.workers, "tasks": self.tasks, "lamport": self.lamport.read()}
            with open(settings.STATE_CHECKPOINT_FILE, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2, default=str)
        except Exception:
            logger.exception("Erro ao salvar checkpoint")

if __name__ == "__main__":
    Orchestrator().start()
