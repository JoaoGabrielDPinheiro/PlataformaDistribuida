# src/orchestrator.py
import os, sys
ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import socket
import threading
import json
import time
import uuid
import struct
from config import settings
from lamport import LamportClock
from auth import SimpleAuth
from logger_setup import get_logger
from common import recvall, start_multicast_sender

logger = get_logger("orchestrator")

class Orchestrator:
    def __init__(self, host=settings.ORCHESTRATOR_HOST, port=settings.ORCHESTRATOR_PORT, initial_state=None):
        self.host = host
        self.port = port
        self.server = None
        self.lock = threading.Lock()
        self.lamport = LamportClock()
        self.auth = SimpleAuth()
        self.workers = {}
        self.tasks = {}
        self.multicast_sock = start_multicast_sender(settings.BACKUP_MULTICAST_GROUP, settings.BACKUP_MULTICAST_PORT)
        self.running = True
        self.balance_policy = "least_load"

        # restaurar estado passado (ex: backup ao assumir)
        if initial_state:
            try:
                self.workers = initial_state.get("workers", {}) or {}
                self.tasks = initial_state.get("tasks", {}) or {}
                lam = initial_state.get("lamport", 0)
                # ajustar relógio lamport
                self.lamport.time = lam
                logger.info("Estado inicial carregado no orquestrador (from backup).")
            except Exception:
                logger.exception("Falha ao carregar estado inicial")

        # tentar carregar checkpoint local se existir
        self._load_checkpoint_on_startup()

    def _load_checkpoint_on_startup(self):
        try:
            if os.path.exists(settings.STATE_CHECKPOINT_FILE):
                with open(settings.STATE_CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                    st = json.load(f)
                    with self.lock:
                        self.workers = st.get("workers", {}) or {}
                        self.tasks = st.get("tasks", {}) or {}
                        lam = st.get("lamport", 0)
                        self.lamport.time = lam
                    logger.info("Checkpoint carregado do disco.")
        except Exception:
            logger.exception("Erro ao carregar checkpoint na inicialização")

    def start(self):
        t = threading.Thread(target=self._tcp_server_loop, daemon=True)
        t.start()
        t2 = threading.Thread(target=self._heartbeat_checker_loop, daemon=True)
        t2.start()
        t3 = threading.Thread(target=self._state_multicast_loop, daemon=True)
        t3.start()
        logger.info(f"Orquestrador iniciado em {self.host}:{self.port}")

    def _tcp_server_loop(self):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((self.host, self.port))
        self.server.listen(10)
        while self.running:
            try:
                conn, addr = self.server.accept()
                threading.Thread(target=self._handle_conn, args=(conn, addr), daemon=True).start()
            except Exception:
                logger.exception("Erro accept no servidor TCP do orquestrador")

    def _handle_conn(self, conn: socket.socket, addr):
        try:
            raw_len = recvall(conn, 4)
            if not raw_len:
                conn.close()
                return
            length = int.from_bytes(raw_len, "big")
            raw = recvall(conn, length)
            msg = json.loads(raw.decode("utf-8"))
            typ = msg.get("type")
            if typ == "register_worker":
                resp = self._register_worker(msg, addr)
                self._send_response(conn, resp)
            elif typ == "heartbeat":
                resp = self._handle_heartbeat(msg)
                self._send_response(conn, resp)
            elif typ == "submit_task":
                resp = self._handle_submit_task(msg)
                self._send_response(conn, resp)
            elif typ == "query_status":
                resp = self._handle_query(msg)
                self._send_response(conn, resp)
            elif typ == "auth":
                token = self.auth.login(msg.get("username"), msg.get("password"))
                self._send_response(conn, {"ok": bool(token), "token": token})
            elif typ == "task_result":
                # caso um worker envie resultado assíncrono (extensão)
                resp = self._handle_task_result(msg)
                self._send_response(conn, resp)
            else:
                self._send_response(conn, {"ok": False, "error": "unknown_type"})
        except Exception:
            logger.exception("Erro ao tratar conexão no orquestrador")
        finally:
            try:
                conn.close()
            except:
                pass

    def _send_response(self, conn, obj):
        try:
            data = json.dumps(obj).encode("utf-8")
            conn.sendall(struct.pack("!I", len(data)))
            conn.sendall(data)
        except Exception:
            logger.exception("Erro enviando resposta TCP")

    def _register_worker(self, msg, addr):
        wid = msg.get("worker_id") or str(uuid.uuid4())
        host = msg.get("host") or addr[0]
        port = msg.get("port") or msg.get("listen_port")
        with self.lock:
            self.workers[wid] = {
                "host": host,
                "port": port,
                "last_hb": time.time(),
                "load": int(msg.get("load", 0)),
                "status": "active"
            }
            self.lamport.tick()
            logger.info(f"Worker registrado: {wid} em {host}:{port}")
        self._checkpoint_state()
        return {"ok": True, "worker_id": wid}

    def _handle_heartbeat(self, msg):
        wid = msg.get("worker_id")
        load = int(msg.get("load", 0))
        rtime = msg.get("lamport", 0)
        with self.lock:
            if wid in self.workers:
                self.workers[wid]["last_hb"] = time.time()
                # atualizar carga reportada
                self.workers[wid]["load"] = load
                self.lamport.update(rtime)
            else:
                # aceitar registro via heartbeat
                self.workers[wid] = {
                    "host": msg.get("host"),
                    "port": msg.get("port"),
                    "last_hb": time.time(),
                    "load": load,
                    "status": "active"
                }
        self._checkpoint_state()
        return {"ok": True, "lamport": self.lamport.read()}

    def _choose_worker(self):
        with self.lock:
            alive = {k:v for k,v in self.workers.items() if v.get("status")=="active"}
            if not alive:
                return None
            if self.balance_policy == "least_load":
                sorted_workers = sorted(alive.items(), key=lambda it: int(it[1].get("load",0)))
                return sorted_workers[0][0]
            else:
                return list(alive.keys())[0]

    def _handle_submit_task(self, msg):
        token = msg.get("token")
        if not self.auth.validate(token):
            return {"ok": False, "error": "unauthenticated"}
        payload = msg.get("payload")
        task_id = str(uuid.uuid4())
        lamport_ts = self.lamport.tick()
        assigned = self._choose_worker()
        with self.lock:
            self.tasks[task_id] = {
                "client": self.auth.user_of(token),
                "payload": payload,
                "status": "assigned" if assigned else "pending",
                "assigned_worker": assigned,
                "lamport_ts": lamport_ts,
                "created_at": time.time(),
                "result": None
            }
            logger.info(f"Tarefa {task_id} submetida por {self.tasks[task_id]['client']} payload={payload} assigned={assigned}")
        if assigned:
            ok = self._send_task_to_worker(task_id, assigned)
            if not ok:
                with self.lock:
                    self.tasks[task_id]["status"] = "pending"
                    self.tasks[task_id]["assigned_worker"] = None
        self._checkpoint_state()
        return {"ok": True, "task_id": task_id, "lamport": lamport_ts}

    def _send_task_to_worker(self, task_id, worker_id):
        w = self.workers.get(worker_id)
        if not w:
            return False
        # preparar payload para o worker
        payload = {
            "type": "execute_task",
            "task_id": task_id,
            "payload": self.tasks[task_id]["payload"],
            "lamport": self.lamport.read()
        }
        try:
            # marcar como running e incrementar load antes de enviar
            with self.lock:
                self.tasks[task_id]["status"] = "running"
                self.tasks[task_id]["assigned_worker"] = worker_id
                self.tasks[task_id]["started_at"] = time.time()
                # incrementar load estimada
                w["load"] = int(w.get("load",0)) + 1

            # enviar via TCP e aguardar resposta (sincrono)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(10.0)
            s.connect((w["host"], int(w["port"])))
            data = json.dumps(payload).encode("utf-8")
            s.sendall(struct.pack("!I", len(data)))
            s.sendall(data)
            raw_len = recvall(s, 4)
            if not raw_len:
                s.close()
                raise RuntimeError("No response from worker")
            length = int.from_bytes(raw_len, "big")
            raw = recvall(s, length)
            resp = json.loads(raw.decode("utf-8")) if raw else None
            s.close()

            # processar resposta do worker
            with self.lock:
                # reduzir carga (o worker terminou)
                w["load"] = max(0, int(w.get("load",0)) - 1)
                if resp and resp.get("ok"):
                    self.tasks[task_id]["status"] = "done"
                    self.tasks[task_id]["result"] = resp.get("result")
                    self.tasks[task_id]["finished_at"] = time.time()
                    logger.info(f"Tarefa {task_id} concluída pelo worker {worker_id}")
                else:
                    # erro na execução => marcar pendente e sinalizar falha se necessário
                    self.tasks[task_id]["status"] = "pending"
                    self.tasks[task_id]["assigned_worker"] = None
                    logger.warning(f"Tarefa {task_id} retornou erro do worker {worker_id}; será reatribuída")
                    # considerar worker falho
                    self.workers[worker_id]["status"] = "failed"
            self._checkpoint_state()
            return True
        except Exception:
            logger.exception("Falha ao enviar/receber tarefa do worker")
            # marcar worker como failed e reatribuir
            with self.lock:
                if worker_id in self.workers:
                    self.workers[worker_id]["status"] = "failed"
                    self.workers[worker_id]["load"] = max(0, int(self.workers[worker_id].get("load",0)) - 1)
                # reset task para pending
                if task_id in self.tasks:
                    self.tasks[task_id]["status"] = "pending"
                    self.tasks[task_id]["assigned_worker"] = None
            self._checkpoint_state()
            return False

    def _handle_query(self, msg):
        token = msg.get("token")
        if not self.auth.validate(token):
            return {"ok": False, "error": "unauthenticated"}
        task_id = msg.get("task_id")
        with self.lock:
            if task_id in self.tasks:
                return {"ok": True, "task": self.tasks[task_id], "lamport": self.lamport.read()}
            else:
                return {"ok": False, "error": "task_not_found"}

    def _handle_task_result(self, msg):
        # endpoint opcional para worker enviar resultado assíncrono
        task_id = msg.get("task_id")
        ok = msg.get("ok", False)
        with self.lock:
            if task_id in self.tasks:
                if ok:
                    self.tasks[task_id]["status"] = "done"
                    self.tasks[task_id]["result"] = msg.get("result")
                    self.tasks[task_id]["finished_at"] = time.time()
                else:
                    self.tasks[task_id]["status"] = "pending"
                    self.tasks[task_id]["assigned_worker"] = None
                self._checkpoint_state()
                return {"ok": True}
            else:
                return {"ok": False, "error": "task_not_found"}

    def _heartbeat_checker_loop(self):
        while self.running:
            now = time.time()
            to_mark = []
            with self.lock:
                for wid, meta in list(self.workers.items()):
                    if now - meta.get("last_hb", 0) > settings.WORKER_TIMEOUT:
                        to_mark.append(wid)
            for wid in to_mark:
                with self.lock:
                    if wid in self.workers:
                        self.workers[wid]["status"] = "failed"
                        logger.warning(f"Worker {wid} considerado falho (timeout). Reatribuindo tarefas.")
                        # reatribuir tarefas atribuídas a esse worker
                        for tid, t in self.tasks.items():
                            if t.get("assigned_worker") == wid and t.get("status") != "done":
                                t["status"] = "pending"
                                t["assigned_worker"] = None
                # tentar redistribuir pendentes fora do lock
                self._redistribute_pending()
            time.sleep(1.0)

    def _redistribute_pending(self):
        with self.lock:
            pendings = [tid for tid,t in self.tasks.items() if t["status"] in ("pending",)]
        for tid in pendings:
            chosen = self._choose_worker()
            if chosen:
                self._send_task_to_worker(tid, chosen)
        self._checkpoint_state()

    def _state_multicast_loop(self):
        while self.running:
            try:
                state = self._gather_state()
                msg = json.dumps({"type":"state_sync","state":state}).encode("utf-8")
                self.multicast_sock.sendto(msg, (settings.BACKUP_MULTICAST_GROUP, settings.BACKUP_MULTICAST_PORT))
            except Exception:
                logger.exception("Erro no multicast state")
            time.sleep(1.0)

    def _gather_state(self):
        with self.lock:
            return {
                "workers": self.workers,
                "tasks": self.tasks,
                "lamport": self.lamport.read(),
                "timestamp": time.time()
            }

    def _checkpoint_state(self):
        try:
            state = self._gather_state()
            with open(settings.STATE_CHECKPOINT_FILE, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2, default=str)
        except Exception:
            logger.exception("Erro ao salvar checkpoint")

if __name__ == "__main__":
    o = Orchestrator()
    o.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Orquestrador encerrando...")
