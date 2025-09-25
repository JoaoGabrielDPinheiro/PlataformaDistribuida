import socket
import threading
import json
import time
import uuid
from config import settings
from lamport import LamportClock
from auth import SimpleAuth
from logger_setup import get_logger
from common import recvall, start_multicast_sender

logger = get_logger("orchestrator")

class Orchestrator:
    def __init__(self, host=settings.ORCHESTRATOR_HOST, port=settings.ORCHESTRATOR_PORT):
        self.host = host
        self.port = port
        self.server = None
        self.lock = threading.Lock()
        self.lamport = LamportClock()
        self.auth = SimpleAuth()
        # workers: worker_id -> {host, port, last_hb, load, status}
        self.workers = {}
        # tasks: task_id -> {client, payload, status, assigned_worker, lamport_ts}
        self.tasks = {}
        self.multicast_sock = start_multicast_sender(settings.BACKUP_MULTICAST_GROUP, settings.BACKUP_MULTICAST_PORT)
        self.running = True
        self.balance_policy = "least_load"  # grupo pode justificar essa escolha: minimiza tempo médio por distribuir para worker com menos carga

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
        self.server.bind((self.host, self.port))
        self.server.listen(10)
        while self.running:
            conn, addr = self.server.accept()
            threading.Thread(target=self._handle_conn, args=(conn, addr), daemon=True).start()

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
            else:
                self._send_response(conn, {"ok": False, "error": "unknown_type"})
        except Exception as e:
            logger.exception("Erro ao tratar conn")
        finally:
            conn.close()

    def _send_response(self, conn, obj):
        data = json.dumps(obj).encode("utf-8")
        conn.sendall(len(data).to_bytes(4, "big"))
        conn.sendall(data)

    def _register_worker(self, msg, addr):
        wid = msg.get("worker_id") or str(uuid.uuid4())
        host = msg.get("host") or addr[0]
        port = msg.get("port") or msg.get("listen_port")
        with self.lock:
            self.workers[wid] = {
                "host": host,
                "port": port,
                "last_hb": time.time(),
                "load": 0,
                "status": "active"
            }
            self.lamport.tick()
            logger.info(f"Worker registrado: {wid} em {host}:{port}")
        return {"ok": True, "worker_id": wid}

    def _handle_heartbeat(self, msg):
        wid = msg.get("worker_id")
        load = msg.get("load", 0)
        rtime = msg.get("lamport", 0)
        with self.lock:
            if wid in self.workers:
                self.workers[wid]["last_hb"] = time.time()
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
        return {"ok": True, "lamport": self.lamport.read()}

    def _choose_worker(self):
        with self.lock:
            alive = {k:v for k,v in self.workers.items() if v["status"]=="active"}
            if not alive:
                return None
            if self.balance_policy == "least_load":
                # escolhe worker de menor carga
                sorted_workers = sorted(alive.items(), key=lambda it: it[1]["load"])
                return sorted_workers[0][0]
            else:
                # fallback round-robin
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
                "created_at": time.time()
            }
            logger.info(f"Tarefa {task_id} submetida por {self.tasks[task_id]['client']} payload={payload} assigned={assigned}")
        if assigned:
            ok = self._send_task_to_worker(task_id, assigned)
            if not ok:
                # marca pendente para redistribuição
                with self.lock:
                    self.tasks[task_id]["status"] = "pending"
                    self.tasks[task_id]["assigned_worker"] = None
        self._checkpoint_state()
        return {"ok": True, "task_id": task_id, "lamport": lamport_ts}

    def _send_task_to_worker(self, task_id, worker_id):
        w = self.workers.get(worker_id)
        if not w:
            return False
        payload = {
            "type": "execute_task",
            "task_id": task_id,
            "payload": self.tasks[task_id]["payload"],
            "lamport": self.lamport.read()
        }
        try:
            resp = None
            # use TCP connect directly
            import socket, struct, json
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(4.0)
            s.connect((w["host"], int(w["port"])))
            data = json.dumps(payload).encode("utf-8")
            s.sendall(struct.pack("!I", len(data)))
            s.sendall(data)
            raw_len = recvall(s,4)
            if not raw_len:
                s.close()
                return False
            length = int.from_bytes(raw_len, "big")
            raw = recvall(s, length)
            resp = json.loads(raw.decode("utf-8"))
            s.close()
            if resp and resp.get("ok"):
                with self.lock:
                    self.workers[worker_id]["load"] += 1
                    self.tasks[task_id]["status"] = "running"
                    self.tasks[task_id]["assigned_worker"] = worker_id
                logger.info(f"Tarefa {task_id} enviada para worker {worker_id}")
                return True
            return False
        except Exception:
            logger.exception("Falha ao enviar tarefa para worker")
            with self.lock:
                if worker_id in self.workers:
                    self.workers[worker_id]["status"] = "failed"
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

    def _heartbeat_checker_loop(self):
        while self.running:
            now = time.time()
            to_mark = []
            with self.lock:
                for wid, meta in list(self.workers.items()):
                    if now - meta["last_hb"] > settings.WORKER_TIMEOUT:
                        to_mark.append(wid)
            for wid in to_mark:
                with self.lock:
                    self.workers[wid]["status"] = "failed"
                    logger.warning(f"Worker {wid} considerado falho (timeout). Reatribuindo tarefas.")
                    # reatribuir tarefas
                    for tid, t in self.tasks.items():
                        if t.get("assigned_worker") == wid and t.get("status") != "done":
                            t["status"] = "pending"
                            t["assigned_worker"] = None
                    # tentar redistribuir pendentes
                self._redistribute_pending()
            time.sleep(1.0)

    def _redistribute_pending(self):
        with self.lock:
            pendings = [tid for tid,t in self.tasks.items() if t["status"] in ("pending","assigned")]
        for tid in pendings:
            with self.lock:
                if self.tasks[tid]["status"] == "done":
                    continue
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
        import json
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
