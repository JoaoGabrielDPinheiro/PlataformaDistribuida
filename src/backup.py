# src/backup.py
import os, sys
ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import threading
import json
import time
from config import settings
from common import start_multicast_listener
from lamport import LamportClock
from logger_setup import get_logger

logger = get_logger("backup")

class BackupOrchestrator:
    def __init__(self):
        self.lamport = LamportClock()
        self.state = {}
        self.multicast_sock = start_multicast_listener(settings.BACKUP_MULTICAST_GROUP, settings.BACKUP_MULTICAST_PORT)
        self.running = True
        self.last_state_time = time.time()
        self.is_primary = False
        self.assumed_orchestrator = None

    def start(self):
        t = threading.Thread(target=self._listen_loop, daemon=True)
        t.start()
        t2 = threading.Thread(target=self._failover_detector_loop, daemon=True)
        t2.start()
        logger.info("Backup Orchestrador iniciado; aguardando multicast do orquestrador principal")

    def _listen_loop(self):
        while self.running:
            try:
                data, addr = self.multicast_sock.recvfrom(65536)
                msg = json.loads(data.decode("utf-8"))
                if msg.get("type") == "state_sync":
                    self.state = msg.get("state")
                    lam = self.state.get("lamport", 0)
                    self.lamport.update(lam)
                    self.last_state_time = time.time()
                    # guardar checkpoint local
                    self._checkpoint()
            except Exception:
                # É normal gerar exceções ao encerrar
                logger.debug("Erro/timeout recebendo multicast (ou socket fechado).")
            time.sleep(0.01)

    def _failover_detector_loop(self):
        while self.running:
            # se há mais de 5s sem mensagem => assumir primário
            if time.time() - self.last_state_time > 5.0 and not self.is_primary:
                logger.warning("Perda de contato com orquestrador principal. Tentando assumir (failover).")
                self.is_primary = True
                # iniciar um orquestrador local com o estado que temos
                try:
                    from orchestrator import Orchestrator
                    # iniciar em uma thread separado para não bloquear o backup
                    self.assumed_orchestrator = Orchestrator(initial_state=self.state)
                    threading.Thread(target=self.assumed_orchestrator.start, daemon=True).start()
                    logger.info("Orquestrador local iniciado pelo backup.")
                except Exception:
                    logger.exception("Erro ao iniciar orquestrador local a partir do backup")
            time.sleep(1.0)

    def _checkpoint(self):
        try:
            with open(settings.STATE_CHECKPOINT_FILE + ".backup", "w", encoding="utf-8") as f:
                json.dump(self.state, f, indent=2, default=str)
        except Exception:
            logger.exception("Erro ao salvar checkpoint backup")

if __name__ == "__main__":
    b = BackupOrchestrator()
    b.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Backup encerrando...")
