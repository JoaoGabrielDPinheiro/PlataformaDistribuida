# src/backup.py
import threading
import time
import json
import socket
import traceback

import config.settings as settings
from src.common import create_multicast_listener, recv_json_udp
from src.logger_setup import get_logger

logger = get_logger("Backup")

class BackupOrchestrator:
    def __init__(self):
        self.state = {}
        self.last_state_time = time.time()
        self.running = True
        self.assumed_orchestrator = None

    def start(self):
        try:
            sock = create_multicast_listener(settings.BACKUP_MULTICAST_GROUP, settings.BACKUP_MULTICAST_PORT)
        except Exception:
            logger.exception("Falha ao criar socket multicast")
            return

        t = threading.Thread(target=self._failover_monitor, daemon=True)
        t.start()

        logger.info("Backup iniciado: escutando estado do orquestrador principal via multicast")
        while self.running:
            try:
                msg, addr = recv_json_udp(sock)
                if msg.get("type") == "state_sync":
                    self.state = msg.get("state", {})
                    self.last_state_time = time.time()
            except Exception:
                # ler exceptions mas não parar
                time.sleep(0.1)

    def _failover_monitor(self):
        while self.running:
            time.sleep(1.0)
            if time.time() - self.last_state_time > 5.0 and self.assumed_orchestrator is None:
                logger.warning("Backup: não recebe estado - assumindo orquestrador principal.")
                # iniciar orquestrador local com o estado que tem
                try:
                    from src.orchestrator import Orchestrator
                    self.assumed_orchestrator = Orchestrator(initial_state=self.state)
                    threading.Thread(target=self.assumed_orchestrator.start, daemon=True).start()
                    logger.info("Backup: Orchestrador local iniciado (failover).")
                except Exception:
                    logger.exception("Backup: erro ao iniciar orquestrador local")
                    traceback.print_exc()
                break

if __name__ == "__main__":
    BackupOrchestrator().start()
