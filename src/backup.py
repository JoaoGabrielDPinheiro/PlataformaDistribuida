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
                logger.exception("Erro ao receber multicast")
            time.sleep(0.001)

    def _failover_detector_loop(self):
        while self.running:
            # se há mais de 5*interval sem mensagem => assumir primário
            if time.time() - self.last_state_time > 5.0:
                if not self.is_primary:
                    logger.warning("Perda de contato com orquestrador principal. Assumindo papel de orquestrador principal (failover).")
                    self.is_primary = True
                    # aqui, idealmente, ativar serviços TCP para clientes/workers (simulado)
                    # para a entrega acadêmica, podemos escrever state em arquivo e abrir porta para novos clients.
                # se já é primário, poderia tentar operar; por simplicidade, apenas mantém estado e log
            time.sleep(1.0)

    def _checkpoint(self):
        try:
            import json
            with open(settings.STATE_CHECKPOINT_FILE + ".backup", "w", encoding="utf-8") as f:
                json.dump(self.state, f, indent=2, default=str)
        except Exception:
            logger.exception("Erro ao salvar checkpoint backup")

if __name__ == "__main__":
    b = BackupOrchestrator()
    b.start()
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        logger.info("Backup encerrando...")
