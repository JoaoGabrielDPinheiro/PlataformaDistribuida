# config/settings.py
import os

# BASE_DIR -> pasta raiz do projeto (uma pasta acima de config/)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Endpoints e portas
ORCHESTRATOR_HOST = "127.0.0.1"
ORCHESTRATOR_PORT = 50000

# Multicast (backup)
BACKUP_MULTICAST_GROUP = "224.1.1.1"
BACKUP_MULTICAST_PORT = 50001

# Intervalos/Timeouts
HEARTBEAT_INTERVAL = 2.0  # segundos entre heartbeats do worker
WORKER_TIMEOUT = 6.0      # timeout para considerar worker morto

# Arquivos e diretórios
STATE_CHECKPOINT_FILE = os.path.join(BASE_DIR, "state_checkpoint.json")
LOG_DIR = os.path.join(BASE_DIR, "logs")
