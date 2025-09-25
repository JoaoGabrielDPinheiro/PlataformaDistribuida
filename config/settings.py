import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Endpoints e portas
ORCHESTRATOR_HOST = "127.0.0.1"
ORCHESTRATOR_PORT = 50000

BACKUP_MULTICAST_GROUP = "224.1.1.1"
BACKUP_MULTICAST_PORT = 50001

WORKER_REG_PORT = 50002  # (não usado separadamente; workers se registram via TCP no orquestrador)
HEARTBEAT_INTERVAL = 2.0  # segundos
WORKER_TIMEOUT = 6.0  # segundos sem heartbeat => falha
STATE_CHECKPOINT_FILE = os.path.join(BASE_DIR, "..", "state_checkpoint.json")
LOG_DIR = os.path.join(BASE_DIR, "..", "logs")
