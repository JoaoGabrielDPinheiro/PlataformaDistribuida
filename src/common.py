import socket
import json
import struct
from config import settings
from logger_setup import get_logger

logger = get_logger("common")

def send_json_tcp(host: str, port: int, obj: dict, timeout=5.0):
    data = json.dumps(obj).encode("utf-8")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(timeout)
        s.connect((host, port))
        s.sendall(struct.pack("!I", len(data)))
        s.sendall(data)
        # espera resposta (mesmo protocolo)
        try:
            raw_len = recvall(s, 4)
            if not raw_len:
                return None
            length = struct.unpack("!I", raw_len)[0]
            resp = recvall(s, length)
            return json.loads(resp.decode("utf-8"))
        except socket.timeout:
            return None

def recvall(sock: socket.socket, n: int):
    data = b""
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data += packet
    return data

def start_multicast_sender(group_ip: str, port: int):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    ttl = struct.pack('b', 1)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
    return sock

def start_multicast_listener(group_ip: str, port: int):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind(('', port))
    except OSError:
        sock.bind((group_ip, port))
    mreq = struct.pack("4sl", socket.inet_aton(group_ip), socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    return sock
