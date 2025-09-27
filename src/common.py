# src/common.py
import socket
import struct
import json

def recvall(sock: socket.socket, n: int):
    """Recebe exatamente n bytes do socket (ou None se EOF)."""
    data = b""
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data += packet
    return data

def send_json_tcp(sock: socket.socket, obj: dict):
    """Envia um objeto JSON com comprimento prefixado (4 bytes, network order)."""
    data = json.dumps(obj).encode("utf-8")
    sock.sendall(struct.pack("!I", len(data)))
    sock.sendall(data)

def recv_json_tcp(sock: socket.socket):
    """Recebe um JSON prefixado por 4 bytes e retorna o objeto (ou None)."""
    raw_len = recvall(sock, 4)
    if not raw_len:
        return None
    length = struct.unpack("!I", raw_len)[0]
    raw = recvall(sock, length)
    if raw is None:
        return None
    return json.loads(raw.decode("utf-8"))

def send_json_udp(sock: socket.socket, obj: dict, addr):
    data = json.dumps(obj).encode("utf-8")
    sock.sendto(data, addr)

def recv_json_udp(sock: socket.socket, bufsize=65536):
    data, addr = sock.recvfrom(bufsize)
    return json.loads(data.decode("utf-8")), addr

def create_multicast_sender(ttl=1):
    """Cria um socket UDP para enviar multicast (não bind)."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    # TTL para multicast
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, struct.pack('b', ttl))
    return sock

def create_multicast_listener(group: str, port: int):
    """Cria e retorna um socket UDP configurado para receber multicast."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind(("", port))
    except Exception:
        sock.bind((group, port))
    mreq = struct.pack("4s4s", socket.inet_aton(group), socket.inet_aton("0.0.0.0"))
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    return sock
