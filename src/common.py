# src/common.py
import os
import sys
import socket
import json
import struct
from typing import Optional, Dict, Any
from contextlib import closing

from config import settings
from logger_setup import get_logger

logger = get_logger("common")

# Constantes do protocolo
LENGTH_PREFIX_SIZE = 4  # número de bytes do prefixo que carrega o tamanho da mensagem


def send_json_tcp(host: str, port: int, obj: Dict[str, Any], timeout: float = 5.0) -> Optional[Dict[str, Any]]:
    """
    Envia um objeto JSON via TCP para (host, port), seguindo o protocolo:
    - Prefixo de 4 bytes (big-endian) indicando o tamanho da mensagem
    - Payload JSON UTF-8

    Retorna:
        O JSON decodificado da resposta do servidor, ou None em caso de erro/timeout.
    """
    data = json.dumps(obj).encode("utf-8")

    try:
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.settimeout(timeout)
            logger.debug("Conectando a %s:%d ...", host, port)
            s.connect((host, port))

            # Envia prefixo de tamanho + dados
            s.sendall(struct.pack("!I", len(data)))
            s.sendall(data)
            logger.debug("Enviado: %s", obj)

            # Espera resposta
            raw_len = recvall(s, LENGTH_PREFIX_SIZE)
            if not raw_len:
                logger.warning("Conexão encerrada sem resposta")
                return None

            length = struct.unpack("!I", raw_len)[0]
            resp = recvall(s, length)
            if not resp:
                logger.warning("Resposta incompleta recebida")
                return None

            decoded = json.loads(resp.decode("utf-8"))
            logger.debug("Recebido: %s", decoded)
            return decoded

    except (socket.timeout, ConnectionRefusedError) as e:
        logger.error("Erro de conexão/timeout: %s", e)
        return None
    except Exception as e:
        logger.exception("Erro inesperado em send_json_tcp: %s", e)
        return None


def recvall(sock: socket.socket, n: int) -> Optional[bytes]:
    """
    Lê exatamente `n` bytes do socket ou retorna None se a conexão for encerrada.
    """
    data = b""
    try:
        while len(data) < n:
            packet = sock.recv(n - len(data))
            if not packet:
                return None
            data += packet
        return data
    except Exception as e:
        logger.error("Erro em recvall: %s", e)
        return None


def start_multicast_sender(group_ip: str, port: int) -> socket.socket:
    """
    Cria e retorna um socket UDP configurado para envio multicast.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    # Define TTL = 1 (pacotes não saem da rede local)
    ttl = struct.pack("b", 1)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
    logger.info("Multicast sender pronto para %s:%d", group_ip, port)
    return sock


def start_multicast_listener(group_ip: str, port: int) -> socket.socket:
    """
    Cria e retorna um socket UDP configurado para escutar mensagens multicast.
    Junta-se ao grupo multicast fornecido.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        sock.bind(("", port))
    except OSError:
        sock.bind((group_ip, port))

    mreq = struct.pack("4sl", socket.inet_aton(group_ip), socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    logger.info("Multicast listener pronto em %s:%d", group_ip, port)
    return sock
