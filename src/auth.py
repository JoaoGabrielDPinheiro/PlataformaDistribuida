import uuid
import time

class SimpleAuth:
    def __init__(self):
        # Usuários estáticos para a simulação
        self._users = {"alice": "password1", "bob": "password2", "carol": "pwd3"}
        self._tokens = {}  # token -> (username, expiry)
        self._ttl = 3600

    def login(self, username: str, password: str):
        if username in self._users and self._users[username] == password:
            token = str(uuid.uuid4())
            self._tokens[token] = (username, time.time() + self._ttl)
            return token
        return None

    def validate(self, token: str):
        if not token:
            return False
        entry = self._tokens.get(token)
        if not entry:
            return False
        username, expiry = entry
        if time.time() > expiry:
            del self._tokens[token]
            return False
        return True

    def user_of(self, token: str):
        entry = self._tokens.get(token)
        return entry[0] if entry else None
