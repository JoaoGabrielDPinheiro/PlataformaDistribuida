# src/auth.py
import uuid

class SimpleAuth:
    def __init__(self):
        # Usuários de teste
        self._users = {
            "alice": "password1",
            "bob": "password2",
            "carol": "pwd3",
        }
        self._tokens = {}  # token -> username

    def login(self, username: str, password: str):
        pw = self._users.get(username)
        if pw and pw == password:
            token = str(uuid.uuid4())
            self._tokens[token] = username
            return token
        return None

    def validate(self, token: str) -> bool:
        return token in self._tokens

    def user_of(self, token: str):
        return self._tokens.get(token)
