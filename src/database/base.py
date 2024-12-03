from threading import Lock
from typing import Dict, Any


class ConnectionMap:

    def __init__(self):
        self.__bucket__: Dict = {}
        self.__lock: Lock = Lock()
        self.configs: Dict[str, Any] = {}

    def __getattr__(self, name: str):
        with self.__lock:
            if name in self.__bucket__:
                return self.__bucket__[name]

            else:

                if not self.configs.get(name):
                    raise AttributeError(
                        "Can't find '%s' at config" % name)

                connection = self.create_connection(self.configs[name])
                self.__bucket__.setdefault(name, connection)
                return connection

    def get(self, name: str):
        return self.__getattr__(name)

    def create_connection(self, config: dict) -> object:
        return object()

    def load_config(self, config: dict) -> None:
        for name, info in config.items():
            self.configs[name] = {
                k.lower(): v for k, v in info.items()
            }
