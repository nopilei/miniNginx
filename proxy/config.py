from dataclasses import dataclass

import yaml


@dataclass
class ParsedConfig:
    listen: str
    upstreams: list[dict[str, str]]


class ConfigLoader:
    def __init__(self, path: str) -> None:
        self.path = path

    def get_config(self):
        raw_config = self._get_raw_config(self.path)
        self._validate_config(raw_config)
        return ParsedConfig(listen=raw_config["listen"], upstreams=raw_config["upstreams"])

    def _get_raw_config(self, path: str) -> dict:
        with open(path, "r") as f:
            return yaml.safe_load(f)

    def _validate_config(self, raw_config: dict):
        if "listen" not in raw_config:
            raise ValueError("'listen' param required")
        server_uri = raw_config["listen"].split(":")
        if len(server_uri) != 2:
            raise ValueError("invalid 'listen' param")

        if "upstreams" not in raw_config:
            raise ValueError("'upstreams' param required")
        if not isinstance(raw_config["upstreams"], list):
            raise ValueError("upstreams must be list")
        for upstream in raw_config["upstreams"]:
            if "host" not in upstream:
                raise ValueError("host not set in upstream")
            if "port" not in upstream:
                raise ValueError("port not set in upstream")
