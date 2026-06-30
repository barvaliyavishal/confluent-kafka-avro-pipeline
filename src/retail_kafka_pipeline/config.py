import os
from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class PipelineSettings:
    kafka_bootstrap_servers: str
    kafka_username: str
    kafka_password: str
    schema_registry_url: str
    schema_registry_username: str
    schema_registry_password: str

    @classmethod
    def from_env(cls) -> "PipelineSettings":
        values = {
            "kafka_bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").strip(),
            "kafka_username": os.getenv("KAFKA_USERNAME", "").strip(),
            "kafka_password": os.getenv("KAFKA_PASSWORD", "").strip(),
            "schema_registry_url": os.getenv("SCHEMA_REGISTRY_URL", "").strip(),
            "schema_registry_username": os.getenv("SCHEMA_REGISTRY_USERNAME", "").strip(),
            "schema_registry_password": os.getenv("SCHEMA_REGISTRY_PASSWORD", "").strip(),
        }
        missing = [name for name, value in values.items() if not value]
        if missing:
            readable = ", ".join(missing)
            raise ValueError(f"Missing required environment variables: {readable}")
        return cls(**values)

    def kafka_config(self) -> Dict[str, str]:
        return {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": self.kafka_username,
            "sasl.password": self.kafka_password,
        }

    def schema_registry_config(self) -> Dict[str, str]:
        return {
            "url": self.schema_registry_url,
            "basic.auth.user.info": f"{self.schema_registry_username}:{self.schema_registry_password}",
        }
