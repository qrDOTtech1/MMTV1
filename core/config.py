import os
import re
from dataclasses import dataclass
from pathlib import Path


ENV_PATH = Path(__file__).parent.parent / ".env"
ENV_EXAMPLE_PATH = Path(__file__).parent.parent / ".env.example"


@dataclass
class Config:
    private_key: str
    rpc_url: str
    chain_id: int
    mode: str  # "paper" or "live"

    @property
    def is_live(self) -> bool:
        return self.mode == "live"


def _normalize_key(pk: str) -> str:
    """Valide et normalise une clé privée hex. Retourne "" si invalide."""
    pk = (pk or "").strip()
    if not pk or pk == "ta_cle_privee_ici":
        return ""
    if not pk.startswith("0x"):
        pk = "0x" + pk
    hex_part = pk[2:]
    if len(hex_part) != 64 or not re.fullmatch(r"[0-9a-fA-F]{64}", hex_part):
        return ""
    return pk


def load_config() -> Config:
    pk = _normalize_key(os.environ.get("PRIVATE_KEY", ""))

    return Config(
        private_key=pk,
        rpc_url=os.environ.get("RPC_URL", "https://mainnet.base.org"),
        chain_id=int(os.environ.get("CHAIN_ID", "8453")),
        mode=os.environ.get("MODE", "paper"),
    )


def save_env_value(key: str, value: str) -> None:
    """Met à jour ou ajoute une variable dans .env, en préservant le reste du fichier."""
    if ENV_PATH.exists():
        lines = ENV_PATH.read_text(encoding="utf-8").splitlines()
    elif ENV_EXAMPLE_PATH.exists():
        lines = ENV_EXAMPLE_PATH.read_text(encoding="utf-8").splitlines()
    else:
        lines = []

    pattern = re.compile(rf"^{re.escape(key)}\s*=")
    found = False
    new_lines = []
    for line in lines:
        if pattern.match(line):
            new_lines.append(f"{key}={value}")
            found = True
        else:
            new_lines.append(line)
    if not found:
        new_lines.append(f"{key}={value}")

    ENV_PATH.write_text("\n".join(new_lines) + "\n", encoding="utf-8")


def validate_private_key(pk: str) -> tuple[bool, str]:
    """Retourne (valide, message_erreur)."""
    raw = (pk or "").strip()
    if not raw:
        return False, "clé vide"
    normalized = _normalize_key(raw)
    if not normalized:
        return False, "format invalide (attendu: 64 caractères hexadécimaux, avec ou sans 0x)"
    return True, ""
