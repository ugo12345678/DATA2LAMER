from __future__ import annotations

import os

from supabase import Client, create_client


def get_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Variable d'environnement manquante: {name}")
    return value


def get_supabase() -> Client:
    url = get_env("SUPABASE_URL")
    key = get_env("SUPABASE_KEY")
    return create_client(url, key)