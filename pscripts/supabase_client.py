from __future__ import annotations

import os

from supabase import Client, create_client


def get_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Variable d'environnement manquante: {name}")
    return value


def get_first_env(*names: str) -> str:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    raise ValueError(f"Variable d'environnement manquante: {' ou '.join(names)}")


def get_supabase() -> Client:
    url = get_first_env("VU2LAMER_SUPABASE_URL", "SUPABASE_URL")
    key = get_first_env("VU2LAMER_SUPABASE_SERVICE_KEY", "VU2LAMER_SUPABASE_KEY", "SUPABASE_SERVICE_KEY", "SUPABASE_KEY")
    return create_client(url, key)


def get_vu2lamer_supabase() -> Client:
    url = get_first_env("VU2LAMER_SUPABASE_URL", "SUPABASE_URL")
    key = get_first_env("VU2LAMER_SUPABASE_SERVICE_KEY", "VU2LAMER_SUPABASE_KEY", "SUPABASE_SERVICE_KEY", "SUPABASE_KEY")
    return create_client(url, key)


def get_data2lamer_supabase() -> Client | None:
    url = os.getenv("DATA2LAMER_SUPABASE_URL")
    key = os.getenv("DATA2LAMER_SUPABASE_SERVICE_KEY") or os.getenv("DATA2LAMER_SUPABASE_KEY")
    if not url or not key:
        return None
    return create_client(url, key)
