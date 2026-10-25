from __future__ import annotations

import pytest

from floe import lake
from floe.config import Settings
from floe.contract import Contract
from floe.produce import load_sample


@pytest.fixture(scope="session")
def contract() -> Contract:
    return Contract.load()


@pytest.fixture(scope="session")
def sample() -> list[dict]:
    return load_sample()


@pytest.fixture()
def settings(tmp_path, monkeypatch) -> Settings:
    wh = tmp_path / "warehouse"
    wh.mkdir()
    monkeypatch.setenv("FLOE_CATALOG_URI", f"sqlite:///{tmp_path}/catalog.db")
    monkeypatch.setenv("FLOE_WAREHOUSE", f"file://{wh}")
    return Settings()


@pytest.fixture()
def cat(settings, contract):
    c = lake.catalog(settings)
    lake.ensure_tables(c, contract)
    return c
