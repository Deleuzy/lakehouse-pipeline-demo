import uuid

import pytest

from lakehouse.spark_session import get_spark


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    session = get_spark("lakehouse-tests", warehouse=tmp_path_factory.mktemp("warehouse"))
    yield session
    session.stop()


@pytest.fixture
def database(spark):
    name = f"test_{uuid.uuid4().hex[:8]}"
    yield name
    spark.sql(f"DROP DATABASE IF EXISTS {name} CASCADE")


def make_record(utc="2026-11-01T10:00:00", area="DK1", **overrides):
    """One record shaped like the Energinet ElectricityProdex5MinRealtime API."""
    record = {
        "Minutes5UTC": utc,
        "Minutes5DK": utc,
        "PriceArea": area,
        "ProductionLt100MW": 100.0,
        "ProductionGe100MW": 200.0,
        "OffshoreWindPower": 50.0,
        "OnshoreWindPower": 30.0,
        "SolarPower": 20.0,
        "ExchangeGreatBelt": 0.0,
        "ExchangeGermany": 0.0,
        "ExchangeNetherlands": None,
        "ExchangeGreatBritain": None,
        "ExchangeNorway": None,
        "ExchangeSweden": 0.0,
        "BornholmSE4": None,
    }
    record.update(overrides)
    return record

