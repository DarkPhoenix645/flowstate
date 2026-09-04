from flowstate.config import Settings


def test_default_data_dir():
    s = Settings()
    assert s.data_dir.name == "data"
    assert s.spark_master == "spark://spark:7077"
