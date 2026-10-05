import freshness


def test_package_has_version() -> None:
    assert freshness.__version__ == "0.1.0"
