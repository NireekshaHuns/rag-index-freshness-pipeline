from prometheus_client import REGISTRY


def sample(name: str, **labels: str) -> float:
    value = REGISTRY.get_sample_value(name, labels or None)
    return value or 0.0
