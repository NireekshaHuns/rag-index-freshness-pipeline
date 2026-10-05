"""`python -m freshness migrate` applies pending migrations."""

import logging
import sys

from freshness.config import Settings
from freshness.db import migrate


def main(argv: list[str]) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if argv[:1] != ["migrate"]:
        print("usage: python -m freshness migrate", file=sys.stderr)
        return 2
    settings = Settings.from_env()
    applied = migrate(settings.database_url, settings)
    print(f"applied {len(applied)} migration(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
