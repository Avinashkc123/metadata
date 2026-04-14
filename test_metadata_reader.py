from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.metadata_reader import load_metadata


def main() -> None:
    path = ROOT / "input" / "energy_siccode_bmq_neg1.json"
    metadata = load_metadata(path)
    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    main()
