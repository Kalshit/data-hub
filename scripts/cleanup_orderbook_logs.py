from __future__ import annotations

import argparse
from pathlib import Path


def cleanup(out_dir: Path) -> None:
    if not out_dir.exists():
        print(f"{out_dir} does not exist; nothing to clean.")
        return
    removed = 0
    for path in out_dir.glob("*.jsonl"):
        path.unlink()
        removed += 1
    print(f"Removed {removed} file(s) from {out_dir}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Delete orderbook snapshot JSONL files."
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("logs"),
        help="Directory containing JSONL logs",
    )
    args = parser.parse_args()
    cleanup(args.out_dir)


if __name__ == "__main__":
    main()

