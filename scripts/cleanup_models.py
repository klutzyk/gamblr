import argparse
import re
from pathlib import Path


DATE_SUFFIX_RE = re.compile(r"^(?P<prefix>.+)_(?P<date>\d{8})\.pkl$")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Clean up ml/models by keeping the latest N dated models per prefix."
    )
    parser.add_argument(
        "--keep",
        type=int,
        default=3,
        help="Number of dated models to keep per prefix (default: 3).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be deleted without removing files.",
    )
    args = parser.parse_args()

    root_dir = Path(__file__).resolve().parents[1]
    models_dir = root_dir / "ml" / "models"
    if not models_dir.exists():
        print(f"Models directory not found: {models_dir}")
        return 1

    keep = max(0, int(args.keep))
    keep_files: set[Path] = set()
    groups: dict[str, list[tuple[str, Path]]] = {}

    for file in models_dir.glob("*.pkl"):
        match = DATE_SUFFIX_RE.match(file.name)
        if not match:
            keep_files.add(file)
            continue
        prefix = match.group("prefix")
        date = match.group("date")
        groups.setdefault(prefix, []).append((date, file))

    for prefix, items in groups.items():
        items.sort(key=lambda x: x[0])
        if keep > 0:
            for _, file in items[-keep:]:
                keep_files.add(file)

    to_delete = [f for f in models_dir.glob("*.pkl") if f not in keep_files]
    if not to_delete:
        print("No old models to delete.")
        return 0

    print(f"Found {len(to_delete)} old model(s) to delete.")
    for file in to_delete:
        if args.dry_run:
            print(f"[dry-run] delete {file.name}")
        else:
            file.unlink(missing_ok=True)
            print(f"deleted {file.name}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
