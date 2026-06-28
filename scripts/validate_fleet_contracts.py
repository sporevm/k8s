#!/usr/bin/env python3
from __future__ import annotations

from fleet_contracts import repo_root_from_script, validate_examples


def main() -> None:
    root = repo_root_from_script()
    validate_examples(root)
    print("fleet contracts ok")


if __name__ == "__main__":
    main()
