#!/usr/bin/env python3
"""Build and run `middleware_client_test.py` inside a Docker container."""

import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MIDDLEWARE_DIR = REPO_ROOT / "middleware"
DOCKERFILE = MIDDLEWARE_DIR / "Dockerfile.test"
DEFAULT_TAG = "middleware-tests"


def run(command):
    result = subprocess.run(command, cwd=REPO_ROOT, check=False)
    if result.returncode != 0:
        raise SystemExit(result.returncode)


def main(image_tag: str) -> None:
    if not DOCKERFILE.exists():
        raise SystemExit(f"{DOCKERFILE} not found. Did you remove it?")

    build_cmd = [
        "docker",
        "build",
        "-f",
        str(DOCKERFILE.relative_to(REPO_ROOT)),
        "-t",
        image_tag,
        ".",
    ]
    run(build_cmd)

    test_cmd = [
        "docker",
        "run",
        "--rm",
        image_tag,
    ]
    run(test_cmd)


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description="Build and run middleware tests inside Docker"
    )
    parser.add_argument(
        "--tag",
        default=DEFAULT_TAG,
        help=f"Docker image tag to build (default: {DEFAULT_TAG})",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    main(args.tag)
