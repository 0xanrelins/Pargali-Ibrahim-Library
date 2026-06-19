#!/usr/bin/env python3
"""Run the BTC analyst agent CLI (Layer 5 v1)."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from agents.btc_analyst import ask, brief, load_context

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="BTC Intelligence analyst — catalog + derived + knowledge",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("brief", help="Print a one-paragraph BTC market briefing")

    ask_parser = subparsers.add_parser("ask", help="Ask a supported question")
    ask_parser.add_argument("question", help="Question text (Turkish or English)")

    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    args = build_parser().parse_args(argv)
    context = load_context(project_root=Path(__file__).resolve().parent.parent)

    if args.command == "brief":
        print(brief(context))
        return 0

    if args.command == "ask":
        print(ask(args.question, context))
        return 0

    logger.error("Unknown command: %s", args.command)
    return 1


if __name__ == "__main__":
    sys.exit(main())
