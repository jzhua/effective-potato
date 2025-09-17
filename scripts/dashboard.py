from __future__ import annotations

import argparse

from data_dashboard import create_app


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Launch the data pipeline dashboard.")
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind the dashboard server.")
    parser.add_argument("--port", type=int, default=8050, help="Port to serve the dashboard on.")
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable Dash debug mode (auto-reload and extra logging).",
    )
    args = parser.parse_args(argv)

    app = create_app()
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":  # pragma: no cover - manual entry point
    main()
