import argparse
import logging
from aiohttp import web
from sqslite.api import create_app

def main():
    parser = argparse.ArgumentParser(description="SQSLite: A lightweight HTTP message broker backed by SQLite")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8080, help="Port to bind to")
    parser.add_argument("--db", default="sqslite.db", help="Path to SQLite database")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    
    level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    
    app = create_app(args.db)
    web.run_app(app, host=args.host, port=args.port)

if __name__ == "__main__":
    main()
