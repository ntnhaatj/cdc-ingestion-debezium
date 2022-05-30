import time
import logging
import os
import argparse
import json
from pydoc import locate

from svc.helpers import try_configure_mysql_connectors
from schemas import try_to_fetch_all_schemas
from svc import settings


logging.basicConfig(level=logging.INFO)


def setup_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cls", help="class of stream pipeline", required=True, type=str
    )
    parser.add_argument(
        "--opts", help="pipeline options as dictionary", default={}, type=json.loads
    )
    parser.add_argument(
        "--to-console", help="load result to console", type=bool, default=False,
    )
    return parser.parse_args()


def main():
    args = setup_args()
    stream_cls = locate(args.cls)
    opts = args.opts
    stream = stream_cls(**opts, load_to_console=args.to_console)
    stream.process()

    while True:
        time.sleep(1)


def blocking_initialization():
    try_configure_mysql_connectors(
        settings.MYSQL_CONNECTOR_CONF,
        hostname=settings.DEBEZIUM_CONNECTOR_HOST,
        port=settings.DEBEZIUM_CONNECTOR_PORT)
    try_to_fetch_all_schemas()


if __name__ == '__main__':
    # to wait for the demo system start
    time.sleep(int(os.environ.get('IDLE_FOR_WAITING_SYSTEM_START_SECS'), 0))
    blocking_initialization()
    main()
