import time
import logging
import os

from svc import streams


logging.basicConfig(level=logging.INFO)


def main():
    # TODO: load class dynamically by parsing script argument
    streams.CDCMySQLTable('CDC Single MySQL Table', 'customers').process()
    streams.CDCMySQLTable('CDC Single MySQL Table', 'addresses').process()
    while True:
        time.sleep(1)


if __name__ == '__main__':
    # to wait for the demo system start
    time.sleep(int(os.environ.get('IDLE_FOR_WAITING_SYSTEM_START_SECS'), 0))
    main()
