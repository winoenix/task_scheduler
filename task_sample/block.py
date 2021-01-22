import sys
from pandas import to_datetime
import datetime
import time

if __name__ == "__main__":
    block_time = to_datetime(sys.argv[1])
    while to_datetime(datetime.datetime.now()) < block_time:
        time.sleep(1)
    print("%s: block released. " % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
