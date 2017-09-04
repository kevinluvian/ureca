import sys
from scrapy.cmdline import execute
import time

def gen_argv(s):
    sys.argv = s.split()

cnt = 0
if __name__ == '__main__':
    gen_argv('scrapy crawl foursquare')
    while True:
        cnt += 1
        print('Running', cnt);
        execute()
        time.sleep(3600)
