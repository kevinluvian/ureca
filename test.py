import sys
from scrapy.cmdline import execute
import time

def gen_argv(s):
    sys.argv = s.split()


if __name__ == '__main__':
    gen_argv('scrapy crawl foursquare')
    execute()
    #while True:
    #    execute()
    #    time.sleep(3600)
