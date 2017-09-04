from subprocess import call
import time

cnt = 0
while True:
    cnt += 1
    print('Running', cnt)
    call(["scrapy", "crawl", "foursquare"])
    print('finished')
    time.sleep(3600)
