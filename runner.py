from subprocess import call
import time

cnt = 0
while True:
    cnt += 1
    print('Running', cnt)
    start_time = time.time()
    call(["scrapy", "crawl", "foursquare"])
    elapsed_time = time.time() - start_time
    print('finished', elapsed_time)
    time.sleep(3600)
