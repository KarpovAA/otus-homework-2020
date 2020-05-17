Simple HTTP server
The server uses multiple threads to handle requests. 

Requirements: Python 3.6+


Test:
./httpd.py -w100
ab -n 50000 -c 100 -r http://127.0.0.1:8000/

Test result:
This is ApacheBench, Version 2.3 <$Revision: 1843412 $>
Copyright 1996 Adam Twiss, Zeus Technology Ltd, http://www.zeustech.net/
Licensed to The Apache Software Foundation, http://www.apache.org/

Benchmarking 127.0.0.1 (be patient)
Completed 5000 requests
Completed 10000 requests
Completed 15000 requests
Completed 20000 requests
Completed 25000 requests
Completed 30000 requests
Completed 35000 requests
Completed 40000 requests
Completed 45000 requests
Completed 50000 requests
Finished 50000 requests


Server Software:        simple-http-server
Server Hostname:        127.0.0.1
Server Port:            8000

Document Path:          /
Document Length:        0 bytes

Concurrency Level:      100
Time taken for tests:   53.473 seconds
Complete requests:      50000
Failed requests:        0
Total transferred:      4450000 bytes
HTML transferred:       0 bytes
Requests per second:    935.05 [#/sec] (mean)
Time per request:       106.946 [ms] (mean)
Time per request:       1.069 [ms] (mean, across all concurrent requests)
Transfer rate:          81.27 [Kbytes/sec] received

Connection Times (ms)
              min  mean[+/-sd] median   max
Connect:        0    0   0.1      0       3
Processing:     0    1   0.3      1      10
Waiting:        0    1   0.2      1       8
Total:          1    1   0.3      1      10

Percentage of the requests served within a certain time (ms)
  50%      1
  66%      1
  75%      1
  80%      1
  90%      1
  95%      1
  98%      2
  99%      2
 100%     10 (longest request)


Test in Docker:
./httpd.py --host 172.17.0.2 -w100
ab -n 50000 -c 100 -r http://172.17.0.2:8000/

Result test:
This is ApacheBench, Version 2.3 <$Revision: 1843412 $>
Copyright 1996 Adam Twiss, Zeus Technology Ltd, http://www.zeustech.net/
Licensed to The Apache Software Foundation, http://www.apache.org/

Benchmarking 172.17.0.2 (be patient)
Completed 5000 requests
Completed 10000 requests
Completed 15000 requests
Completed 20000 requests
Completed 25000 requests
Completed 30000 requests
Completed 35000 requests
Completed 40000 requests
Completed 45000 requests
Completed 50000 requests
Finished 50000 requests


Server Software:        simple-http-server
Server Hostname:        172.17.0.2
Server Port:            8000

Document Path:          /
Document Length:        14 bytes

Concurrency Level:      100
Time taken for tests:   39.745 seconds
Complete requests:      50000
Failed requests:        0
Total transferred:      7150000 bytes
HTML transferred:       700000 bytes
Requests per second:    1258.02 [#/sec] (mean)
Time per request:       79.490 [ms] (mean)
Time per request:       0.795 [ms] (mean, across all concurrent requests)
Transfer rate:          175.68 [Kbytes/sec] received

Connection Times (ms)
              min  mean[+/-sd] median   max
Connect:        0    3  48.3      0    3036
Processing:     1   76  84.1     55     647
Waiting:        0   75  84.0     52     642
Total:          1   79  97.8     56    3041

Percentage of the requests served within a certain time (ms)
  50%     56
  66%     94
  75%    116
  80%    136
  90%    195
  95%    248
  98%    319
  99%    369
 100%   3041 (longest request)

