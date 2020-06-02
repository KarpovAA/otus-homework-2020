# MemcLoad
The script parses and uploads logs to the memcached. 
Script use Python's multiprocessing and threading modules for parsing.


## **Requirements**
Python 3.6+
  - python-memcached
  - protobuf
Memcached

## **Examples**
To get help:
```
python3 -m memc_load --help
```
or
```
./memc_load.py --help
```

Make sure that Memcached is running:

Run:
```
python3 -m memc_load --pattern="./data/*.tsv.gz"
```
or
```
./memc_load.py --pattern="./data/*.tsv.gz"
```


