### Ycrawler
Async web crawler for https://news.ycombinator.com/ 

Crawler finds and downloads all articles from the https://news.ycombinator.com/ index page. 
Beginning with a base URL, it fetches each article and its comments, parses it for links, and download to specified directory. 
Crawler works in background and periodically checks the index page for new articles.

### Requirements
* Python 3.7+
* aiohttp
* aiofiles
* asyncio

### Run examples
for help:
``` 
python3 -m ycrawler -h
```

``` 
./ycrawler -h
```
for run and download to news directory
``` 
./ycrawler -o news
```



