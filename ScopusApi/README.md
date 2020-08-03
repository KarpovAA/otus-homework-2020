## Analyzer of publication activity in the Scopus databas
The analyzer receives data from the Scopus (Elsevier) database by organization identifier via the API (api.elsevier.com).

### Functional 
+ Gets a list of journals organization.
+ Returns publications rating by citation.
+ Returns journals rating report by count article and by count citation.
+ Returns authors rating report by count article and by count citation.

### Docs API
Elsevier API Interface Specification: https://dev.elsevier.com/api_docs.html

### Requirements
* Python 3.7+


### Run examples
for help:
``` 
python3 -m scopus_analyzer -h
```

``` 
./scopus_analyzer -h
```




