### Анализатор публикационной активности в БД Scopus

Анализатор получает данные из БД Scopus (Elsevier) через API (api.elsevier.com).
+ Elsevier API Interface Specification: https://dev.elsevier.com/api_docs.html
+ Elsapy https://github.com/ElsevierDev/elsapy

**Введение**

На сегодняшний день для рейтингования научных сотрудников и научных институтов используют в основном две зарубежные базы данных Web Of Science и Scopus, а также данные отечественной бахы данных РИНЦ.
Данный скрипт осуществляет выгрузку данных из БД Scopus с целью их дальнейшего анализа.

**Исходные данные** 

В организации имеется информационная система учета публикаций. Внесение и редактирование данных в этой системе осуществляется вручную. 
Хранение данных осуществляется в локальной БД MySQL. 

**Цель**
Автоматизизация процесса выгрузки необходимых данных из БД Scopus. Формирование отчетов по публикационной активности по заданным параметрам.

**Постановка задачи**
Сформулируем основные функции, которые должен выполнять программный продукт 
1. Формирование рейтинга сотрудников научной организации по количеству публикаций и по количеству цитирований.
2. Формирование рейтинга журналов, в которых публикуется организация, по количеству публикаций и по количеству цитирований
3. Выгрузка рейтинга SCImago Journal Rank журналов (по списку).
4. Выгрузка количества цитирования публикаций (по списку).
5. Отчет по выпадающим публикациям из профиля организации.

**API интерфейс Scopus**
Документацияю по API Scopus можно найти по адресу https://dev.elsevier.com/api_docs.html.
Для взаимодействия с API Scopus используем библиотеку **elsapy** (https://github.com/ElsevierDev/elsapy).
Итак, начнем. 
Для создания соединения и получения доступа к БД Scopus необходимо иметь APIKEY, который указывается в заголовке соединения, формат заголовка:

    header = {
        "X-ELS-APIKey": api_key,
        "Accept": 'application/json'
    }

**Список публикаций**

Используя модуль elsapy осуществим первую выгрузку данных, где в качестве поискового запроса используем AF-ID() с указанием идентификатора организации в БД Scopus:
  
    from elsapy.elsclient import ElsClient
    from elsapy.elssearch import ElsSearch
    
    client = ElsClient(api_key)
    search = ElsSearch(f"(AF-ID({org_id}))", 'scopus')
    search.execute(client, get_all=True)
    
    print(search.results)

Результат ответа на запрос возвращается в виде списка словарей (https://dev.elsevier.com/sc_search_views.html).
В результате выполнения запроса получили список с данными и видим, что API выдает результаты порциями (https://github.com/ElsevierDev/elsapy/blob/5fda0434f106fdd89fa851144c25d80d7e1100d5/elsapy/elssearch.py#L100), что заняло значительное время.
Модуль elsapy выполняется в синхронном режиме и возможно при реализации в асинхронном режиме позволит сократить время ожидания результата.

**Авторы и их афиляции**

Для получения данных по авторам публикаций и их аффиляции с органнизациями, сформируем список запросов из полученных данных. 
URL запрос имеет следующий вид:
    
    https://api.elsevier.com/content/abstract/scopus_id/{id_pub}?field=author,affiliation

Реализуем выполнение запросов в асинхнонном режиме с использованием aiohttp:

    async def async_get_request(session, url, headers):
        async with session.get(url, headers=headers) as r:
            return await r.text()


    async def run_async_get_requests(urls, headers):
        connector = aiohttp.TCPConnector(limit_per_host=LIMIT_PER_HOST_CONNECTIONS, force_close=True)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [(async_get_request(session, url, headers)) for url in urls]
            result = await asyncio.gather(*tasks)
            return result

В результате мы получили основные данные для осуществления анализа и составления рейтингов.

**Рейтинги и отчеты**

Обработка полученных данных осуществлялась с использованием pandas. Все рейтинги, отчеты и списки сохраняем в файлы csv.

**Планы**
- Реализовать кэширование запросов к API (memcached, redis)
- Реализовать выгрузку всех данных из API Scopus в асинхронном режиме, тем самым ускорить выгрузку данных и обойти ограничение API на 5000 результатов.

