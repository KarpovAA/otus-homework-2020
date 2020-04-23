Scoring API
HTTP API сервис сĸоринга. 

Параметры запуска сервера: apy.py -p <номер порта> -l <лог файл>
По умолчанию сервер поднимается на localhost:8080, журнал логов пишется в stdout.


Чтобы получить результат пользователь отправляет в POST запросе валидный JSON определенного формата на лоĸейшн /method.

Струĸтура запроса
{"account": "<имя компании партнера>", "login": "<имя пользователя>", "method": "<имя метода>", "token": "<аутентификационный токен>", "arguments": {<словарь с аргументами вызываемого метода>}}

account - строĸа, опционально, может быть пустым
login - строĸа, обязательно, может быть пустым
method - строĸа, обязательно, может быть пустым
token - строĸа, обязательно, может быть пустым
arguments - словарь (объеĸт json), обязательно, может быть пустым

В сервере реализованы следующие методы:
1. online_score.
2. clients_interests.

  1. Метод online_score.
Аргументы:
phone - строĸа или число, длиной 11, начинается с 7, опционально, может быть пустым
email - строĸа, в ĸоторой есть @, опционально, может быть пустым
fi rst_name - строĸа, опционально, может быть пустым
last_name - строĸа, опционально, может быть пустым
birthday - дата в формате DD.MM. YYYY, с ĸоторой прошло не больше 70 лет, опционально, может быть пустым
gender - число 0, 1 или 2, опционально, может быть пустым

Пример:
curl -X POST -H "Content-Type: application/json" -d '{"account": "horns&hoofs", "login": "h&f", "method": "online_score", "token": "55cc9ce545bcd144300fe9efc28e65d415b923ebb6be1e19d2750a2c03e80dd209a27954dca045e5bb12418e7d89b6d718a9e35af34e14e1d5bcd5a08f21fc95", 
"arguments": {"phone": "79175002040", "email": "ivanov@ivanov.ru", "first_name": "Иван", "last_name": "Иванов", "birthday": "01.01.1990", "gender": 1}}' http://127.0.0.1:8080/method/


  2. Метод clients_interests.
Аргументы
clie nt_id s - массив чисел, обязательно, не пустое
date - дата в формате DD.MM. YYYY, опционально, может быть пустым

Пример:
curl -X POST -H "Content-Type: application/json" -d '{"account": "horns&hoofs", "login": "h&f", "method": "clients_interests", "token": "55cc9ce545bcd144300fe9efc28e65d415b923ebb6be1e19d2750a2c03e80dd209a27954dca045e5bb12418e7d89b6d718a9e35af34e14e1d5bcd5a08f21fc95", "arguments": {"client_ids": [1,2,3,4], "date": "20.07.2017"}}' http://127.0.0.1:8080/method/

