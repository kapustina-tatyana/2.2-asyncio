# Ответ:

Для создания БД выполнить в консоли:
```shell
docker-compose up -d
```

В docker-compose также запустит контейнер с PHP клиентом баз данных - "Adminer" 


Он доступен по адресу http://localhost:8081/adminer


Проверила работу скрипта с разным CHUNK_SIZE


CHUNK_SIZE = 3
![img_5.png](img_5.png)

CHUNK_SIZE = 7
![img_4.png](img_4.png)

При CHUNK_SIZE = 14
![img_3.png](img_3.png)

Вывод:
Чем больше параллельных заданий, тем быстрее выполняется скрипт.