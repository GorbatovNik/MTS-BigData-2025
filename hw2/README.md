## Автоматический запуск YARN

Скрипт настраивает и запускает YARN (и JobHistory Server) в существующем Hadoop-кластере по параметрам из `cluster.conf`.

### Основные шаги
1. Выполняет подстановку в файлы конфигурации `yarn-site.xml` и `mapred-site.xml` информации о хостах и версии hadoop из `cluster.conf`.
2. Копирует файлы конфигурации `yarn-site.xml` и `mapred-site.xml` на все узлы (`ALL_HOSTS`), кроме узла `$JN`.  
3. Перемещает конфигурацию в `${HADOOP_HOME}/etc/hadoop/` и выставляет владельца `${HADOOP_USER}`.  
4. На узле `$NN` запускает:
   - `start-yarn.sh` для старта YARN (ResourceManager + NodeManager).  
   - `mapred --daemon start historyserver` для запуска JobHistory Server.  
5. Проверяет процессы `jps` на всех узлах.

### Требования
- Развернутый (c помощью скрипта из hw1) HDFS на кластере.
- Файлы `yarn-site.xml`, `mapred-site.xml` находятся рядом со скриптами.  

### Запуск скрипта
```bash
bash preprocess_xml.sh
bash setup_yarn.sh
```

### Запуск веб-интерфейса с локальной машины
```bash
ssh -L 9870:{внутренний адрес namenode}:9870 -L 8088:{внутренний адрес namenode}:8088 -L 19888:{внутренний адрес namenode}:19888 team@{внешний адрес для входа}
```

### Остановка YARN
```bash
bash stop_yarn.sh
```
