## Автоматический запуск HIVE

Скрипт разворачивает Hive с метастором на PostgreSQL в существующем Hadoop-кластере по параметрам из `cluster.conf`.

### Основные шаги
1. Загружает `cluster.conf`.  
2. **PostgreSQL (на `$DN01`):**
   - Устанавливает `postgresql-16`.  
   - Выполняет скрипт `setup_metastore.sql` для создания пользователя и базы Hive (пароль берётся из `$PG_HIVE_USER_PASS`).  
   - Копирует и применяет `postgresql.conf` и `pg_hba.conf`.  
   - Перезапускает сервис PostgreSQL.  
3. **Hive (на `$NN`):**
   - Скачивает архив Hive (`$HIVE_URL` → `$HIVE_ARCHIVE`) и распаковывает в `$HIVE_INSTALL_DIR`.  
   - Загружает JDBC-драйвер `postgresql-$PG_JAR_VERSION.jar` в `$HIVE_HOME/lib/`.  
   - Копирует `hive-site.xml` в `$HIVE_HOME/conf/`.  
   - Прописывает переменные окружения (`HIVE_HOME`, `HIVE_CONF_DIR`, `HIVE_AUX_JARS_PATH`, `PATH`) в `.profile` пользователя `$HADOOP_USER`.  
4. **HDFS директории (на `$NN`):**
   - Создаёт `/user/hive/warehouse` и `/tmp` в HDFS.  
   - Устанавливает права `g+w`.  
5. **Инициализация и запуск:**
   - Выполняет `schematool -dbType postgres -initSchema`.  
   - Запускает `hiveserver2` в фоне с логами в `/tmp/hs2.log`.

### Требования
- Развернутый (c помощью скрипта из hw1) HDFS на кластере.
- Запущенный (c помощью скрипта из hw2) YARN на кластере.
- Доступные файлы рядом со скриптом:  
  - `setup_metastore.sql`  
  - `postgresql.conf`  
  - `pg_hba.conf`  
  - `hive-site.xml`

### Запуск
```bash
bash setup_hive.sh
```

### Создание тестовой таблицы и запроса
```bash
bash make_test_data.sh
```

### Запуск веб-интерфейсов с локальной машины

```bash
ssh -L 10002:{внутренний адрес namenode}:10002 9870:{внутренний адрес namenode}:9870 -L 8088:{внутренний адрес namenode}:8088 -L 19888:{внутренний адрес namenode}:19888 team@{внешний адрес для входа}
```