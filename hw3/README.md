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

### Входные данные (`cluster.conf`)
- `DN01` — хост с PostgreSQL (метастор).  
- `NN` — namenode (узел для Hive).  
- `ADMIN` — пользователь для SSH-доступа.  
- `HADOOP_USER` — пользователь Hadoop.  
- `HADOOP_HOME` — путь Hadoop.  
- `HIVE_HOME`, `HIVE_INSTALL_DIR`, `HIVE_URL`, `HIVE_ARCHIVE`.  
- `PG_HIVE_USER_PASS` — пароль для пользователя Hive в PostgreSQL.  
- `PG_JAR_VERSION` — версия JDBC-драйвера.  

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

### Остановка YARN
```bash
bash stop_hive.sh
```