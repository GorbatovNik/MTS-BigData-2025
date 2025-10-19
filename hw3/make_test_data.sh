#!/bin/bash

set -euo pipefail

if [ -f ./cluster.conf ]; then
    source ./cluster.conf
else
    echo "ERROR: Configuration file cluster.conf not found. Exiting."
    exit 1
fi

log() {
    echo "--- $1 ---"
}

load_data() {
    log "Downloading test data"
    if [ ! -f "$DATA_NAME" ]; then
        wget "$DATA_URL"
    fi

    log "Copying data to $NN"
    scp "$DATA_NAME" "$ADMIN@$NN:/tmp/"
    ssh "$ADMIN@$NN" "
        sudo mv /tmp/$DATA_NAME ~$HADOOP_USER/
        sudo chown $HADOOP_USER:$HADOOP_USER ~$HADOOP_USER/$DATA_NAME
    "

    log "Making /input dir on hdfs"
    ssh "$ADMIN@$NN" "
        sudo -i -u $HADOOP_USER bash -c '
            hdfs dfs -mkdir -p /input &&
            hdfs dfs -chmod g+w /input &&
            hdfs dfs -put -f $DATA_NAME /input &&
            hdfs fsck /input/$DATA_NAME | grep \"is HEALTHY\"
        '
    "
}

create_test_database() {
    log "Creating Hive database 'test' via Beeline"
    ssh "$ADMIN@$NN" "
        sudo -i -u $HADOOP_USER bash -c '
            $HIVE_HOME/bin/beeline -u jdbc:hive2://$NN:5433 -e \"DROP DATABASE IF EXISTS test CASCADE; CREATE DATABASE IF NOT EXISTS test;\"
        '
    "
}

create_test_table() {
    log "Copying create_table.sql to $NN"
    scp "create_table.sql" "$ADMIN@$NN:/tmp/"
    ssh "$ADMIN@$NN" "
        sudo mv /tmp/create_table.sql ~$HADOOP_USER/
        sudo chown $HADOOP_USER:$HADOOP_USER ~$HADOOP_USER/create_table.sql
    "

    log "Creating table 'test.some_data' and loading data"
    ssh "$ADMIN@$NN" "
        sudo -i -u $HADOOP_USER bash -c \"
            $HIVE_HOME/bin/beeline -u jdbc:hive2://$NN:5433 -f create_table.sql &&
            $HIVE_HOME/bin/beeline -u jdbc:hive2://$NN:5433 -e \\\"LOAD DATA INPATH '/input/$DATA_NAME' INTO TABLE test.some_data;\\\"
        \"
    "
}

check_count() {
    log "Executing SELECT COUNT(*) on test.some_data"
    ssh "$ADMIN@$NN" "
        sudo -i -u $HADOOP_USER bash -c '
            $HIVE_HOME/bin/beeline -u jdbc:hive2://$NN:5433 -e \"SELECT COUNT(*) FROM test.some_data;\"
        '
    "
}

main() {
    load_data
    create_test_database
    create_test_table
    check_count
}

main