# #!/bin/bash

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

setup_metastore_db() {
    log "Install PostgreSQL and perform setup commands"

    ssh "$ADMIN@$DN01" \
        "sudo apt update && sudo apt install -y postgresql-16"

    scp setup_metastore.sql "$ADMIN@$DN01:/tmp/"
    ssh "$ADMIN@$DN01" \
        "sudo -u postgres psql -X -v ON_ERROR_STOP=1 -v hive_pass='${PG_HIVE_USER_PASS}' -f /tmp/setup_metastore.sql"

    log "Copy and configure 'postgresql.conf'"
    scp postgresql.conf "$ADMIN@$DN01:/tmp/"
    ssh "$ADMIN@$DN01" "
        sudo mv /tmp/postgresql.conf /etc/postgresql/16/main/
        sudo chown postgres:postgres /etc/postgresql/16/main/postgresql.conf
    "

    log "Copy and configure 'pg_hba.conf'"
    scp pg_hba.conf "$ADMIN@$DN01:/tmp/"
    ssh "$ADMIN@$DN01" "
        sudo mv /tmp/pg_hba.conf /etc/postgresql/16/main/ 
        sudo chown postgres:postgres /etc/postgresql/16/main/pg_hba.conf
    "

    log "Restart the PostgreSQL service to apply new configuration files"
    ssh "$ADMIN@$DN01" "
        sudo systemctl restart postgresql@16-main
        sudo systemctl status postgresql@16-main | grep Active
    "
}

setup_hive() {
    log "Downloading and installing Hive"
    
    if [ ! -f "$HIVE_ARCHIVE" ]; then
        wget "$HIVE_URL"
    fi

    log "Copying and unpacking Hive on $NN"
    scp "$HIVE_ARCHIVE" "$ADMIN@$NN:/tmp/"
    ssh "$ADMIN@$NN" "
        sudo mv /tmp/$HIVE_ARCHIVE $HIVE_INSTALL_DIR/
        sudo chown -R $HADOOP_USER:$HADOOP_USER $HIVE_INSTALL_DIR/$HIVE_ARCHIVE
        sudo -u $HADOOP_USER tar -xzf $HIVE_INSTALL_DIR/$HIVE_ARCHIVE -C $HIVE_INSTALL_DIR
        "

    log "Downloading jar file"
    ssh "$ADMIN@$NN" "
        sudo -u $HADOOP_USER wget -O "$HIVE_HOME/lib/postgresql-$PG_JAR_VERSION.jar" https://jdbc.postgresql.org/download/postgresql-$PG_JAR_VERSION.jar
    "

    log "Copy and configure 'hive-site.xml'"
    scp "hive-site.xml" "$ADMIN@$NN:/tmp/"
    ssh "$ADMIN@$NN" "
        sudo mv /tmp/hive-site.xml $HIVE_HOME/conf/
        sudo chown $HADOOP_USER:$HADOOP_USER $HIVE_HOME/conf/hive-site.xml
    "

    PROFILE_CONFIG="
    export HIVE_HOME=${HIVE_HOME}
    export HIVE_CONF_DIR=\${HIVE_HOME}/conf
    export HIVE_AUX_JARS_PATH=\${HIVE_HOME}/lib/*
    export PATH=\${PATH}:\${HIVE_HOME}/bin
    "

    log "Setting profile on $NN"
    ssh "$ADMIN@$NN" "
        echo -e '$PROFILE_CONFIG' | sudo -u $HADOOP_USER tee -a ~$HADOOP_USER/.profile > /dev/null
    "
}

warehouse_init() {
    log "Creating Hive warehouse directories"
    ssh "$ADMIN@$NN" "
        sudo -i -u $HADOOP_USER bash -c '
            hdfs dfs -mkdir -p /user/hive/warehouse &&
            hdfs dfs -mkdir -p /tmp &&
            hdfs dfs -chmod g+w /user/hive/warehouse &&
            hdfs dfs -chmod g+w /tmp
        '
    "
}

run_hive() {
    log "Initing schema"
    ssh "$ADMIN@$NN" "
        sudo -i -u $HADOOP_USER bash -c '$HIVE_HOME/bin/schematool -dbType postgres -initSchema'
    "

    log "Running hive"
    ssh "$ADMIN@$NN" "
        sudo -i -u $HADOOP_USER bash -c 'hive --hiveconf hive.server2.enable.doAs=false --hiveconf hive.security.authorization.enable=false --service hiveserver2 1>> /tmp/hs2.log 2>> /tmp/hs2.log &'
    "
}

main() {
    setup_metastore_db
    setup_hive
    warehouse_init
    run_hive

    log "Hive setup script finished."
    log "HiveServer2 should be available at: http://$NN:10002"
}

main