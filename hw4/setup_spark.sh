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

run_metastore() {
    log "Running metastore"
    ssh "$ADMIN@$NN" "
        sudo -i -u $HADOOP_USER bash -c 'hive --hiveconf hive.server2.enable.doAs=false --hiveconf hive.security.authorization.enable=false --service metastore 1>> /tmp/metastore.log 2>> /tmp/metastore.log &'
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

copy_hadoop_and_hive_to_jn() {
    log "Creating user '$HADOOP_USER' on $JN node"
    read -sp "Enter a strong password for the new user '$HADOOP_USER': " HADOOP_PASSWORD
    echo
    sudo useradd -m $HADOOP_USER
    echo $HADOOP_USER:$HADOOP_PASSWORD | sudo chpasswd
    sudo usermod -s /bin/bash $HADOOP_USER

    log "Copying $NN configs to $JN"
    ssh "$ADMIN@$NN" "
        sudo scp -r /home/$HADOOP_USER/hadoop-3.4.0 $ADMIN@$JN:/tmp
        sudo scp -r /home/$HADOOP_USER/.profile $ADMIN@$JN:/tmp
        sudo scp -r /home/$HADOOP_USER/apache-hive-4.0.0-alpha-2-bin $ADMIN@$JN:/tmp
    "
    sudo mv /tmp/hadoop-3.4.0 /home/$HADOOP_USER
    sudo mv /tmp/.profile /home/$HADOOP_USER
    sudo mv /tmp/apache-hive-4.0.0-alpha-2-bin /home/$HADOOP_USER
}


setup_python() {
    log "python3 installing"
    
    sudo apt install -y python3-venv
    sudo apt install -y python3-pip

    log "Python environment setup as $HADOOP_USER user..."
    sudo -i -u $HADOOP_USER bash -c '
        python3 -m venv .venv

        source .venv/bin/activate
        pip install -U pip
        pip install "pyspark==3.5.6"
        pip install onetl
    '
}

main() {
    run_metastore
    create_test_database
    load_data
    copy_hadoop_and_hive_to_jn
    setup_python
    sudo -i -u $HADOOP_USER bash -c '
        python3 spark_setup.py
    '
    log "Spark setup script finished."
}

main