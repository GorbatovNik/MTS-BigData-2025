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

configure_yarn_files() {
    log "Deploying Hadoop configuration files"
    
    for host in "${ALL_HOSTS[@]}"; do
        log "Copying configuration to $host"
        scp -o StrictHostKeyChecking=no -o LogLevel=ERROR yarn-site.xml mapred-site.xml "$ADMIN@$host:/tmp/"
        ssh "$ADMIN@$host" "
            sudo mv /tmp/yarn-site.xml ${HADOOP_HOME}/etc/hadoop/
            sudo mv /tmp/mapred-site.xml ${HADOOP_HOME}/etc/hadoop/
            sudo chown -R ${HADOOP_USER}:${HADOOP_USER} ${HADOOP_HOME}/etc/hadoop/
        "
    done
}

start_yarn() {
    log "Starting Yarn from nn"
    ssh "$ADMIN@$NN" "
        sudo -i -u $HADOOP_USER bash -c 'start-yarn.sh'
        sudo -i -u $HADOOP_USER bash -c 'mapred --daemon start historyserver'
    "
}

check_processes() {
    log "Checking JPS status on all nodes"
    echo "Wait 15s..."
    sleep 15s
    for host in "${ALL_HOSTS[@]}"; do
        log "JPS on $host"
        ssh "$ADMIN@$host" "
            sudo -u $HADOOP_USER jps
        "
    done
}

main() {
    configure_yarn_files
    start_yarn
    check_processes

    log "Yarn setup script finished."
    log "NameNode UI should be available at: http://$NN:9870"
    log "ResourceManager UI should be available at: http://$NN:8088"
    log "JobHistory Server UI should be available at: http://$NN:19888"
}

main
