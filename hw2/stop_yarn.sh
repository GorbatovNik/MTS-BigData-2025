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

stop_yarn() {
    log "Stopping YARN services from nn"
    ssh "team@$NN" "
        sudo -i -u $HADOOP_USER bash -c 'stop-yarn.sh'
        sudo -i -u $HADOOP_USER bash -c 'mapred --daemon stop historyserver'
    "
}


main() {
    stop_yarn

    log "Hadoop cluster succesfully stopped."
}

main