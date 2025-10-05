# #!/bin/bash

set -euo pipefail

if [ -f ./cluster.conf ]; then
    source ./cluster.conf
else
    echo "ERROR: Configuration file cluster.conf not found. Exiting."
    exit 1
fi

read -sp "Enter SSH password: " MAINPASS
echo

log() { 
    echo "--- $1 ---" 
} 

stop_dfs() {
    log "Stopping DFS services from nn"
    ssh "team@$NN" "
        echo '$MAINPASS' | sudo -S -p '' echo
        sudo -i -u $HADOOP_USER bash -c 'stop-dfs.sh'
    "
}


main() {
    stop_dfs

    log "Hadoop cluster succesfully stopped."
}

main