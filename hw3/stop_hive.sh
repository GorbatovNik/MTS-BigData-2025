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

stop_hive() {
    ssh "$ADMIN@$NN" "
        sudo systemctl stop hive-server2 hive-metastore
    "
}


main() {
    stop_hive

    log "Hive succesfully stopped."
}

main