#!/bin/bash

set -euo pipefail
set -a

if [ -f ./cluster.conf ]; then
    source ./cluster.conf
else
    echo "ERROR: Configuration file cluster.conf not found. Exiting."
    exit 1
fi

log() { 
    echo "--- $1 ---" 
}

declare -r XML_FILES=(
    "core-site.xml" 
    "hdfs-site.xml"
    "yarn-site.xml"
    "mapred-site.xml"
)

process_xml_files() {
    for xml_file in "${XML_FILES[@]}"; do
        if [ -f "$xml_file" ]; then
            log "Processing $xml_file"
            envsubst < "$xml_file" > "$xml_file.tmp" && mv "$xml_file.tmp" "$xml_file"
            log "Successfully processed $xml_file"
        else
            log "WARNING: $xml_file not found, skipping"
        fi
    done
}

main() {
    log "Starting XML configuration processing"

    process_xml_files
    
    log "XML configuration processing completed"
}

main