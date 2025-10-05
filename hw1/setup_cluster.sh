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

setup_team_ssh_keys() {
    log "Configuring passwordless SSH"

    log "Generating and distributing SSH key for 'team' user"
    rm -f ~/.ssh/id_rsa ~/.ssh/id_rsa.pub
    ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa -q
    for host in "${!NODES[@]}"; do
        log "Copying SSH key to $host"
        sshpass -p "$MAINPASS" ssh-copy-id -i ~/.ssh/id_rsa.pub "team@$host"
    done
}

update_hosts_file() {
    log "Updating /etc/hosts on all nodes"
    HOSTS_ENTRIES=""
    for host in "${!NODES[@]}"; do
        HOSTS_ENTRIES+="${NODES[$host]}\t$host\n"
    done

    for host in "${!NODES[@]}"; do
        log "Updating hosts on $host - ${NODES[$host]}"
        
        if [ "$host" != $JN ]; then
            ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "team@${NODES[$host]}" "
                echo '$MAINPASS' | sudo -S -p '' echo
                echo -e '$HOSTS_ENTRIES' | sudo tee /etc/hosts > /dev/null
            "
        else
            echo "$MAINPASS" | sudo -S -p '' echo
            echo -e "$HOSTS_ENTRIES" | sudo tee /etc/hosts > /dev/null
        fi
    done
}

create_hadoop_user() {
    log "Creating user '$HADOOP_USER' on all nodes"
    read -sp "Enter a strong password for the new user '$HADOOP_USER': " HADOOP_PASSWORD
    echo

    for host in "${ALL_HOSTS[@]}"; do
        log "Creating user on $host"
        ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "team@$host" "
            echo '$MAINPASS' | sudo -S -p '' echo
            if ! id -u '$HADOOP_USER' >/dev/null 2>&1; then
                sudo useradd -m '$HADOOP_USER'
                echo '$HADOOP_USER:$HADOOP_PASSWORD' | sudo chpasswd
                echo 'User $HADOOP_USER created on $host'
            else
                echo 'User $HADOOP_USER already exists on $host'
            fi
        "
    done
}

setup_hadoop_ssh_keys() {
    log "Configuring passwordless SSH"

    log "Generating and distributing SSH key for $HADOOP_USER user"
    ssh "team@$NN" "
        echo '$MAINPASS' | sudo -S -p '' echo
        sudo -u $HADOOP_USER rm -f ~$HADOOP_USER/.ssh/id_rsa ~$HADOOP_USER/.ssh/id_rsa.pub
        sudo -u $HADOOP_USER ssh-keygen -t rsa -N '' -f ~$HADOOP_USER/.ssh/id_rsa -q
    "
    HADOOP_PUB_KEY=$(ssh "team@$NN" "
        echo '$MAINPASS' | sudo -S -p '' echo
        sudo cat ~$HADOOP_USER/.ssh/id_rsa.pub
    ")

    for host in "${ALL_HOSTS[@]}"; do
        log "Copying SSH key to $host"
        ssh "team@$host" "
            echo '$MAINPASS' | sudo -S -p '' echo
            sudo -u $HADOOP_USER mkdir -p ~$HADOOP_USER/.ssh
            echo '$HADOOP_PUB_KEY' | sudo -u $HADOOP_USER tee ~$HADOOP_USER/.ssh/authorized_keys > /dev/null
        "
    done
}

distribute_hadoop() {
    log "Downloading and distributing Hadoop"
    if [ ! -f "$HADOOP_ARCHIVE" ]; then
        wget "$HADOOP_URL"
    fi

    for host in "${ALL_HOSTS[@]}"; do
        log "Copying and unpacking Hadoop on $host"
        scp "$HADOOP_ARCHIVE" "team@$host:/tmp/"
        ssh "team@$host" "
            echo '$MAINPASS' | sudo -S -p '' echo
            sudo mv /tmp/$HADOOP_ARCHIVE $HADOOP_INSTALL_DIR/
            sudo chown $HADOOP_USER:$HADOOP_USER $HADOOP_INSTALL_DIR/$HADOOP_ARCHIVE
            sudo -u $HADOOP_USER tar -xzf $HADOOP_INSTALL_DIR/$HADOOP_ARCHIVE -C $HADOOP_INSTALL_DIR
        "
    done
}

configure_environment() {
    log "Configuring environment on nn and distributing"
    JAVA_HOME_PATH=$(ssh "team@$NN" "dirname \"\$(dirname \"\$(readlink -f \$(which java))\")\"")

    PROFILE_CONFIG="
    export JAVA_HOME=${JAVA_HOME_PATH}
    export HADOOP_HOME=${HADOOP_HOME}
    export HADOOP_CONF_DIR=\${HADOOP_HOME}/etc/hadoop
    export PATH=\${PATH}:\${HADOOP_HOME}/bin:\${HADOOP_HOME}/sbin
    "

    for host in "${ALL_HOSTS[@]}"; do
        echo "Setting profile on $host"
        ssh "team@$host" "
            echo '$MAINPASS' | sudo -S -p '' echo
            echo -e '$PROFILE_CONFIG' | sudo -u $HADOOP_USER tee -a ~$HADOOP_USER/.profile > /dev/null
            echo 'JAVA_HOME=${JAVA_HOME_PATH}' | sudo -u $HADOOP_USER tee -a ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh > /dev/null
        "
    done
}

configure_hadoop_files() {
    log "Deploying Hadoop configuration files"
    
    for host in "${ALL_HOSTS[@]}"; do
        log "Copying configuration to $host"
        scp -o StrictHostKeyChecking=no -o LogLevel=ERROR core-site.xml hdfs-site.xml "team@$host:/tmp/"
        ssh "team@$host" "
            echo '$MAINPASS' | sudo -S -p '' echo
            sudo mv /tmp/core-site.xml ${HADOOP_HOME}/etc/hadoop/
            sudo mv /tmp/hdfs-site.xml ${HADOOP_HOME}/etc/hadoop/
            sudo chown -R ${HADOOP_USER}:${HADOOP_USER} ${HADOOP_HOME}/etc/hadoop/
        "
    done

    log "Creating workers file on nn"
    WORKERS_CONTENT=""
    for host in $WORKER_NODES; do
        WORKERS_CONTENT+=$host$'\n'
    done

    ssh "team@$NN" "
        echo '$MAINPASS' | sudo -S -p '' echo
        echo -e '$WORKERS_CONTENT' | sudo -u $HADOOP_USER tee ${HADOOP_HOME}/etc/hadoop/workers > /dev/null
    "
}

format_namenode() {
    log "Formatting HDFS NameNode on nn"
    ssh "team@$NN" "
        echo '$MAINPASS' | sudo -S -p '' echo
        sudo -i -u $HADOOP_USER bash -c 'hdfs namenode -format'
    "
}

start_dfs() {
    log "Starting DFS services from nn"
    ssh "team@$NN" "
        echo '$MAINPASS' | sudo -S -p '' echo
        sudo -i -u $HADOOP_USER bash -c 'start-dfs.sh'
    "
}

check_processes() {
    log "Checking JPS status on all nodes (wait 30s)"
    sleep 30s
    for host in "${ALL_HOSTS[@]}"; do
        echo "--- JPS on $host ---"
        ssh "team@$host" "jps"
    done
}


main() {
    setup_team_ssh_keys
    update_hosts_file
    create_hadoop_user
    setup_hadoop_ssh_keys
    distribute_hadoop
    configure_environment
    configure_hadoop_files
    format_namenode
    start_dfs
    check_processes

    log "Hadoop cluster setup script finished."
    log "NameNode Web UI should be available at: http://nn:9870 or http://192.168.1.31:9870"
}

main