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

setup_passwordless_sudo() {
    log "Сonfiguring passwordless sudo for user '$ADMIN'"
    
    read -sp "Enter sudo password for user '$ADMIN':" SUDOPASS
    echo

    local sudo_rule="$ADMIN ALL=(ALL) NOPASSWD: ALL"
    local sudo_file="/etc/sudoers.d/010_$ADMIN-nopasswd"
    
    for host in "${!NODES[@]}"; do
        log "Applying sudo configuration on $host"
        
        sshpass -p "$SUDOPASS" ssh -o StrictHostKeyChecking=no "$ADMIN@${NODES[$host]}" "
            echo '$SUDOPASS' | sudo -S -p '' bash -c \"
                echo '$sudo_rule' > $sudo_file && \\
                chmod 440 $sudo_file && \\
                echo 'Passwordless sudo configured successfully on $host.' || \\
                echo 'ERROR: Failed to configure passwordless sudo on $host.'
            \"
        "
    done
    
    unset SUDOPASS
}

setup_admin_ssh_keys() {
    log "Configuring passwordless SSH"

    read -sp "Enter SSH password for user '$ADMIN':" SSHPASS
    echo

    log "Downloading sshpass"
    sudo apt install sshpass

    log "Generating and distributing SSH key for '$ADMIN' user"
    rm -f ~/.ssh/id_rsa ~/.ssh/id_rsa.pub
    ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa -q
    for host in "${!NODES[@]}"; do
        log "Copying SSH key to $host"
        sshpass -p "$SSHPASS" ssh-copy-id -i ~/.ssh/id_rsa.pub "$ADMIN@$host"
    done

    unset SSHPASS
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
            ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "$ADMIN@${NODES[$host]}" "
                echo -e '$HOSTS_ENTRIES' | sudo tee /etc/hosts > /dev/null
            "
        else
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
        ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR "$ADMIN@$host" "
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

    log "Generating and distributing SSH key for '$HADOOP_USER' user"
    ssh "$ADMIN@$NN" "
        sudo -u $HADOOP_USER rm -f ~$HADOOP_USER/.ssh/id_rsa ~$HADOOP_USER/.ssh/id_rsa.pub
        sudo -u $HADOOP_USER ssh-keygen -t rsa -N '' -f ~$HADOOP_USER/.ssh/id_rsa -q
    "
    HADOOP_PUB_KEY=$(ssh "$ADMIN@$NN" "
        sudo cat ~$HADOOP_USER/.ssh/id_rsa.pub
    ")

    for host in "${ALL_HOSTS[@]}"; do
        log "Copying SSH key to $host"
        ssh "$ADMIN@$host" "
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
        scp "$HADOOP_ARCHIVE" "$ADMIN@$host:/tmp/"
        ssh "$ADMIN@$host" "
            sudo mv /tmp/$HADOOP_ARCHIVE $HADOOP_INSTALL_DIR/
            sudo chown $HADOOP_USER:$HADOOP_USER $HADOOP_INSTALL_DIR/$HADOOP_ARCHIVE
            sudo -u $HADOOP_USER tar -xzf $HADOOP_INSTALL_DIR/$HADOOP_ARCHIVE -C $HADOOP_INSTALL_DIR
        "
    done
}

configure_environment() {
    log "Configuring environment on nn and distributing"
    JAVA_HOME_PATH=$(ssh "$ADMIN@$NN" "dirname \"\$(dirname \"\$(readlink -f \$(which java))\")\"")

    PROFILE_CONFIG="
    export JAVA_HOME=${JAVA_HOME_PATH}
    export HADOOP_HOME=${HADOOP_HOME}
    export HADOOP_CONF_DIR=\${HADOOP_HOME}/etc/hadoop
    export PATH=\${PATH}:\${HADOOP_HOME}/bin:\${HADOOP_HOME}/sbin
    "

    for host in "${ALL_HOSTS[@]}"; do
        log "Setting profile on $host"
        ssh "$ADMIN@$host" "
            echo -e '$PROFILE_CONFIG' | sudo -u $HADOOP_USER tee -a ~$HADOOP_USER/.profile > /dev/null
            echo 'JAVA_HOME=${JAVA_HOME_PATH}' | sudo -u $HADOOP_USER tee -a ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh > /dev/null
        "
    done
}

configure_hadoop_files() {
    log "Deploying Hadoop configuration files"
    
    for host in "${ALL_HOSTS[@]}"; do
        log "Copying configuration to $host"
        scp -o StrictHostKeyChecking=no -o LogLevel=ERROR core-site.xml hdfs-site.xml "$ADMIN@$host:/tmp/"
        ssh "$ADMIN@$host" "
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

    ssh "$ADMIN@$NN" "
        echo -e '$WORKERS_CONTENT' | sudo -u $HADOOP_USER tee ${HADOOP_HOME}/etc/hadoop/workers > /dev/null
    "
}

format_namenode() {
    log "Formatting HDFS NameNode on nn"
    ssh "$ADMIN@$NN" "
        sudo -i -u $HADOOP_USER bash -c 'hdfs namenode -format'
    "
}

start_dfs() {
    log "Starting DFS services from nn"
    ssh "$ADMIN@$NN" "
        sudo -i -u $HADOOP_USER bash -c 'start-dfs.sh'
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
    setup_passwordless_sudo
    setup_admin_ssh_keys
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
    log "NameNode Web UI should be available at: http://$NN:9870"
}

main