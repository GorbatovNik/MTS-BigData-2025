#!/bin/bash

set -e

if [ -f ./cluster.conf ]; then
    source ./cluster.conf
else
    echo "ERROR: Configuration file cluster.conf not found. Exiting."
    exit 1
fi


update_hosts_file() {
    log "Updating /etc/hosts on all nodes"
    HOSTS_ENTRIES=""
    for host in "${!NODES[@]}"; do
        HOSTS_ENTRIES+="${NODES[$host]}\t$host\n"
    done

    for host in "${ALL_HOSTS[@]}"; do
        echo "Updating hosts on $host"
        ssh "team@$host" "echo -e '$HOSTS_ENTRIES' | sudo tee /etc/hosts > /dev/null"
    done
}

create_hadoop_user() {
    log "Creating user '$HADOOP_USER' on all nodes"
    read -sp "Enter a strong password for the new user '$HADOOP_USER': " HADOOP_PASSWORD
    echo ""

    for host in "${ALL_HOSTS[@]}"; do
        echo "Creating user on $host"
        ssh "team@$host" "
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

setup_ssh_keys() {
    log "Configuring passwordless SSH"

    log "Generating and distributing SSH key for 'team' user"
    rm -f ~/.ssh/id_rsa ~/.ssh/id_rsa.pub
    ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa -q
    for host in "${ALL_HOSTS[@]}"; do
        [ "$host" = "jn" ] && continue
        ssh-copy-id -i ~/.ssh/id_rsa.pub "team@$host"
    done

    log "Generating and distributing SSH key for '$HADOOP_USER' user"
    ssh team@jn "sudo -u $HADOOP_USER bash -c 'rm -f ~$HADOOP_USER/.ssh/id_rsa* && ssh-keygen -t rsa -N \"\" -f ~$HADOOP_USER/.ssh/id_rsa -q'"
    HADOOP_PUB_KEY=$(ssh team@jn "sudo cat ~$HADOOP_USER/.ssh/id_rsa.pub")

    for host in "${ALL_HOSTS[@]}"; do
        echo "Copying $HADOOP_USER key to $host"
        ssh "team@$host" "
            sudo -u $HADOOP_USER mkdir -p ~$HADOOP_USER/.ssh
            echo '$HADOOP_PUB_KEY' | sudo -u $HADOOP_USER tee ~$HADOOP_USER/.ssh/authorized_keys > /dev/null
            sudo -u $HADOOP_USER chmod 700 ~$HADOOP_USER/.ssh
            sudo -u $HADOOP_USER chmod 600 ~$HADOOP_USER/.ssh/authorized_keys
        "
    done
}


distribute_hadoop() {
    log "Downloading and distributing Hadoop"
    if [ ! -f "$HADOOP_ARCHIVE" ]; then
        wget "$HADOOP_URL"
    fi

    for host in "${ALL_HOSTS[@]}"; do
        echo "Copying and unpacking Hadoop on $host"
        scp "$HADOOP_ARCHIVE" "team@$host:/tmp/"
        ssh "team@$host" "
            sudo mv /tmp/$HADOOP_ARCHIVE $HADOOP_INSTALL_DIR/
            sudo chown $HADOOP_USER:$HADOOP_USER $HADOOP_INSTALL_DIR/$HADOOP_ARCHIVE
            sudo -u $HADOOP_USER tar -xzf $HADOOP_INSTALL_DIR/$HADOOP_ARCHIVE -C $HADOOP_INSTALL_DIR
        "
    done
}

configure_environment() {
    log "Configuring environment on nn and distributing"
    JAVA_HOME_PATH=$(ssh "team@nn" "dirname \"\$(dirname \"\$(readlink -f \$(which java))\")\"")


    PROFILE_CONFIG="
    export JAVA_HOME=${JAVA_HOME_PATH}
    export HADOOP_HOME=${HADOOP_HOME}
    export HADOOP_CONF_DIR=\${HADOOP_HOME}/etc/hadoop
    export PATH=\${PATH}:\${HADOOP_HOME}/bin:\${HADOOP_HOME}/sbin
    "
    ssh "team@nn" "echo -e '$PROFILE_CONFIG' | sudo -u $HADOOP_USER tee ~$HADOOP_USER/.profile > /dev/null"

    ssh "team@nn" "echo 'export JAVA_HOME=${JAVA_HOME_PATH}' | sudo -u $HADOOP_USER tee -a ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh > /dev/null"

    for host in "${ALL_HOSTS[@]}"; do
        [ "$host" = "nn" ] && continue
        echo "Copying profile to $host"
        ssh "team@nn" "scp ~$HADOOP_USER/.profile team@$host:/tmp/"
        ssh "team@$host" "sudo mv /tmp/.profile ~$HADOOP_USER/ && sudo chown $HADOOP_USER:$HADOOP_USER ~$HADOOP_USER/.profile"
    done
}

configure_hadoop_files() {
    log "Deploying Hadoop configuration files"

    scp core-site.xml hdfs-site.xml "team@nn:/tmp/"
    ssh "team@nn" "
        sudo mv /tmp/core-site.xml ${HADOOP_HOME}/etc/hadoop/
        sudo mv /tmp/hdfs-site.xml ${HADOOP_HOME}/etc/hadoop/
    "

    WORKERS_CONTENT=""
    for host in $WORKER_NODES; do
        WORKERS_CONTENT+="$host\n"
    done
    ssh "team@nn" "echo -e '$WORKERS_CONTENT' | sudo -u $HADOOP_USER tee ${HADOOP_HOME}/etc/hadoop/workers > /dev/null"

    for host in "${ALL_HOSTS[@]}"; do
        [ "$host" = "nn" ] && continue
        echo "Copying Hadoop config from nn to $host"
        ssh "team@nn" "sudo -u $HADOOP_USER scp -r ${HADOOP_HOME}/etc/hadoop/* $HADOOP_USER@$host:${HADOOP_HOME}/etc/hadoop/"
    done
}

format_namenode() {
    log "Formatting HDFS NameNode on nn"
    ssh "team@nn" "sudo -i -u $HADOOP_USER bash -c 'source ~/.profile && hdfs namenode -format'"
}

start_dfs() {
    log "Starting DFS services from nn"
    ssh "team@nn" "sudo -i -u $HADOOP_USER bash -c 'source ~/.profile && start-dfs.sh'"
}

check_processes() {
    log "Checking JPS status on all nodes"
    for host in "${ALL_HOSTS[@]}"; do
        echo "--- JPS on $host ---"
        ssh "team@$host" "jps"
    done
}


main() {
    update_hosts_file
    create_hadoop_user
    setup_ssh_keys
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