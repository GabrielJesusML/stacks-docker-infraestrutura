# Dockerfile auto-contido para Apache Kafka 3.9.0 com KRaft
FROM gbrljml/jdk:11-jdk

# Variáveis de ambiente
ENV KAFKA_VERSION=3.9.0 \
    SCALA_VERSION=2.13 \
    KAFKA_HOME=/opt/kafka \
    KAFKA_DATA_DIR=/var/lib/kafka/data \
    PATH=$PATH:/opt/kafka/bin

# Instalar dependências
RUN apt-get update && \
    apt-get install -y --no-install-recommends wget curl netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

# Baixar e instalar Kafka
RUN wget -q https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    tar -xzf kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -C /opt && \
    mv /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} ${KAFKA_HOME} && \
    rm kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    mkdir -p ${KAFKA_DATA_DIR}

# Criar script de inicialização embutido
RUN echo '#!/bin/bash' > /usr/local/bin/start-kafka.sh && \
    echo 'set -e' >> /usr/local/bin/start-kafka.sh && \
    echo '' >> /usr/local/bin/start-kafka.sh && \
    echo 'KAFKA_HOME=/opt/kafka' >> /usr/local/bin/start-kafka.sh && \
    echo 'KAFKA_DATA_DIR=/var/lib/kafka/data' >> /usr/local/bin/start-kafka.sh && \
    echo '' >> /usr/local/bin/start-kafka.sh && \
    echo 'echo "=========================================="' >> /usr/local/bin/start-kafka.sh && \
    echo 'echo "Iniciando Kafka 3.9.0"' >> /usr/local/bin/start-kafka.sh && \
    echo 'echo "Broker ID: ${KAFKA_BROKER_ID:-1}"' >> /usr/local/bin/start-kafka.sh && \
    echo 'echo "=========================================="' >> /usr/local/bin/start-kafka.sh && \
    echo '' >> /usr/local/bin/start-kafka.sh && \
    echo 'if [ -z "$KAFKA_CLUSTER_ID" ]; then' >> /usr/local/bin/start-kafka.sh && \
    echo '    export KAFKA_CLUSTER_ID=$(kafka-storage.sh random-uuid)' >> /usr/local/bin/start-kafka.sh && \
    echo 'fi' >> /usr/local/bin/start-kafka.sh && \
    echo '' >> /usr/local/bin/start-kafka.sh && \
    echo 'HOSTNAME=$(hostname)' >> /usr/local/bin/start-kafka.sh && \
    echo '' >> /usr/local/bin/start-kafka.sh && \
    echo 'if [ -z "$KAFKA_LISTENERS" ]; then' >> /usr/local/bin/start-kafka.sh && \
    echo '    export KAFKA_LISTENERS="PLAINTEXT://${HOSTNAME}:9092,CONTROLLER://${HOSTNAME}:9093"' >> /usr/local/bin/start-kafka.sh && \
    echo 'fi' >> /usr/local/bin/start-kafka.sh && \
    echo '' >> /usr/local/bin/start-kafka.sh && \
    echo 'if [ -z "$KAFKA_ADVERTISED_LISTENERS" ]; then' >> /usr/local/bin/start-kafka.sh && \
    echo '    export KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://${HOSTNAME}:9092"' >> /usr/local/bin/start-kafka.sh && \
    echo 'fi' >> /usr/local/bin/start-kafka.sh && \
    echo '' >> /usr/local/bin/start-kafka.sh && \
    echo 'CONFIG_FILE=${KAFKA_HOME}/config/kraft-server.properties' >> /usr/local/bin/start-kafka.sh && \
    echo '' >> /usr/local/bin/start-kafka.sh && \
    echo 'cat > $CONFIG_FILE << ENDCONF' >> /usr/local/bin/start-kafka.sh && \
    echo 'broker.id=${KAFKA_BROKER_ID:-1}' >> /usr/local/bin/start-kafka.sh && \
    echo 'process.roles=broker,controller' >> /usr/local/bin/start-kafka.sh && \
    echo 'node.id=${KAFKA_BROKER_ID:-1}' >> /usr/local/bin/start-kafka.sh && \
    echo 'listeners=${KAFKA_LISTENERS}' >> /usr/local/bin/start-kafka.sh && \
    echo 'advertised.listeners=${KAFKA_ADVERTISED_LISTENERS}' >> /usr/local/bin/start-kafka.sh && \
    echo 'listener.security.protocol.map=${KAFKA_LISTENER_SECURITY_PROTOCOL_MAP:-PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT}' >> /usr/local/bin/start-kafka.sh && \
    echo 'inter.broker.listener.name=${KAFKA_INTER_BROKER_LISTENER_NAME:-PLAINTEXT}' >> /usr/local/bin/start-kafka.sh && \
    echo 'controller.listener.names=${KAFKA_CONTROLLER_LISTENER_NAMES:-CONTROLLER}' >> /usr/local/bin/start-kafka.sh && \
    echo 'sasl.enabled.mechanisms=${KAFKA_SASL_ENABLED_MECHANISMS:-PLAIN}' >> /usr/local/bin/start-kafka.sh && \
    echo 'sasl.mechanism.inter.broker.protocol=${KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL:-PLAIN}' >> /usr/local/bin/start-kafka.sh && \
    echo 'log.dirs=${KAFKA_DATA_DIR}' >> /usr/local/bin/start-kafka.sh && \
    echo 'num.partitions=${KAFKA_NUM_PARTITIONS:-3}' >> /usr/local/bin/start-kafka.sh && \
    echo 'default.replication.factor=${KAFKA_DEFAULT_REPLICATION_FACTOR:-1}' >> /usr/local/bin/start-kafka.sh && \
    echo 'min.insync.replicas=${KAFKA_MIN_INSYNC_REPLICAS:-1}' >> /usr/local/bin/start-kafka.sh && \
    echo 'offsets.topic.replication.factor=${KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR:-1}' >> /usr/local/bin/start-kafka.sh && \
    echo 'transaction.state.log.replication.factor=${KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR:-1}' >> /usr/local/bin/start-kafka.sh && \
    echo 'transaction.state.log.min.isr=${KAFKA_TRANSACTION_STATE_LOG_MIN_ISR:-1}' >> /usr/local/bin/start-kafka.sh && \
    echo 'log.retention.hours=${KAFKA_LOG_RETENTION_HOURS:-168}' >> /usr/local/bin/start-kafka.sh && \
    echo 'log.segment.bytes=1073741824' >> /usr/local/bin/start-kafka.sh && \
    echo 'log.retention.check.interval.ms=300000' >> /usr/local/bin/start-kafka.sh && \
    echo 'controller.quorum.voters=${KAFKA_CONTROLLER_QUORUM_VOTERS:-1@localhost:9093}' >> /usr/local/bin/start-kafka.sh && \
    echo 'auto.create.topics.enable=${KAFKA_AUTO_CREATE_TOPICS_ENABLE:-true}' >> /usr/local/bin/start-kafka.sh && \
    echo 'delete.topic.enable=true' >> /usr/local/bin/start-kafka.sh && \
    echo 'group.initial.rebalance.delay.ms=0' >> /usr/local/bin/start-kafka.sh && \
    echo 'ENDCONF' >> /usr/local/bin/start-kafka.sh && \
    echo '' >> /usr/local/bin/start-kafka.sh && \
    echo 'if [ -n "$KAFKA_LISTENER_NAME_BROKER_SCRAM_SHA_256_SASL_JAAS_CONFIG" ]; then' >> /usr/local/bin/start-kafka.sh && \
    echo '    echo "listener.name.broker.scram-sha-256.sasl.jaas.config=${KAFKA_LISTENER_NAME_BROKER_SCRAM_SHA_256_SASL_JAAS_CONFIG}" >> $CONFIG_FILE' >> /usr/local/bin/start-kafka.sh && \
    echo 'fi' >> /usr/local/bin/start-kafka.sh && \
    echo '' >> /usr/local/bin/start-kafka.sh && \
    echo 'if [ -n "$KAFKA_LISTENER_NAME_EXTERNAL_SCRAM_SHA_256_SASL_JAAS_CONFIG" ]; then' >> /usr/local/bin/start-kafka.sh && \
    echo '    echo "listener.name.external.scram-sha-256.sasl.jaas.config=${KAFKA_LISTENER_NAME_EXTERNAL_SCRAM_SHA_256_SASL_JAAS_CONFIG}" >> $CONFIG_FILE' >> /usr/local/bin/start-kafka.sh && \
    echo 'fi' >> /usr/local/bin/start-kafka.sh && \
    echo '' >> /usr/local/bin/start-kafka.sh && \
    echo 'if [ -n "$KAFKA_AUTHORIZER_CLASS_NAME" ]; then' >> /usr/local/bin/start-kafka.sh && \
    echo '    echo "authorizer.class.name=${KAFKA_AUTHORIZER_CLASS_NAME}" >> $CONFIG_FILE' >> /usr/local/bin/start-kafka.sh && \
    echo '    echo "allow.everyone.if.no.acl.found=${KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND:-false}" >> $CONFIG_FILE' >> /usr/local/bin/start-kafka.sh && \
    echo '    echo "super.users=${KAFKA_SUPER_USERS:-User:admin}" >> $CONFIG_FILE' >> /usr/local/bin/start-kafka.sh && \
    echo 'fi' >> /usr/local/bin/start-kafka.sh && \
    echo '' >> /usr/local/bin/start-kafka.sh && \
    echo 'if [ ! -f "${KAFKA_DATA_DIR}/meta.properties" ]; then' >> /usr/local/bin/start-kafka.sh && \
    echo '    echo "Formatando storage KRaft..."' >> /usr/local/bin/start-kafka.sh && \
    echo '    SCRAM_ARGS=""' >> /usr/local/bin/start-kafka.sh && \
    echo '    if [ -n "$KAFKA_SCRAM_USERS" ]; then' >> /usr/local/bin/start-kafka.sh && \
    echo '        for user_pass in $(echo $KAFKA_SCRAM_USERS | tr "," " "); do' >> /usr/local/bin/start-kafka.sh && \
    echo '            username=$(echo $user_pass | cut -d: -f1)' >> /usr/local/bin/start-kafka.sh && \
    echo '            password=$(echo $user_pass | cut -d: -f2)' >> /usr/local/bin/start-kafka.sh && \
    echo '            SCRAM_ARGS="$SCRAM_ARGS --add-scram SCRAM-SHA-256=[name=$username,password=$password]"' >> /usr/local/bin/start-kafka.sh && \
    echo '            echo "Adicionando usuario SCRAM: $username"' >> /usr/local/bin/start-kafka.sh && \
    echo '        done' >> /usr/local/bin/start-kafka.sh && \
    echo '    fi' >> /usr/local/bin/start-kafka.sh && \
    echo '    kafka-storage.sh format --config=$CONFIG_FILE --cluster-id=${KAFKA_CLUSTER_ID} --ignore-formatted $SCRAM_ARGS' >> /usr/local/bin/start-kafka.sh && \
    echo 'fi' >> /usr/local/bin/start-kafka.sh && \
    echo '' >> /usr/local/bin/start-kafka.sh && \
    echo 'echo "Iniciando Kafka Server..."' >> /usr/local/bin/start-kafka.sh && \
    echo 'exec kafka-server-start.sh $CONFIG_FILE' >> /usr/local/bin/start-kafka.sh

# Tornar script executável
RUN chmod +x /usr/local/bin/start-kafka.sh

# Criar usuário kafka
RUN groupadd -r kafka && \
    useradd -r -g kafka kafka && \
    chown -R kafka:kafka ${KAFKA_HOME} ${KAFKA_DATA_DIR}

USER kafka
WORKDIR ${KAFKA_HOME}

# Expor portas
EXPOSE 9092 9093 9094

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 --start-period=60s \
    CMD kafka-broker-api-versions.sh --bootstrap-server localhost:9092 || exit 1

# Comando de inicialização
CMD ["start-kafka.sh"]