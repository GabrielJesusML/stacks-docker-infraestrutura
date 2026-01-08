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
RUN cat > /usr/local/bin/start-kafka.sh << 'EOF'
#!/bin/bash
set -e

echo "=========================================="
echo "Iniciando Kafka ${KAFKA_VERSION}"
echo "Broker ID: ${KAFKA_BROKER_ID:-1}"
echo "=========================================="

# Gerar Cluster ID se não existir
if [ -z "$KAFKA_CLUSTER_ID" ]; then
    export KAFKA_CLUSTER_ID=$(kafka-storage.sh random-uuid)
fi

# Obter hostname do container
HOSTNAME=$(hostname)

# Configurar listeners se não estiver definido
if [ -z "$KAFKA_LISTENERS" ]; then
    export KAFKA_LISTENERS="PLAINTEXT://${HOSTNAME}:9092,CONTROLLER://${HOSTNAME}:9093"
fi

# Configurar advertised.listeners se não estiver definido
if [ -z "$KAFKA_ADVERTISED_LISTENERS" ]; then
    export KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://${HOSTNAME}:9092"
fi

# Criar configuração dinâmica
cat > ${KAFKA_HOME}/config/kraft-server.properties << CONF
# Broker Configuration
broker.id=${KAFKA_BROKER_ID:-1}
process.roles=broker,controller
node.id=${KAFKA_BROKER_ID:-1}

# Listeners
listeners=${KAFKA_LISTENERS}
advertised.listeners=${KAFKA_ADVERTISED_LISTENERS}
listener.security.protocol.map=${KAFKA_LISTENER_SECURITY_PROTOCOL_MAP:-PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT}
inter.broker.listener.name=${KAFKA_INTER_BROKER_LISTENER_NAME:-PLAINTEXT}
controller.listener.names=${KAFKA_CONTROLLER_LISTENER_NAMES:-CONTROLLER}

# SASL Configuration
sasl.enabled.mechanisms=${KAFKA_SASL_ENABLED_MECHANISMS:-PLAIN}
sasl.mechanism.inter.broker.protocol=${KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL:-PLAIN}
CONF

# Adicionar JAAS config se SASL estiver habilitado
if [ -n "\$KAFKA_LISTENER_NAME_BROKER_SCRAM_SHA_256_SASL_JAAS_CONFIG" ]; then
    echo "listener.name.broker.scram-sha-256.sasl.jaas.config=\${KAFKA_LISTENER_NAME_BROKER_SCRAM_SHA_256_SASL_JAAS_CONFIG}" >> \${KAFKA_HOME}/config/kraft-server.properties
fi

# Adicionar ACL config se definido
if [ -n "\$KAFKA_AUTHORIZER_CLASS_NAME" ]; then
    cat >> \${KAFKA_HOME}/config/kraft-server.properties << ACL_CONF
authorizer.class.name=\${KAFKA_AUTHORIZER_CLASS_NAME}
allow.everyone.if.no.acl.found=\${KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND:-false}
super.users=\${KAFKA_SUPER_USERS:-User:admin}
ACL_CONF
fi

# Continuar com o resto da configuração
cat >> \${KAFKA_HOME}/config/kraft-server.properties << CONF

# Log Configuration
log.dirs=${KAFKA_DATA_DIR}
num.partitions=${KAFKA_NUM_PARTITIONS:-3}
default.replication.factor=${KAFKA_DEFAULT_REPLICATION_FACTOR:-1}
min.insync.replicas=${KAFKA_MIN_INSYNC_REPLICAS:-1}
offsets.topic.replication.factor=${KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR:-1}
transaction.state.log.replication.factor=${KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR:-1}
transaction.state.log.min.isr=${KAFKA_TRANSACTION_STATE_LOG_MIN_ISR:-1}

# Retention
log.retention.hours=${KAFKA_LOG_RETENTION_HOURS:-168}
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000

# KRaft Configuration
controller.quorum.voters=${KAFKA_CONTROLLER_QUORUM_VOTERS:-1@localhost:9093}

# Additional Settings
auto.create.topics.enable=${KAFKA_AUTO_CREATE_TOPICS_ENABLE:-true}
delete.topic.enable=true
group.initial.rebalance.delay.ms=0
CONF

# Formatar storage se necessário
if [ ! -f "${KAFKA_DATA_DIR}/meta.properties" ]; then
    echo "Formatando storage KRaft..."
    kafka-storage.sh format \
        --config=${KAFKA_HOME}/config/kraft-server.properties \
        --cluster-id=${KAFKA_CLUSTER_ID} \
        --ignore-formatted
fi

# Iniciar Kafka
echo "Iniciando Kafka Server..."
exec kafka-server-start.sh ${KAFKA_HOME}/config/kraft-server.properties
EOF

# Tornar script executável
RUN chmod +x /usr/local/bin/start-kafka.sh

# Criar usuário kafka
RUN groupadd -r kafka && \
    useradd -r -g kafka kafka && \
    chown -R kafka:kafka ${KAFKA_HOME} ${KAFKA_DATA_DIR}

USER kafka
WORKDIR ${KAFKA_HOME}

# Expor portas
EXPOSE 9092 9093

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 --start-period=60s \
    CMD kafka-broker-api-versions.sh --bootstrap-server localhost:9092 || exit 1

# Comando de inicialização
CMD ["start-kafka.sh"]