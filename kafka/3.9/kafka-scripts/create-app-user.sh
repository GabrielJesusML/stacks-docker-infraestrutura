#!/bin/bash
# Script para criar um usuário de aplicação com ACLs específicas
# Uso: ./create-app-user.sh <username> <password> <topic> <role>
# Role: producer, consumer, ou both

BOOTSTRAP_SERVER="localhost:9092"

if [ -z "$KAFKA_ADMIN_USER" ] || [ -z "$KAFKA_ADMIN_PASSWORD" ]; then
    echo "ERRO: Defina as variáveis KAFKA_ADMIN_USER e KAFKA_ADMIN_PASSWORD"
    exit 1
fi

if [ $# -lt 4 ]; then
    echo "Uso: $0 <username> <password> <topic> <role>"
    echo "Role: producer, consumer, ou both"
    echo ""
    echo "Exemplo:"
    echo "  $0 app-orders secret123 orders-topic producer"
    echo "  $0 app-analytics secret456 events consumer"
    echo "  $0 app-service secret789 notifications both"
    exit 1
fi

USERNAME=$1
PASSWORD=$2
TOPIC=$3
ROLE=$4

# Criar arquivo de config temporário
TEMP_CONFIG=$(mktemp)
cat > $TEMP_CONFIG << EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="$KAFKA_ADMIN_USER" password="$KAFKA_ADMIN_PASSWORD";
EOF

cleanup() {
    rm -f $TEMP_CONFIG
}
trap cleanup EXIT

echo "=========================================="
echo "Criando usuário: $USERNAME"
echo "Tópico: $TOPIC"
echo "Role: $ROLE"
echo "=========================================="

# Criar usuário SCRAM
echo "→ Criando credenciais SCRAM-SHA-256..."
kafka-configs.sh --bootstrap-server $BOOTSTRAP_SERVER \
    --command-config $TEMP_CONFIG \
    --alter --add-config "SCRAM-SHA-256=[password=$PASSWORD]" \
    --entity-type users --entity-name "$USERNAME"

# Configurar ACLs baseado no role
case $ROLE in
    producer)
        echo "→ Configurando ACLs de PRODUCER..."
        kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVER \
            --command-config $TEMP_CONFIG \
            --add --allow-principal "User:$USERNAME" \
            --operation WRITE \
            --operation DESCRIBE \
            --operation CREATE \
            --topic "$TOPIC"
        ;;
    consumer)
        echo "→ Configurando ACLs de CONSUMER..."
        kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVER \
            --command-config $TEMP_CONFIG \
            --add --allow-principal "User:$USERNAME" \
            --operation READ \
            --operation DESCRIBE \
            --topic "$TOPIC"
        
        kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVER \
            --command-config $TEMP_CONFIG \
            --add --allow-principal "User:$USERNAME" \
            --operation READ \
            --group "*"
        ;;
    both)
        echo "→ Configurando ACLs de PRODUCER + CONSUMER..."
        kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVER \
            --command-config $TEMP_CONFIG \
            --add --allow-principal "User:$USERNAME" \
            --operation WRITE \
            --operation READ \
            --operation DESCRIBE \
            --operation CREATE \
            --topic "$TOPIC"
        
        kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVER \
            --command-config $TEMP_CONFIG \
            --add --allow-principal "User:$USERNAME" \
            --operation READ \
            --group "*"
        ;;
    *)
        echo "ERRO: Role inválido. Use: producer, consumer, ou both"
        exit 1
        ;;
esac

echo ""
echo "=========================================="
echo "✓ Usuário '$USERNAME' criado com sucesso!"
echo "=========================================="
echo ""
echo "Configuração para a aplicação:"
echo "  bootstrap.servers: kfkpdhel01.dths.com.br:9094"
echo "  security.protocol: SASL_PLAINTEXT"
echo "  sasl.mechanism: SCRAM-SHA-256"
echo "  sasl.username: $USERNAME"
echo "  sasl.password: $PASSWORD"
echo "  topic: $TOPIC"
echo ""
