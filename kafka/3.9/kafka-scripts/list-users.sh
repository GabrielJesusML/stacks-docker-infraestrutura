#!/bin/bash
# Script simples para listar usuários e ACLs

BOOTSTRAP_SERVER="localhost:9092"

if [ -z "$KAFKA_ADMIN_USER" ] || [ -z "$KAFKA_ADMIN_PASSWORD" ]; then
    echo "ERRO: Defina as variáveis KAFKA_ADMIN_USER e KAFKA_ADMIN_PASSWORD"
    exit 1
fi

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
echo "USUÁRIOS CADASTRADOS"
echo "=========================================="
kafka-configs.sh --bootstrap-server $BOOTSTRAP_SERVER \
    --command-config $TEMP_CONFIG \
    --describe --entity-type users

echo ""
echo "=========================================="
echo "ACLs CONFIGURADAS"
echo "=========================================="
kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVER \
    --command-config $TEMP_CONFIG \
    --list
