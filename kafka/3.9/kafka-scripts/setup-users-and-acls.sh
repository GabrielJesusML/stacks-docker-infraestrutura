#!/bin/bash
# Script para criar usuários e ACLs no Kafka
# Execute este script DEPOIS que o Kafka estiver rodando

BOOTSTRAP_SERVER="localhost:9092"

echo "=========================================="
echo "Criando usuários no Kafka com SCRAM-SHA-256"
echo "=========================================="

# Criar usuário admin (já configurado como super user)
echo "Criando usuário: admin"
kafka-configs.sh --bootstrap-server $BOOTSTRAP_SERVER \
  --alter --add-config 'SCRAM-SHA-256=[password=admin-secret]' \
  --entity-type users --entity-name admin

# Criar usuário producer
echo "Criando usuário: producer-app"
kafka-configs.sh --bootstrap-server $BOOTSTRAP_SERVER \
  --alter --add-config 'SCRAM-SHA-256=[password=producer123]' \
  --entity-type users --entity-name producer-app

# Criar usuário consumer
echo "Criando usuário: consumer-app"
kafka-configs.sh --bootstrap-server $BOOTSTRAP_SERVER \
  --alter --add-config 'SCRAM-SHA-256=[password=consumer123]' \
  --entity-type users --entity-name consumer-app

echo ""
echo "=========================================="
echo "Listando usuários criados"
echo "=========================================="
kafka-configs.sh --bootstrap-server $BOOTSTRAP_SERVER \
  --describe --entity-type users

echo ""
echo "=========================================="
echo "Configurando ACLs (Permissões)"
echo "=========================================="

# ACLs para producer-app: pode WRITE e DESCRIBE no tópico 'events'
echo "Configurando ACLs para producer-app..."
kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVER \
  --add --allow-principal User:producer-app \
  --operation WRITE \
  --operation DESCRIBE \
  --topic events

# ACLs para consumer-app: pode READ e DESCRIBE no tópico 'events'
echo "Configurando ACLs para consumer-app..."
kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVER \
  --add --allow-principal User:consumer-app \
  --operation READ \
  --operation DESCRIBE \
  --topic events \
  --group '*'

echo ""
echo "=========================================="
echo "Listando todas as ACLs configuradas"
echo "=========================================="
kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVER --list

echo ""
echo "=========================================="
echo "Setup concluído!"
echo "=========================================="
echo "Usuários criados:"
echo "  - admin (super user - pode tudo)"
echo "  - producer-app (pode produzir no tópico 'events')"
echo "  - consumer-app (pode consumir do tópico 'events')"
