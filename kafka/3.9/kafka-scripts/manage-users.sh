#!/bin/bash
# Script interativo para gerenciar usuários e ACLs
# Requer variáveis: KAFKA_ADMIN_USER e KAFKA_ADMIN_PASSWORD

BOOTSTRAP_SERVER="localhost:9092"
CONFIG_FILE="/opt/kafka-scripts/admin.properties"

# Verificar se as credenciais estão configuradas
if [ -z "$KAFKA_ADMIN_USER" ] || [ -z "$KAFKA_ADMIN_PASSWORD" ]; then
    echo "ERRO: Defina as variáveis KAFKA_ADMIN_USER e KAFKA_ADMIN_PASSWORD"
    echo "Exemplo: export KAFKA_ADMIN_USER=admin KAFKA_ADMIN_PASSWORD=senha"
    exit 1
fi

# Criar arquivo de config temporário com credenciais
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

show_menu() {
    echo ""
    echo "=========================================="
    echo "Gerenciamento de Usuários e ACLs - Kafka"
    echo "=========================================="
    echo "1. Listar todos os usuários"
    echo "2. Criar novo usuário"
    echo "3. Deletar usuário"
    echo "4. Alterar senha de usuário"
    echo "5. Listar todas as ACLs"
    echo "6. Adicionar ACL (WRITE - Producer)"
    echo "7. Adicionar ACL (READ - Consumer)"
    echo "8. Adicionar ACL (ALL - Full Access)"
    echo "9. Remover ACLs de um usuário"
    echo "10. Listar ACLs de um usuário específico"
    echo "0. Sair"
    echo "=========================================="
    echo -n "Escolha uma opção: "
}

list_users() {
    echo ""
    echo "Usuários cadastrados:"
    kafka-configs.sh --bootstrap-server $BOOTSTRAP_SERVER \
        --command-config $TEMP_CONFIG \
        --describe --entity-type users
}

create_user() {
    echo ""
    read -p "Nome do usuário: " username
    read -sp "Senha: " password
    echo ""
    
    kafka-configs.sh --bootstrap-server $BOOTSTRAP_SERVER \
        --command-config $TEMP_CONFIG \
        --alter --add-config "SCRAM-SHA-256=[password=$password]" \
        --entity-type users --entity-name "$username"
    
    echo "✓ Usuário '$username' criado com sucesso!"
}

delete_user() {
    echo ""
    read -p "Nome do usuário para deletar: " username
    read -p "Tem certeza? (s/n): " confirm
    
    if [ "$confirm" = "s" ]; then
        kafka-configs.sh --bootstrap-server $BOOTSTRAP_SERVER \
            --command-config $TEMP_CONFIG \
            --alter --delete-config 'SCRAM-SHA-256' \
            --entity-type users --entity-name "$username"
        
        echo "✓ Usuário '$username' deletado!"
    else
        echo "Operação cancelada."
    fi
}

change_password() {
    echo ""
    read -p "Nome do usuário: " username
    read -sp "Nova senha: " password
    echo ""
    
    kafka-configs.sh --bootstrap-server $BOOTSTRAP_SERVER \
        --command-config $TEMP_CONFIG \
        --alter --add-config "SCRAM-SHA-256=[password=$password]" \
        --entity-type users --entity-name "$username"
    
    echo "✓ Senha do usuário '$username' alterada!"
}

list_acls() {
    echo ""
    echo "ACLs configuradas:"
    kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVER \
        --command-config $TEMP_CONFIG \
        --list
}

add_write_acl() {
    echo ""
    read -p "Nome do usuário: " username
    read -p "Nome do tópico (ou * para todos): " topic
    
    kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVER \
        --command-config $TEMP_CONFIG \
        --add --allow-principal "User:$username" \
        --operation WRITE \
        --operation DESCRIBE \
        --operation CREATE \
        --topic "$topic"
    
    echo "✓ ACL de WRITE adicionada para '$username' no tópico '$topic'"
}

add_read_acl() {
    echo ""
    read -p "Nome do usuário: " username
    read -p "Nome do tópico (ou * para todos): " topic
    read -p "Consumer group (ou * para todos): " group
    
    kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVER \
        --command-config $TEMP_CONFIG \
        --add --allow-principal "User:$username" \
        --operation READ \
        --operation DESCRIBE \
        --topic "$topic"
    
    kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVER \
        --command-config $TEMP_CONFIG \
        --add --allow-principal "User:$username" \
        --operation READ \
        --group "$group"
    
    echo "✓ ACL de READ adicionada para '$username' no tópico '$topic' com grupo '$group'"
}

add_full_acl() {
    echo ""
    read -p "Nome do usuário: " username
    read -p "Nome do tópico (ou * para todos): " topic
    
    kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVER \
        --command-config $TEMP_CONFIG \
        --add --allow-principal "User:$username" \
        --operation ALL \
        --topic "$topic"
    
    kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVER \
        --command-config $TEMP_CONFIG \
        --add --allow-principal "User:$username" \
        --operation ALL \
        --group "*"
    
    echo "✓ ACL FULL ACCESS adicionada para '$username' no tópico '$topic'"
}

remove_user_acls() {
    echo ""
    read -p "Nome do usuário: " username
    read -p "Remover TODAS as ACLs deste usuário? (s/n): " confirm
    
    if [ "$confirm" = "s" ]; then
        kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVER \
            --command-config $TEMP_CONFIG \
            --remove --allow-principal "User:$username" \
            --force
        
        echo "✓ Todas as ACLs do usuário '$username' foram removidas!"
    else
        echo "Operação cancelada."
    fi
}

list_user_acls() {
    echo ""
    read -p "Nome do usuário: " username
    
    echo "ACLs do usuário '$username':"
    kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVER \
        --command-config $TEMP_CONFIG \
        --list --principal "User:$username"
}

# Loop principal
while true; do
    show_menu
    read choice
    
    case $choice in
        1) list_users ;;
        2) create_user ;;
        3) delete_user ;;
        4) change_password ;;
        5) list_acls ;;
        6) add_write_acl ;;
        7) add_read_acl ;;
        8) add_full_acl ;;
        9) remove_user_acls ;;
        10) list_user_acls ;;
        0) echo "Saindo..."; exit 0 ;;
        *) echo "Opção inválida!" ;;
    esac
    
    echo ""
    read -p "Pressione Enter para continuar..."
done
