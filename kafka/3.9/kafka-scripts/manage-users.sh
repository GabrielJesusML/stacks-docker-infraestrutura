#!/bin/bash
# Script interativo para gerenciar usuários e ACLs

BOOTSTRAP_SERVER="localhost:9092"

show_menu() {
    echo ""
    echo "=========================================="
    echo "Gerenciamento de Usuários e ACLs - Kafka"
    echo "=========================================="
    echo "1. Listar todos os usuários"
    echo "2. Criar novo usuário"
    echo "3. Deletar usuário"
    echo "4. Listar todas as ACLs"
    echo "5. Adicionar ACL (WRITE)"
    echo "6. Adicionar ACL (READ)"
    echo "7. Remover ACL"
    echo "8. Listar ACLs de um usuário específico"
    echo "9. Sair"
    echo "=========================================="
    echo -n "Escolha uma opção: "
}

list_users() {
    echo ""
    echo "Usuários cadastrados:"
    kafka-configs.sh --bootstrap-server $BOOTSTRAP_SERVER \
        --describe --entity-type users
}

create_user() {
    echo ""
    read -p "Nome do usuário: " username
    read -sp "Senha: " password
    echo ""
    
    kafka-configs.sh --bootstrap-server $BOOTSTRAP_SERVER \
        --alter --add-config "SCRAM-SHA-256=[password=$password]" \
        --entity-type users --entity-name "$username"
    
    echo "Usuário '$username' criado com sucesso!"
}

delete_user() {
    echo ""
    read -p "Nome do usuário para deletar: " username
    
    kafka-configs.sh --bootstrap-server $BOOTSTRAP_SERVER \
        --alter --delete-config 'SCRAM-SHA-256' \
        --entity-type users --entity-name "$username"
    
    echo "Usuário '$username' deletado!"
}

list_acls() {
    echo ""
    echo "ACLs configuradas:"
    kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVER --list
}

add_write_acl() {
    echo ""
    read -p "Nome do usuário: " username
    read -p "Nome do tópico: " topic
    
    kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVER \
        --add --allow-principal "User:$username" \
        --operation WRITE \
        --operation DESCRIBE \
        --topic "$topic"
    
    echo "ACL de WRITE adicionada para '$username' no tópico '$topic'"
}

add_read_acl() {
    echo ""
    read -p "Nome do usuário: " username
    read -p "Nome do tópico: " topic
    read -p "Consumer group (ou * para todos): " group
    
    kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVER \
        --add --allow-principal "User:$username" \
        --operation READ \
        --operation DESCRIBE \
        --topic "$topic" \
        --group "$group"
    
    echo "ACL de READ adicionada para '$username' no tópico '$topic'"
}

remove_acl() {
    echo ""
    read -p "Nome do usuário: " username
    read -p "Operação (READ/WRITE/CREATE/DELETE): " operation
    read -p "Nome do tópico: " topic
    
    kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVER \
        --remove --allow-principal "User:$username" \
        --operation "$operation" \
        --topic "$topic" \
        --force
    
    echo "ACL removida!"
}

list_user_acls() {
    echo ""
    read -p "Nome do usuário: " username
    
    echo "ACLs do usuário '$username':"
    kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVER \
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
        4) list_acls ;;
        5) add_write_acl ;;
        6) add_read_acl ;;
        7) remove_acl ;;
        8) list_user_acls ;;
        9) echo "Saindo..."; exit 0 ;;
        *) echo "Opção inválida!" ;;
    esac
    
    echo ""
    read -p "Pressione Enter para continuar..."
done
