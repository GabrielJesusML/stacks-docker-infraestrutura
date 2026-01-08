# Guia de Segurança e Gerenciamento de Usuários - Apache Kafka

## Introdução

Este documento explica o modelo de segurança implementado no Apache Kafka 3.9.0 com KRaft, como funciona a autenticação e autorização, e como gerenciar usuários e permissões para suas aplicações.

---

## O que é o Apache Kafka?

O Apache Kafka é uma plataforma de streaming de eventos distribuída, usada para construir pipelines de dados em tempo real e aplicações de streaming. Ele funciona como um sistema de mensageria onde:

- **Producers** (produtores) enviam mensagens para tópicos
- **Consumers** (consumidores) leem mensagens dos tópicos
- **Tópicos** são categorias ou feeds onde as mensagens são publicadas
- **Partições** dividem os tópicos para permitir paralelismo e escalabilidade

---

## Modelo de Segurança Implementado

### Autenticação: SASL/SCRAM-SHA-256

A autenticação garante que apenas usuários conhecidos possam se conectar ao Kafka. Utilizamos o mecanismo SASL/SCRAM-SHA-256, que oferece:

- Autenticação baseada em usuário e senha
- Senhas armazenadas de forma segura usando hash criptográfico
- Proteção contra ataques de replay
- Não requer infraestrutura externa como LDAP ou Kerberos

Cada aplicação que se conecta ao Kafka precisa fornecer credenciais válidas (usuário e senha) para estabelecer a conexão.

### Autorização: ACLs (Access Control Lists)

Após autenticado, o usuário precisa ter permissão para realizar operações. As ACLs definem:

- Quais usuários podem acessar quais tópicos
- Quais operações cada usuário pode realizar (ler, escrever, criar, deletar)
- Quais consumer groups o usuário pode utilizar

O modelo segue o princípio do menor privilégio: cada aplicação recebe apenas as permissões necessárias para sua função.

### Tipos de Usuários

1. **Super User (Administrador)**: Tem acesso total ao Kafka, pode criar tópicos, gerenciar usuários e ACLs. Usado apenas para administração.

2. **Producer**: Usuário que apenas envia mensagens para tópicos específicos. Não consegue ler mensagens.

3. **Consumer**: Usuário que apenas lê mensagens de tópicos específicos. Não consegue enviar mensagens.

4. **Producer + Consumer**: Usuário que pode tanto enviar quanto receber mensagens em tópicos específicos.

---

## Arquitetura de Rede

O Kafka está configurado com dois pontos de acesso (listeners):

### Listener Interno (Porta 9092)
- Usado para comunicação entre serviços dentro da mesma rede Docker
- Acessível pelo hostname "kafka"
- Requer autenticação SASL/SCRAM-SHA-256

### Listener Externo (Porta 9094)
- Usado para aplicações externas se conectarem ao Kafka
- Acessível pelo domínio público: kfkpdhel01.dths.com.br
- Requer autenticação SASL/SCRAM-SHA-256

---

## Gerenciamento de Usuários

### Pré-requisitos

Antes de criar usuários, você precisa:

1. Ter acesso SSH ao servidor onde o Kafka está rodando
2. Conhecer as credenciais do usuário administrador (Super User)

### Acessando o Ambiente de Gerenciamento

1. Conecte-se ao servidor via SSH

2. Entre no container do Kafka executando o comando para acessar o shell do container

3. Configure as credenciais de administrador definindo as variáveis de ambiente KAFKA_ADMIN_USER e KAFKA_ADMIN_PASSWORD com os valores do usuário administrador

### Criando um Novo Usuário

Para criar um usuário, utilize o script de criação de usuários informando:

- Nome do usuário (identificador único para a aplicação)
- Senha (deve ser forte e única)
- Nome do tópico que o usuário terá acesso
- Papel do usuário (producer, consumer ou both)

O script irá:
1. Criar as credenciais SCRAM-SHA-256 para o usuário
2. Configurar automaticamente as ACLs apropriadas para o papel escolhido
3. Exibir as informações de conexão para configurar na aplicação

### Papéis Disponíveis

**Producer**
- Permissão para escrever mensagens no tópico
- Permissão para descrever o tópico (obter metadados)
- Permissão para criar o tópico se não existir
- Ideal para: serviços que geram eventos, APIs que publicam dados

**Consumer**
- Permissão para ler mensagens do tópico
- Permissão para descrever o tópico (obter metadados)
- Permissão para usar consumer groups
- Ideal para: serviços que processam eventos, workers, analytics

**Both (Producer + Consumer)**
- Todas as permissões de producer e consumer combinadas
- Ideal para: serviços que precisam ler e escrever no mesmo tópico

### Gerenciamento Interativo

Para operações mais complexas como listar usuários, alterar senhas, remover ACLs ou deletar usuários, utilize o script de gerenciamento interativo que apresenta um menu com todas as opções disponíveis.

---

## Boas Práticas de Segurança

### Criação de Usuários

1. **Um usuário por aplicação**: Cada aplicação deve ter seu próprio usuário. Nunca compartilhe credenciais entre aplicações diferentes.

2. **Nomes descritivos**: Use nomes que identifiquem claramente a aplicação, como "app-pedidos", "svc-notificacoes", "worker-analytics".

3. **Senhas fortes**: Utilize senhas com pelo menos 16 caracteres, combinando letras, números e símbolos.

4. **Princípio do menor privilégio**: Conceda apenas as permissões necessárias. Se a aplicação só precisa ler, crie como consumer.

### Gerenciamento de Tópicos

1. **Nomenclatura consistente**: Adote um padrão de nomes para tópicos, como "dominio-evento" (ex: pedidos-criados, usuarios-atualizados).

2. **Um tópico por tipo de evento**: Evite misturar diferentes tipos de eventos no mesmo tópico.

3. **Planeje as partições**: O número de partições afeta o paralelismo. Considere o volume esperado de mensagens.

### Rotação de Credenciais

1. **Rotação periódica**: Altere as senhas dos usuários periodicamente (recomendado: a cada 90 dias).

2. **Processo de rotação**: 
   - Crie uma nova senha para o usuário
   - Atualize a configuração da aplicação
   - Faça o deploy da aplicação
   - Verifique se está funcionando corretamente

### Monitoramento

1. **Acompanhe os logs**: O Kafka registra tentativas de autenticação falhas. Monitore para detectar possíveis ataques.

2. **Use o Kafka UI**: A interface web permite visualizar tópicos, mensagens, consumers e o estado geral do cluster.

---

## Configuração nas Aplicações

Após criar um usuário, a aplicação precisa ser configurada com os seguintes parâmetros:

- **Servidor**: kfkpdhel01.dths.com.br:9094
- **Protocolo de segurança**: SASL_PLAINTEXT
- **Mecanismo SASL**: SCRAM-SHA-256
- **Usuário**: o nome do usuário criado
- **Senha**: a senha definida na criação
- **Tópico**: o tópico para o qual o usuário tem permissão

Cada linguagem de programação e framework tem sua própria forma de configurar esses parâmetros. Consulte a documentação da biblioteca Kafka que você está utilizando.

---

## Solução de Problemas

### Erro de Autenticação

Se a aplicação não consegue se conectar com erro de autenticação:

1. Verifique se o usuário e senha estão corretos
2. Confirme que o usuário foi criado corretamente listando os usuários existentes
3. Verifique se não há espaços extras no usuário ou senha

### Erro de Autorização

Se a aplicação conecta mas não consegue produzir ou consumir:

1. Verifique se as ACLs foram configuradas corretamente
2. Confirme que o tópico informado na ACL é o mesmo que a aplicação está tentando acessar
3. Para consumers, verifique se a ACL do consumer group foi criada

### Conexão Recusada

Se a aplicação não consegue estabelecer conexão:

1. Verifique se está usando a porta correta (9094 para acesso externo)
2. Confirme que o firewall permite conexões na porta 9094
3. Teste a conectividade de rede com o servidor

---

## Configuração para Novos Ambientes

Ao implantar esta estrutura em uma nova máquina ou ambiente, algumas configurações precisam ser ajustadas para refletir o novo contexto.

### Arquivo docker-compose.yml

Este é o arquivo principal de configuração. As seguintes variáveis devem ser revisadas:

**Domínio/IP Externo**

A variável KAFKA_ADVERTISED_LISTENERS contém o endereço que as aplicações externas usarão para se conectar. Altere o valor "kfkpdhel01.dths.com.br" para o domínio ou IP público do seu servidor.

**Credenciais do Administrador Kafka**

As variáveis SERVICE_USER_KAFKA e SERVICE_PASSWORD_KAFKA definem o usuário administrador do Kafka. Se estiver usando Coolify, essas variáveis são geradas automaticamente. Caso contrário, defina valores seguros manualmente.

**Credenciais do Kafka UI**

As variáveis SERVICE_USER_ADMIN e SERVICE_PASSWORD_ADMIN definem o login da interface web do Kafka UI. Se estiver usando Coolify, essas variáveis são geradas automaticamente. Caso contrário, defina valores seguros manualmente.

**Volumes de Dados**

O caminho "/mnt/HC_Volume_104350228/kafka_data/data" é onde os dados do Kafka são persistidos. Altere para um caminho apropriado no seu servidor, preferencialmente em um disco com bom desempenho e espaço suficiente.

**Cluster ID**

A variável KAFKA_CLUSTER_ID identifica o cluster. Para um novo ambiente, você pode gerar um novo ID ou manter o existente se estiver migrando dados.

### Arquivo admin.properties

Este arquivo contém as credenciais para executar comandos administrativos manualmente. Preencha os campos de usuário e senha com as credenciais do administrador Kafka do seu ambiente.

### Scripts de Gerenciamento (pasta kafka-scripts)

**create-app-user.sh**

O endereço do servidor exibido nas instruções finais do script está configurado como "kfkpdhel01.dths.com.br:9094". Altere para o domínio ou IP do seu ambiente.

### Aplicações Cliente (pasta kafka-client)

**producer.py e consumer.py**

A variável KAFKA_HOST está configurada com "kfkpdhel01.dths.com.br:9094". Altere para o endereço do Kafka no seu ambiente.

**admin_manager.py**

A variável KAFKA_HOST também precisa ser alterada para o endereço do seu ambiente.

### Resumo das Alterações Necessárias

| Arquivo | Variável/Campo | O que alterar |
|---------|---------------|---------------|
| docker-compose.yml | KAFKA_ADVERTISED_LISTENERS | Domínio ou IP externo do servidor |
| docker-compose.yml | Volumes | Caminho de persistência dos dados |
| docker-compose.yml | SERVICE_USER_KAFKA | Usuário admin (se não usar Coolify) |
| docker-compose.yml | SERVICE_PASSWORD_KAFKA | Senha admin (se não usar Coolify) |
| docker-compose.yml | SERVICE_USER_ADMIN | Usuário do Kafka UI (se não usar Coolify) |
| docker-compose.yml | SERVICE_PASSWORD_ADMIN | Senha do Kafka UI (se não usar Coolify) |
| admin.properties | username | Usuário administrador |
| admin.properties | password | Senha do administrador |
| kafka-scripts/create-app-user.sh | Endereço nas instruções | Domínio ou IP do servidor |
| kafka-client/*.py | KAFKA_HOST | Endereço do servidor Kafka |

### Portas que Devem Estar Abertas

Certifique-se de que as seguintes portas estejam liberadas no firewall do servidor:

- **9092**: Comunicação interna (entre containers Docker)
- **9094**: Acesso externo (aplicações fora do servidor)
- **8080**: Interface web do Kafka UI

---

## Referências

- Documentação oficial do Apache Kafka: https://kafka.apache.org/documentation/
- Documentação de segurança do Kafka: https://kafka.apache.org/documentation/#security
- KRaft (Kafka sem Zookeeper): https://kafka.apache.org/documentation/#kraft

---

*Documento atualizado em Janeiro de 2026*
