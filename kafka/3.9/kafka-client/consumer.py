from kafka import KafkaConsumer
import json

# ============================================
# CONFIGURAÇÃO - ALTERE AQUI
# ============================================
KAFKA_HOST = "kfkpdhel01.dths.com.br:9094"
KAFKA_USER = "6kkH0oilLh2Q7ve1"      # Preencha com SERVICE_USER_KAFKA
KAFKA_PASSWORD = "pFWThxHjZyp5tNpS6qlatN3Ru10pGvjD"  # Preencha com SERVICE_PASSWORD_KAFKA
TOPIC = "test-topic"
# ============================================

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_HOST],
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username=KAFKA_USER,
    sasl_plain_password=KAFKA_PASSWORD,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='test-consumer-group'
)

print(f"Conectado ao Kafka em {KAFKA_HOST}")
print(f"Consumindo mensagens do tópico '{TOPIC}'...")
print("Pressione Ctrl+C para parar\n")

try:
    for message in consumer:
        print(f"✓ Recebido: {message.value}")
        print(f"  Partição: {message.partition}, Offset: {message.offset}\n")
        
except KeyboardInterrupt:
    print("\n\nParando consumer...")
finally:
    consumer.close()
    print("Consumer finalizado.")
