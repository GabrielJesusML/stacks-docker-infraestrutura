from kafka import KafkaProducer
import json
import time

# ============================================
# CONFIGURAÇÃO - ALTERE AQUI
# ============================================
KAFKA_HOST = "kfkpdhel01.dths.com.br:9094"
KAFKA_USER = "6kkH0oilLh2Q7ve1"      # Preencha com SERVICE_USER_KAFKA
KAFKA_PASSWORD = "pFWThxHjZyp5tNpS6qlatN3Ru10pGvjD"  # Preencha com SERVICE_PASSWORD_KAFKA
TOPIC = "test-topic"
# ============================================

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_HOST],
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username=KAFKA_USER,
    sasl_plain_password=KAFKA_PASSWORD,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Conectado ao Kafka em {KAFKA_HOST}")
print(f"Enviando mensagens para o tópico '{TOPIC}'...")
print("Pressione Ctrl+C para parar\n")

try:
    count = 0
    while True:
        count += 1
        message = {
            "id": count,
            "message": f"Hello Kafka #{count}",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        future = producer.send(TOPIC, value=message)
        result = future.get(timeout=10)
        
        print(f"✓ Enviado: {message}")
        time.sleep(2)
        
except KeyboardInterrupt:
    print("\n\nParando producer...")
finally:
    producer.close()
    print("Producer finalizado.")
