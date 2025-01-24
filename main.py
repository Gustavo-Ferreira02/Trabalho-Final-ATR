import paho.mqtt.client as mqtt
import requests
import psycopg2
import time
from datetime import datetime

# Configurações MQTT
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "crypto/price"

# Configuração da API
API_URL = "https://api.coingecko.com/api/v3/simple/price"
CRYPTO = "bitcoin"
CURRENCY = "usd"
ALERT_PERCENTAGE = 0.01  # Percentual de variação para alertas

# Configurações do Banco (QuestDB)
DB_HOST = "localhost"
DB_PORT = 8812
DB_USER = "admin"
DB_PASSWORD = "quest"
DB_DATABASE = "qdb"

# Variáveis de estado
last_price = None


# Função: Coleta dados da API
def fetch_crypto_price():
    try:
        response = requests.get(API_URL, params={"ids": CRYPTO, "vs_currencies": CURRENCY})
        response.raise_for_status()
        data = response.json()
        return data[CRYPTO][CURRENCY]
    except Exception as e:
        print(f"Erro ao buscar preço: {e}")
        return None


# Função: Processa o preço e verifica alertas
def process_price(price):
    global last_price
    if last_price is None:
        last_price = price
        return None

    percentage_change = ((price - last_price) / last_price) * 100
    if abs(percentage_change) >= ALERT_PERCENTAGE:
        last_price = price
        return f"Alerta! Variação de {percentage_change:.2f}% detectada."
    return None


# Função: Salva dados no QuestDB
def save_to_db(crypto_symbol, price, alert_message):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_DATABASE
        )
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO crypto_data (timestamp, crypto_symbol, price, alert_message) VALUES (%s, %s, %s, %s)",
            (datetime.now(), crypto_symbol, price, alert_message)
        )
        conn.commit()
        cursor.close()
        conn.close()
        print("Dados salvos no QuestDB.")
    except Exception as e:
        print(f"Erro ao salvar dados no QuestDB: {e}")


# Callback: Quando conecta ao broker MQTT
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Conectado ao broker MQTT com sucesso.")
    else:
        print(f"Erro na conexão ao broker: Código {rc}")


# Callback: Quando uma mensagem é publicada
def on_publish(client, userdata, mid):
    print(f"Mensagem publicada. ID: {mid}")


# Configurações do cliente MQTT
client = mqtt.Client()
client.on_connect = on_connect
client.on_publish = on_publish

client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
client.loop_start()


# Loop principal
try:
    while True:
        price = fetch_crypto_price()
        if price is not None:
            print(f"Preço atual do {CRYPTO}: {price} {CURRENCY}")
            alert = process_price(price)
            if alert:
                print(alert)
                save_to_db(CRYPTO, price, alert)
                client.publish(MQTT_TOPIC, f"{CRYPTO}: {price} {CURRENCY} - {alert}")
        time.sleep(60)  # Aguarda X minutos antes de buscar novamente
except KeyboardInterrupt:
    print("Finalizando...")
finally:
    client.loop_stop()
    client.disconnect()
