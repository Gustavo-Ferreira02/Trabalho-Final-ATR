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
CRYPTOS = ["bitcoin", "ethereum", "dogecoin"]
CURRENCY = "usd"

# Configurações do Banco (QuestDB)
DB_HOST = "localhost"
DB_PORT = 8812
DB_USER = "admin"
DB_PASSWORD = "quest"
DB_DATABASE = "qdb"

# Configuração do Alerta
ALERT_PERCENTAGE = 0.05  # Percentual de variação para destaque no Grafana

# Estado anterior dos preços
last_prices = {}

# Função: Coleta os preços das criptomoedas via API
def fetch_crypto_prices():
    try:
        response = requests.get(API_URL, params={"ids": ",".join(CRYPTOS), "vs_currencies": CURRENCY})
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Erro ao buscar preços: {e}")
        return None

# Função: Processa preços e detecta variações relevantes
def process_prices(prices):
    global last_prices
    alerts = {}

    for crypto, data in prices.items():
        price = data[CURRENCY]
        
        if crypto in last_prices:
            percentage_change = ((price - last_prices[crypto]) / last_prices[crypto]) * 100
            if percentage_change != 0:  # Apenas se houver mudança
                alerts[crypto] = f"Variação de {percentage_change:.2f}% detectada!" if abs(percentage_change) >= ALERT_PERCENTAGE else None

        last_prices[crypto] = price  # Atualiza o último preço

    return alerts

# Função: Salva os dados no QuestDB
def save_to_db(crypto, price, alert_message):
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
            (datetime.now(), crypto, price, alert_message if alert_message else "")
        )
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Dados de {crypto} salvos no QuestDB.")
    except Exception as e:
        print(f"Erro ao salvar dados no QuestDB: {e}")

# Callback: Quando conecta ao broker MQTT
def on_connect(client, userdata, flags, rc):
    print("Conectado ao broker MQTT." if rc == 0 else f"Erro na conexão MQTT: Código {rc}")

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
        prices = fetch_crypto_prices()
        if prices:
            alerts = process_prices(prices)
            for crypto, data in prices.items():
                price = data[CURRENCY]
                alert_message = alerts.get(crypto)
                save_to_db(crypto, price, alert_message)
                
                # Publica no MQTT apenas se houver mudança
                client.publish(MQTT_TOPIC, f"{crypto}: {price} {CURRENCY} - {alert_message if alert_message else 'Sem alerta'}")
        
        time.sleep(300)  # Atualiza a cada 300 segundos
except KeyboardInterrupt:
    print("Finalizando...")
finally:
    client.loop_stop()
    client.disconnect()
