import requests
import time
import paho.mqtt.client as mqtt
import json
import sys
import socket
import threading

BINANCE_API_URL = "https://api.binance.com/api/v3/ticker/price"
UPDATE_INTERVAL = 0.5  
MONITOR_INTERVAL = 10

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
BASE_MQTT_TOPIC = "crypto/price/"
MONITOR_TOPIC = "sensor_monitors"

def format_price(price):
    return f"{price:.4f}"

def fetch_crypto_prices(cryptos):
    try:
        response = requests.get(BINANCE_API_URL)
        response.raise_for_status()
        prices = response.json()
        
        filtered_prices = {item["symbol"]: format_price(float(item["price"])) for item in prices if item["symbol"] in cryptos}
        return filtered_prices
    except Exception as e:
        print(f"Error fetching prices from Binance: {e}")
        return {}

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker.")
    else:
        print(f"MQTT connection error: Code {rc}")

def send_sensor_monitor(client, cryptos):
    while True:
        machine_id = socket.gethostname()
        monitor_message = {
            "machine_id": machine_id,
            "sensors": [
                {
                    "sensor_id": crypto,
                    "data_type": "Preço USDT",
                    "data_interval": UPDATE_INTERVAL
                } for crypto in cryptos
            ]
        }
        client.publish(MONITOR_TOPIC, json.dumps(monitor_message))
        print(f"Sensores sendo publicados: {monitor_message}",end="\n")
        time.sleep(MONITOR_INTERVAL)

def main(cryptos):
    client = mqtt.Client()
    client.on_connect = on_connect
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    client.loop_start()

    # Start sensor monitor thread
    monitor_thread = threading.Thread(target=send_sensor_monitor, args=(client, cryptos), daemon=True)
    monitor_thread.start()

    try:
        while True:
            crypto_prices = fetch_crypto_prices(cryptos)
            if crypto_prices:
                for crypto, price in crypto_prices.items():
                    message = json.dumps({
                        "symbol": crypto,
                        "price": price,
                        "timestamp": time.time()
                    })
                    topic = f"{BASE_MQTT_TOPIC}{crypto}"
                    client.publish(topic, message)
                    print(f"Mensagem publicada em {topic}: {message}",end="\n")
            time.sleep(UPDATE_INTERVAL)  
    except KeyboardInterrupt:
        print("Finalizando Coletor")
    finally:
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    cryptos = sys.argv[1:] if len(sys.argv) > 1 else ["BTCUSDT", "ETHUSDT", "DOGEUSDT"]
    
    print(f"Cryptos sendo coletadas: {cryptos}",end="\n")
    main(cryptos)
