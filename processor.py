import paho.mqtt.client as mqtt
import psycopg2
import sys
from datetime import datetime, timedelta, timezone
from threading import Thread, Lock
from collections import deque, defaultdict
import time
import json
import pytz

local_tz = pytz.timezone("America/Sao_Paulo")  # Defina o fuso horário correto
local_time = datetime.now(local_tz)
utc_time = local_time.astimezone(pytz.utc)

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
BASE_MQTT_TOPIC = "crypto/price/"
MONITOR_TOPIC = "sensor_monitors"
DB_HOST = "localhost"
DB_PORT = 8812
DB_USER = "admin"
DB_PASSWORD = "quest"
DB_DATABASE = "qdb"
QUEUE_SIZE = 100  
CRYPTO_TIMEOUT = 5  # seconds

queue = deque(maxlen=QUEUE_SIZE)  
queue_lock = Lock()  
last_prices = {}
price_thresholds = {}
monitored_cryptos = set()
last_crypto_message_time = defaultdict(datetime.now)

def adiciona_msg_fila(message):
    global queue, queue_lock  
    utc_timestamp = datetime.now(timezone.utc)  # Timestamp UTC da chegada da mensagem
    with queue_lock:  
        if len(queue) < QUEUE_SIZE:
            queue.append((message, utc_timestamp))
        else:
            queue.pop()
            queue.append((message, utc_timestamp))

def calculate_variation(current_price, last_price):
    if last_price is None or last_price == 0:
        return 0
    return ((current_price - last_price) / last_price) * 100

def gera_alarme(crypto, alarm_name):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_DATABASE
        )
        cursor = conn.cursor()
        timestamp = datetime.now(timezone.utc).isoformat(timespec='milliseconds')
        cursor.execute(
            "INSERT INTO crypto_alarms (timestamp, crypto_symbol, alarm_name) VALUES (%s, %s, %s)",
            (timestamp, crypto, alarm_name)
        )
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Alarm generated for {crypto}: {alarm_name}")
    except Exception as e:
        print(f"Erro ao gerar alarme: {e}")

def escreve_banco(crypto, price, variation):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_DATABASE
        )
        cursor = conn.cursor()
        timestamp = datetime.now(timezone.utc).isoformat(timespec='milliseconds')
        cursor.execute(
            "INSERT INTO crypto_data (timestamp, crypto_symbol, price, pct_variation) VALUES (%s, %s, %s, %s)",
            (timestamp, crypto, price, variation)
        )
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Dados de {crypto} salvos no QuestDB. Variação: {variation:.4f}%")
    except Exception as e:
        print(f"Erro ao salvar dados no QuestDB: {e}")

def verifica_alarmes(crypto, price, last_price):
    if last_price is not None:
        two_measure_variation = abs(calculate_variation(price, last_price))
        if two_measure_variation > 0.5:
            gera_alarme(crypto, "Significant Two-Measurement Variation")
        
        threshold = price_thresholds.get(crypto, last_price)
        total_variation = calculate_variation(price, threshold)
        
        if abs(total_variation) >= 5:
            gera_alarme(crypto, "Over 5% Total Variation")
            price_thresholds[crypto] = price

def verifica_presenca_cryptos():
    while True:
        current_time = datetime.now()
        for crypto in monitored_cryptos:
            time_since_last_message = current_time - last_crypto_message_time[crypto]
            if time_since_last_message.total_seconds() > CRYPTO_TIMEOUT:
                gera_alarme(crypto, "Crypto cadastrada mas não recebida")
        time.sleep(1)

def processa_mensagem():
    while True:
        queue_lock.acquire()
        if len(queue) > 0:
            message, msg_time = queue.popleft()  # Agora pegamos também o timestamp da mensagem
            queue_lock.release()
            try:
                crypto, price_info = message.split(":")
                price = float(price_info.split()[0])
                
                # Get last known price
                last_price = last_prices.get(crypto)
                
                # Verify and generate alarms
                verifica_alarmes(crypto, price, last_price)
                
                # Calculate variation
                variation = calculate_variation(price, last_price)
                
                # Update last known price
                last_prices[crypto] = price
                
                # Write to database
                escreve_banco(crypto.strip(), price, variation)
            except Exception as e:
                print(f"Erro ao processar mensagem da fila: {e}")
        else:
            queue_lock.release()
            time.sleep(0.1)  # Aguarda 100ms se a fila estiver vazia, para não consumir recursos excessivamente

def on_connect(client, userdata, flags, rc):
    print("Conectado ao broker MQTT." if rc == 0 else f"Erro na conexão MQTT: Código {rc}")
    for crypto in cryptos:
        topic = f"{BASE_MQTT_TOPIC}{crypto}"
        client.subscribe(topic)
        print(f"Inscrito em {topic}")
    
    client.subscribe(MONITOR_TOPIC)
    print(f"Inscrito em {MONITOR_TOPIC}")

def on_message(client, userdata, msg):
    try:
        if msg.topic == MONITOR_TOPIC:
            monitor_message = json.loads(msg.payload.decode("utf-8"))
            print(f"Sensor Monitor Message: {monitor_message}")
            
            monitored_cryptos.clear()
            for sensor in monitor_message.get('sensors', []):
                monitored_cryptos.add(sensor['sensor_id'])
            
            return

        message = msg.payload.decode("utf-8")
        print(f"Mensagem recebida no tópico {msg.topic}: {message}")
        data = json.loads(message)
        crypto = data["symbol"]
        price = data["price"]
        
        last_crypto_message_time[crypto] = datetime.now()  # Atualiza o horário da última mensagem para o crypto
        
        adiciona_msg_fila(f"{crypto}: {price}")
    except Exception as e:
        print(f"Erro ao processar mensagem: {e}")

def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    
    processor_thread = Thread(target=processa_mensagem, daemon=True)
    processor_thread.start()
    
    presence_thread = Thread(target=verifica_presenca_cryptos, daemon=True)
    presence_thread.start()
    
    client.loop_forever()

if __name__ == "__main__":
    cryptos = sys.argv[1:] if len(sys.argv) > 1 else ["BTCUSDT", "ETHUSDT", "DOGEUSDT"]
    
    print(f"Monitorando: {cryptos}")
    main()

