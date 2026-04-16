import random
import json
import datetime
import time
import ssl
import paho.mqtt.client as mqtt

IOT_CORE_ENDPOINT = 'avfhezxts3vkr-ats.iot.eu-west-1.amazonaws.com' # IoT Core endpoint
PATH_TO_CERTIFICATE = 'certificates/certificate.pem.crt' # Thing certificate path
PATH_TO_PRIVATE_KEY = 'certificates/private.pem.key' # Private key path
PATH_TO_AMAZON_ROOT_CA_1 = 'certificates/AmazonRootCA1.pem' # Amazon root CA certificate path
CLIENT_ID = 'device_1' # Client ID

# Function that generates telemetry data
def generate_telemetry_data():
    device_name = random.choice(['device_1', 'device_2', 'device_3'])
    timestamp = datetime.datetime.now().isoformat()
        
    # Generate random temperature data
    if random.random() < 0.95:
        temperature = round(random.uniform(10, 40), 2) # Degrees Celsius
    else:
        temperature =None

    # Generate random humidity data
    if random.random() < 0.95:
        humidity = round(random.uniform(30, 100), 2) 
    else:
        humidity = None

    # Generate random pressure data
    if random.random() < 0.95:
        pressure = round(random.uniform(900, 1100), 2)  
    else:
        pressure = None


    # Build the JSON payload
    payload = {
        "device": device_name,
        "timestamp": timestamp,
        "data": {
            "temperature": temperature,
            "humidity": humidity,
            "pressure": pressure
        }
    }
    return payload

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

if __name__ == '__main__':
    # MQTT client configuration
    client = mqtt.Client(CLIENT_ID, protocol=mqtt.MQTTv311)
    client = mqtt.Client(CLIENT_ID, protocol=mqtt.MQTTv311)
    client.tls_set(ca_certs=PATH_TO_AMAZON_ROOT_CA_1, certfile=PATH_TO_CERTIFICATE, keyfile=PATH_TO_PRIVATE_KEY, tls_version=ssl.PROTOCOL_TLSv1_2)
    client.on_connect = on_connect

    print("Connecting to {} with client ID '{}'...".format(IOT_CORE_ENDPOINT, CLIENT_ID))
    client.connect(IOT_CORE_ENDPOINT, 8883) # Sometimes it is necessary to switch it to 433

    # Publish message
    TOPIC = 'telemetry_data'

    while True:
        try:
            data = generate_telemetry_data()
            client.publish(topic=TOPIC, payload=json.dumps(data), qos=1)
            print("Published to the topic: " + TOPIC)
        except Exception as e:
            print("Error publishing to topic {}: {}".format(TOPIC, str(e)))
        time.sleep(1)
