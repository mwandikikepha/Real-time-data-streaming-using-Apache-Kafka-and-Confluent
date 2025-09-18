from confluent_kafka import Producer, Consumer
import requests
import time
import json

def read_config():
  # reads the client configuration from client.properties
  # and returns it as a key-value map
  config = {}
  with open("client.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config

def produce(topic, config):
    # creates a new producer instance
    producer = Producer(config)
    # send any outstanding or buffered messages to the Kafka broker
 
  
    cities = ['Nairobi', 'Mombasa', 'Kisumu', 'Nakuru','London','Eldoret','Dodoma','Kigali','Meru','Malindi']

    API = "2002102ef9b4e3ec08e61af1497c3a41"

    def weather_stream():
        payloads = []
        for city in cities:
            url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API}"
            response = requests.get(url)
            data =response.json()
            payload = {
            "city": city, 
            "temp": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "condition": data["weather"][0]["main"],
            "timestamp": data["dt"]
        }
            payloads.append(payload)
        return payloads
        

    while True:
        weather_data = weather_stream()
        for weather in weather_data:
            producer.produce(topic, json.dumps(weather))
            print(f"Sent: {weather}")
            time.sleep(1)
            
        producer.flush()
p_config = read_config()
produce('topic_1', p_config)
            
    
        