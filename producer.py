import json  
import requests  
from kafka import KafkaProducer  

# Initialize Kafka producer  

producer = KafkaProducer(bootstrap_servers='localhost:9092', acks='all',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))  

# Weather API configuration  

api_key = 'ea3051e935c22f344513efd8f59f10f3'  

city = 'Belgrade'  

lat=44.787197 

lon=20.457273 

weather_api_url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}'  

# Fetch weather data from the Weather API  

response = requests.get(weather_api_url)  

weather_data = response.json() 

# Send weather data to Kafka topic  

producer.send('raw-weather', value=weather_data)  

# Close the producer  

producer.close() 