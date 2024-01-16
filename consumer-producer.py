from kafka import KafkaConsumer, KafkaProducer
import requests, json

#consumer
consumer = KafkaConsumer('raw-weather',
                         group_id='my_group',
                         bootstrap_servers='localhost:9092',
                         value_deserializer=lambda x:json.loads(x.decode('utf-8')))
#producer
producer = KafkaProducer(bootstrap_servers='localhost:9092', acks='all')

nodejs_server_url="https:/localhost:8000/api/receive-data"


for message in consumer:
    weather_data_json = message.value

    temp_kelvin = float(weather_data_json['main']['temp'])
    temp_celsius = temp_kelvin - 273.15
    

    processed_weather_data={
        'city': weather_data_json.get('name', 'City'),
        'temp_celsius' : temp_celsius,
        'date': weather_data_json.get('dt','no date')
    }

producer.send('weather-processed', value=processed_weather_data)
producer.flush()

consumer.close()
producer.close()
