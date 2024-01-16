from kafka import KafkaConsumer
import requests, json

# Initialize Kafka consumer
consumer = KafkaConsumer('weather-processed', bootstrap_servers='localhost:9092', group_id='my-group')

# Initialize Node.js server endpoint
nodejs_server_url = 'http://localhost:8000/api/receive-data'

# Read and process messages from the Kafka topic
for message in consumer:
    # Decode and load JSON data from the Kafka message
    processed_weather_data = message.value.decode('utf-8')
    processed_weather_data_json = json.loads(processed_weather_data)
    
    # Send data to Node.js server
    response = requests.post(nodejs_server_url, json=processed_weather_data_json)
    
    # Print the response from the Node.js server
    print(f"Response from Node.js server: {response.text}")

# Close the consumer (this part will not be reached in this example)
consumer.close()
