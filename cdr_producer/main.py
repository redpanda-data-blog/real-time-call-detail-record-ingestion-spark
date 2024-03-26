import json
import uuid

from kafka import KafkaProducer

# Define the Redpanda topic and Kafka producer
topic = "cdr_topic"
producer = KafkaProducer(bootstrap_servers='localhost:19092',
                         key_serializer=str.encode,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Input CDRs file - JSON format
source_file = "cdr_records.json"

# Open the file and read the CDRs
with open(source_file, 'r') as file:
    cdrs = json.load(file)
    for cdr in cdrs:
        # Generate message key (optional)
        message_key = str(uuid.uuid4())

        # Send the CDR to the Redpanda topic
        future = producer.send(topic, key=message_key, value=cdr)
        # Block until a single message is sent (or timeout in 15 seconds)
        result = future.get(timeout=15)

        print("Message sent, partition: ", result.partition, ", offset: ", result.offset)
