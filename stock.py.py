import boto3
import json
import random
import datetime
import time

stream_name = 'ExampleInputStream'
region_name = 'us-east-1'


def send_data_to_kinesis(data):
    kinesis = boto3.client('kinesis', region_name=region_name)
    response = kinesis.put_record(
        StreamName=stream_name,
        Data=json.dumps(data),
        PartitionKey='partitionkey'
    )
    print('Data sent to Kinesis:', response)

def generate_data():
    while True:
        data = {
            'event_time': datetime.datetime.now().isoformat(),
            'close': round(random.uniform(4500, 4800), 2)
        }
        send_data_to_kinesis(data)
        print(data)
        time.sleep(1)

if __name__ == '__main__':
    # Esperar unos segundos para que el stream se configure correctamente
    time.sleep(10)

    # Generar datos y enviarlos al stream de Kinesis
    generate_data()