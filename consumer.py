# -*- coding: utf-8 -*-
import boto3
import json
from decimal import Decimal
import time

stream_name = 'ExampleInputStream'
region_name = 'us-east-1'
upper_band = 4700  # Valor de la franja superior de Bollinger
lower_band = 4600  # Valor de la franja inferior de Bollinger
shard_id = 'shardId-000000000003'  # Shard ID específico

def process_record(record):
    data = json.loads(record['Data'])
    
    if 'close' in data:
        close_price = Decimal(str(data['close']))
        
        print("Precio de cierre: " + str(close_price))
        
        if close_price > upper_band:
            print("ALERTA: El precio (" + str(close_price) + ") supera la franja superior de Bollinger.")
        
        if close_price < lower_band:
            print("ALERTA: El precio (" + str(close_price) + ") está por debajo de la franja inferior de Bollinger.")
    else:
        print("No se encontró la clave 'close' en los datos recibidos:", data)



def consume_from_kinesis(consumer_name):
    kinesis = boto3.client('kinesis', region_name=region_name)
    
    while True:
        shard_iterator_response = kinesis.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType='TRIM_HORIZON'
        )
        
        shard_iterator = shard_iterator_response['ShardIterator']
        
        while True:
            records_response = kinesis.get_records(ShardIterator=shard_iterator, Limit=1000)
            records = records_response['Records']
            
            print("Número de registros: " + str(len(records)))
            
            for record in records:
                process_record(record)
            
            if 'NextShardIterator' not in records_response:
                break
            
            shard_iterator = records_response['NextShardIterator']
            time.sleep(1)

if __name__ == '__main__':
    consume_from_kinesis("Upper Band Alert Consumer")
