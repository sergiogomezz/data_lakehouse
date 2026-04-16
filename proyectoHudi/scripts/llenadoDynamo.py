import json
import boto3
import random
import time
import datetime
import aws_cdk as cdk

# Initialize the DynamoDB client
dynamodb_client = boto3.client('dynamodb')

# Global variables
dynamodb_table_name = cdk.Fn.import_value("TableName")

for i in range(10):
    # Generate mock data
    device_name = random.choice(['device_1', 'device_2', 'device_3'])
    timestamp = datetime.datetime.now().isoformat()
    temperature = round(random.uniform(10, 40), 2) 
    humidity = round(random.uniform(30, 100), 2) 
    pressure = round(random.uniform(900, 1100), 2)  
    
    # Insert the record into the DynamoDB table
    dynamodb_client.put_item(
        TableName=dynamodb_table_name,
        Item={
            'device_id': {'S': device_name},
            'timestamp': {'S': timestamp},
            'temperature': {'N': str(temperature)},
            'humidity': {'N': str(humidity)},
            'pressure': {'N': str(pressure)}
        }
    )
