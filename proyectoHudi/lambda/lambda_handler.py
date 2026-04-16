import json
import os
import boto3

# Initialize the DynamoDB client
dynamodb_client = boto3.client('dynamodb')

# DynamoDB table name obtained from environment variables
dynamodb_table_name = os.environ.get('TableName')

def handler(event, context):
    # Store the event in Lambda logs
    print(event)

    for record in event['Records']:
        # Get payload data from the SQS record
        payload = json.loads(record['body'])
        device_id = payload['device']
        timestamp = payload['timestamp']
        data = payload['data']

        try:
            # Build the item for DynamoDB
            item = {
                'device_id': {'S': device_id},
                'timestamp': {'S': timestamp}
            }
            
            # Iterate over data and add fields to the DynamoDB item
            # IN PRINCIPLE, THERE IS NO NEED TO PROCESS OTHER DATA TYPES
            for key, value in data.items():
                if value is not None:
                    if isinstance(value, bool):
                        item[key] = {'BOOL': value}
                    else:
                        item[key] = {'N' if isinstance(value, (int, float)) else 'S': str(value)}

            # Save data to the DynamoDB table
            dynamodb_client.put_item(
                TableName=dynamodb_table_name,
                Item=item
            )
            print("Data saved to DynamoDB successfully")
        except Exception as e:
            # Print the error in the Lambda console
            print(f"Error writing to DynamoDB table: {e}")

    return {
        'statusCode': 200,
        'body': json.dumps('Processing completed')
    }
