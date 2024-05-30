import os, boto3, json
from test import generate_test_event
from handle_sql_queries import bulk_insert_to_rds

sqs = boto3.client('sqs', region_name='eu-west-1')

def is_lambda_about_to_timeout(context, threshold_seconds=10):
    remaining_time = context.get_remaining_time_in_millis() / 1000
    return remaining_time <= threshold_seconds

def is_queue_empty():
    response = sqs.get_queue_attributes(
        QueueUrl=os.getenv('SQS_QUEUE_URL'),
        AttributeNames=['ApproximateNumberOfMessages']
    )
    return int(response['Attributes']['ApproximateNumberOfMessages']) == 0

def lambda_handler(event, context):
    global buffer

    if 'buffer' not in globals():
        buffer.clear()

    for record in event['Records']:
        body = json.loads(record['body'])
        message = json.loads(body['Message'])
        buffer.append(message)
    
    if is_queue_empty() or is_lambda_about_to_timeout(context):
        bulk_insert_to_rds(buffer)
        buffer = []

# Simulate an AWS Lambda event for local testing
if __name__ == "__main__":
    test_event, mock_context = generate_test_event()
    
    lambda_handler(test_event, mock_context)