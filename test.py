from unittest.mock import MagicMock
import boto3, json

sqs = boto3.client('sqs', region_name='eu-west-1')

class MockContext:
    def __init__(self, time_remaining_in_millis):
        self.time_remaining_in_millis = time_remaining_in_millis
    
    def get_remaining_time_in_millis(self):
        return self.time_remaining_in_millis

def generate_test_event():
    sqs.get_queue_attributes = MagicMock(return_value={
            'Attributes': {'ApproximateNumberOfMessages': '0'}
        })

    test_event = {
        "Records": [
            {
                "body": json.dumps({
                    "Type": "Notification",
                    "Message": json.dumps({
                        "messageId": "70096a50-4bd0-4748-b946-f9147dcf2b5f",
                        "locationId": 1494,
                        "location": "Moen",
                        "parameter": "so2",
                        "value": 226.711,
                        "date": {
                            "utc": "2024-05-13 16:26:53.597095",
                            "local": "2024-05-13 16:26:53.597095+02:00"
                        },
                        "unit": "\u00b5g/m\u00b3",
                        "coordinates": {
                            "latitude": 50.7699733,
                            "longitude": 3.3974267
                        },
                        "country": "BE",
                        "city": "Moen",
                        "isMobile": True,
                        "isAnalysis": True,
                        "entity": "analysis",
                        "sensorType": "reference grade"
                    })
                })
            }
        ]
    }

    mock_context = MockContext(time_remaining_in_millis=30000)

    return test_event, mock_context