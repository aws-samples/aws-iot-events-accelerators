import sys
import json
import random
import time
import logging
import boto3
from urllib.parse import unquote_plus
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    
    logger.info("mlLambda Received event: {}".format(json.dumps(event)))
    pumpData = json.loads(json.dumps(event))
    motorID = pumpData['motorID']
    motorType = pumpData['motorType']
    
    # call sagemaker endpoint from here
    runtime = boto3.client('runtime.sagemaker')
    # sagemaker_Response = runtime.invoke_endpoint(EndpointName='xxxxxxx', ContentType='text/csv', Body=payload)
    
    # get prediction PASS / FAIL from Sagemaker
    prediction = 0

    # call iotevents to use batch put api
    client = boto3.client('iotevents-data')
    
    response = client.batch_put_message(
        messages=[
            {
                'messageId': str(random.randint(1, 4098)),
                'inputName': "IoTImlPumpInference",
                'payload': json.dumps({'motorID': motorID, 'motorType': motorType, 'prediction' : prediction}, indent = 4)
            },
        ]
    )

    logger.info(json.dumps({'motorID': motorID, 'motorType': motorType, 'prediction' : prediction}, indent = 4))
    logger.info(response)
    
    return {
        'statusCode': 200
    }
