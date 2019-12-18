import sys
import json
import logging
import cfnresponse
import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger()
logger.setLevel(logging.INFO)

policyDocument = {
    "Version": "2012-10-17",
    "Statement": [
        {"Effect": "Allow", "Action": "iot:*", "Resource": "*"},
    ],
}


def handler(event, context):
    responseData = {}
    try:
        logger.info("Received event: {}".format(json.dumps(event)))
        result = cfnresponse.FAILED
        iote_client = boto3.client('iotevents')
        iot_client = boto3.client('iot')
        SNSArn = event["ResourceProperties"]["SNSArn"]
        
        if event["RequestType"] == "Create":
            logger.info(
                "In Create "
            ) 
            logger.info("create the iote input")

            # create iotevents inputs - This is supported in CFN
            resp = iote_client.create_input(
                inputName = 'IoTImlPumpData',
                inputDescription = 'IoTImlPumpData',
                inputDefinition = {
                    'attributes': [
                        {
                            "jsonPath": "motorType"
                        },
                        {
                            "jsonPath": "motorID"
                        },
                        {
                            "jsonPath": "TT01"
                        },
                        {
                            "jsonPath": "TT02"
                        },
                        {
                            "jsonPath": "TT03"
                        },
                        {
                            "jsonPath": "TT04"
                        },
                        {
                            "jsonPath": "TT05"
                        },
                        {
                            "jsonPath": "TT06"
                        },
                        {
                            "jsonPath": "TT07"
                        },
                        {
                            "jsonPath": "TT08"
                        },
                        {
                            "jsonPath": "TT09"
                        },
                        {
                            "jsonPath": "TT10"
                        },
                        {
                            "jsonPath": "PT01"
                        },
                        {
                            "jsonPath": "FT01"
                        }                                                                            
                    ]
                }
            )

            resp = iote_client.create_input(
                inputName = 'IoTImlPumpAction',
                inputDescription = 'IoTImlPumpAction',
                inputDefinition = {
                    'attributes': [
                        {
                            "jsonPath": "motorType"
                        },
                        {
                            "jsonPath": "motorID"
                        },
                        {
                            "jsonPath": "service_complete"
                        }
                    ]
                }
            )

            resp = iote_client.create_input(
                inputName = 'IoTImlPumpInference',
                inputDescription = 'IoTImlPumpInference',
                inputDefinition = {
                    'attributes': [
                        {
                            "jsonPath": "motorType"
                        },
                        {
                            "jsonPath": "motorID"
                        },
                        {
                            "jsonPath": "prediction"
                        }
                    ]
                }
            )
 
            logger.info("Create the detector model for iote events")

            # create iotevents detector model - This is supported in CFN
            response = iote_client.create_detector_model(
                detectorModelName = 'IoTImlDetectorModel',
                detectorModelDefinition = {
                'states': [
                    {
                        "stateName": "initialize",
                        "onInput": {
                            "events": [], 
                            "transitionEvents": [
                                {
                                    "eventName": "init_complete",
                                    "condition": "true",                                    
                                    "actions": [
                                        {
                                            "setVariable": {
                                                "variableName": "operation_minutes",
                                                "value": "0"
                                            }      
                                        }
                                    ],                             
                                    "nextState": "motor_off"
                                } 
                            ]                           
                        },
                        "onEnter": {
                            "events": []
                        },
                        "onExit": {
                            "events": []
                        }
                    },
                    {
                        "stateName": "motor_off",
                        "onInput": {
                            "events": [], 
                            "transitionEvents": [
                                {
                                    "eventName": "motor_start",
                                    "condition": "$input.IoTImlPumpData.TT01 > 10",                                    
                                    "actions": [
                                        {
                                            "setTimer": {
                                                "timerName": "motorOperationDuration",
                                                "seconds": 60
                                            }
                                        }
                                    ], 
                                    "nextState": "motor_on"
                                }
                            ]
                        },
                        "onEnter": {
                            "events": []
                       },
                        "onExit": {
                            "events": []
                        }
                    },
                    {
                        "stateName": "motor_on",
                        "onInput": {
                            "events": [
                                {
                                    "eventName": "timer_expired", 
                                    "condition": "timeout('motorOperationDuration')",
                                    "actions": [
                                        {
                                            "setVariable": {
                                                "variableName": "operation_minutes",
                                                "value": "$variable.operation_minutes+1"                                            
                                            }
                                        },
                                        {
                                            "resetTimer": {
                                                "timerName": "motorOperationDuration"
                                            }                                        
                                        }
                                    ]
                                },
                                {
                                    "eventName": "temperature_normal",
                                    "condition": "$input.IoTImlPumpData.FT01 <= 200",
                                    "actions": [
                                        {
                                            "clearTimer": {
                                                "timerName": "motortemperatureHigh"
                                            }                                            
                                        },
                                        {
                                            "setVariable": {
                                                "variableName": "high_temperature_timer_state",
                                                "value": "0"
                                            }      
                                        }                                        
                                    ]
                                },
                                {
                                    "eventName": "temperature_high",
                                    "condition": "$input.IoTImlPumpData.FT01 > 200 && $variable.high_temperature_timer_state == 0",
                                    "actions": [
                                        {
                                            "resetTimer": {
                                                "timerName": "motortemperatureHigh"
                                            }                                            
                                        },
                                        {
                                            "setVariable": {
                                                "variableName": "high_temperature_timer_state",
                                                "value": "1"
                                            }      
                                        }                                        
                                    ]                                    
                                }
                            ],
                            "transitionEvents": [
                                {
                                    "eventName": "motor_stop",
                                    "condition": "$input.IoTImlPumpData.TT01 <= 10",                                    
                                    "actions": [
                                        {
                                            "setVariable": {
                                                "variableName": "operation_minutes",
                                                "value": "$variable.operation_minutes+1"                                            
                                            }                                        
                                        }
                                    ], 
                                    "nextState": "motor_off"
                                },
                                { 
                                    "eventName": "runtime_exceeded",
                                    "condition": "$variable.operation_minutes > 6000",                                    
                                    "actions": [], 
                                    "nextState": "scheduled_service"
                                },
                                { 
                                    "eventName": "failure_predicted",
                                    "condition": "$input.IoTImlPumpInference.prediction == 1",                                    
                                    "actions": [], 
                                    "nextState": "likely_failure_24hrs"
                                },
                                { 
                                    "eventName": "threshold_breach",
                                    "condition": "timeout('motortemperatureHigh')",                                    
                                    "actions": [], 
                                    "nextState": "high_temperature"
                                }                                                               
                            ]
                        },
                        "onEnter": {
                            "events": [
                                {
                                    "eventName": "high_temperature_timer",
                                    "condition":"true",
                                    "actions": [
                                        {
                                            "setTimer": {
                                                "timerName": "motortemperatureHigh",
                                                "seconds": 300
                                            }                                            
                                        },
                                        {
                                            "setVariable": {
                                                "variableName": "high_temperature_timer_state",
                                                "value": "1"
                                            }      
                                        }
                                    ]
                                }
                            ]
                        },
                        "onExit": {
                            "events": [
                                {
                                    "eventName": "clear_timer",
                                    "condition": "true",
                                    "actions": [
                                        {
                                            "clearTimer": {
                                                "timerName": "motorOperationDuration"
                                            }
                                        }
                                    ]
                                }   
                            ]
                        }
                    },
                    {
                        "stateName": "scheduled_service",
                        "onInput": {
                            "events": [],
                            "transitionEvents": [
                                {
                                    "eventName": "service_complete",
                                    "condition": "$input.IoTImlPumpAction.service_complete == 1",                                    
                                    "actions": [
                                        {
                                            "setVariable": {
                                                "variableName": "operation_minutes",
                                                "value": "0"                                            
                                            }                                  
                                        }
                                    ], 
                                    "nextState": "motor_off"
                                }
                            ]
                        },
                        "onEnter": {
                            "events": [
                                {
                                    "eventName": "scheduled_service_sns",
                                    "condition": "true",
                                    "actions": [
                                        {
                                            "sns": {
                                                "targetArn": SNSArn
                                            }
                                        }
                                    ]
                                }
                            ]
                        },
                        "onExit": {
                            "events": []
                        }
                    },
                    {
                        "stateName": "high_temperature",
                        "onInput": {
                            "events": [],
                            "transitionEvents": [
                                {
                                    "eventName": "service_complete",
                                    "condition": "$input.IoTImlPumpAction.service_complete == 1",                                    
                                    "actions": [
                                        {
                                            "setVariable": {
                                                "variableName": "operation_minutes",
                                                "value": "0"                                            
                                            }                                  
                                        }
                                    ], 
                                    "nextState": "motor_off"
                                }
                            ]
                        },
                        "onEnter": {
                            "events": [
                                {
                                    "eventName": "high_temperature_sns", 
                                    "condition": "true",
                                    "actions": [
                                        {
                                            "sns": {
                                                "targetArn": SNSArn
                                            }
                                        }
                                    ]
                                }
                            ]
                        },
                        "onExit": {
                            "events": []
                        }
                    },
                    {
                        "stateName": "likely_failure_24hrs",
                        "onInput": {
                            "events": [],
                            "transitionEvents": [
                                {
                                    "eventName": "service_complete",
                                    "condition": "$input.IoTImlPumpAction.service_complete == 1",                                    
                                    "actions": [
                                        {
                                            "setVariable": {
                                                "variableName": "operation_minutes",
                                                "value": "0"                                            
                                            }                                  
                                        }
                                    ], 
                                    "nextState": "motor_off"
                                }
                            ]
                        },
                        "onEnter": {
                            "events": [
                                {
                                    "eventName": "likely_failure_24hrs_sns",
                                    "condition": "true",
                                    "actions": [
                                        {
                                            "sns": {
                                                "targetArn": SNSArn
                                            }
                                        }
                                    ]
                                }
                            ]
                        },
                        "onExit": {
                            "events": []
                        }
                    }
                ],
                "initialStateName": "initialize"
                },
                detectorModelDescription = 'Detector Model for IoT Interactive Machine Learning',
                key = 'motorID',
                roleArn = event['ResourceProperties']['IoTEventRoleArn']
                #evaluationMethod = 'SERIAL'
            )

            logger.info('update rule action with iote input')
            
            #update rule action with iote input - This is not supported in CFN and hence this Lambda function
            response = iot_client.create_topic_rule(
                ruleName='IoTImlPumpDataRule',
                topicRulePayload={
                    'sql': "select * from 'iml_iotevents_data'",
                    'description': 'Routes IoTPump data for processing.',
                    'actions': [
                        {
                            'iotEvents': {
                                'inputName': 'IoTImlPumpData',
                                'roleArn': event['ResourceProperties']['IoTEventRoleArn']
                            }
                        },
                    ],
                    'ruleDisabled': False,
                }
            )

            #update rule action with iote input - This is not supported in CFN and hence this Lambda function
            response = iot_client.create_topic_rule(
                ruleName='IoTImlPumpActionRule',
                topicRulePayload={
                    'sql': "select * from 'iml_iotevents_action'",
                    'description': 'Routes IoTPump action for processing.',
                    'actions': [
                        {
                            'iotEvents': {
                                'inputName': 'IoTImlPumpAction',
                                'roleArn': event['ResourceProperties']['IoTEventRoleArn']
                            }
                        },
                    ],
                    'ruleDisabled': False,
                }
            )

            result = cfnresponse.SUCCESS       
            
        elif event["RequestType"] == "Update":
            logger.info(
                "In Update "
            ) 
            result = cfnresponse.SUCCESS

        elif event["RequestType"] == "Delete":
            logger.info(
                "In Delete "
            ) 

            #delete topic rules
            response = iot_client.delete_topic_rule(
                ruleName='IoTImlPumpDataRule'
            )
            response = iot_client.delete_topic_rule(
                ruleName='IoTImlPumpActionRule'
            )

            #delete detector
            response = iote_client.delete_detector_model(
                detectorModelName='IoTImlDetectorModel'
            )
            
            #delete inputs
            response = iote_client.delete_input(
                inputName='IoTImlPumpData'
            )      
            response = iote_client.delete_input(
                inputName='IoTImlPumpAction'
            )          
            response = iote_client.delete_input(
                inputName='IoTImlPumpInference'
            )  

            result = cfnresponse.SUCCESS   

    except ClientError as e:
        logger.error("Error: {}".format(e))

    logger.info(
        "Returning response of: "
    )

    sys.stdout.flush()
    cfnresponse.send(event, context, result, responseData)