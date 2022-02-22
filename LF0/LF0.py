import json
import boto3
import time
from boto3.dynamodb.conditions import Key


# lambda0 is used to receive message from Lex and send back to front end

def lambda_handler(event, context):
    # text: message from front end
    text = get_request(event)
    # error handler
    if text is None:
        return response("Error: Failed to get text from request.")

    # check if already have suggestion stored in data base
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('StoredRecommendation')
    resp = table.query(KeyConditionExpression=Key('phone').eq(text))
    if resp['Items']:
        return response("this is suggestion for last time:" + resp['Items'][0]['message'])

    # get chatbot response
    ChatBotText = get_chatbot_response(text)
    if ChatBotText is None:
        return response("Error: Failed to connect with Lex.")
    else:
        return response(ChatBotText)


def get_request(event):
    # corner cases handle
    print(event)
    if "messages" not in event:
        return None
    messages = event["messages"]

    if not isinstance(messages, list) or len(messages) == 0:
        return None
    message = messages[0]

    if "unstructured" not in message or "text" not in message["unstructured"]:
        return None

    return message["unstructured"]["text"]


def response(text):
    response = {
        "status code": 200,
        "messages": [
            {
                "type": "unstructured",
                "unstructured": {
                    "uid": "123abc",
                    "text": text,
                    "time": time.time()
                }
            }]
    }
    return response


def get_chatbot_response(text):
    message = ''
    client = boto3.client('lex-runtime')
    # pass message to Lex
    LexResponse = client.post_text(
        botName='TestBot',
        botAlias='$LATEST',
        userId="123abc",
        inputText=text
    )

    if not isinstance(LexResponse, dict):
        return None

    if 'message' not in LexResponse:
        return None
    # get response from Lex
    return LexResponse['message']