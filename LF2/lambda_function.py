import json
import boto3
from boto3.dynamodb.conditions import Key
from SNS import SnsWrapper
import logging
from botocore.exceptions import ClientError
from opensearch_query import find_cuisine_ids
from dynamoDB_query import dynamoDB_search
import requests

AWS_REGION = "us-east-1"
logger = logging.getLogger(__name__)
sns_resource = boto3.resource("sns", region_name=AWS_REGION)

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('yelp-restaurants')


def lambda_handler(event, context):
    sqs_response = sqs()
    if "Messages" not in sqs_response.keys():
        return "Please try later"

    message = sqs_response["Messages"][0]
    msg = message["MessageAttributes"]
    cuisine = msg["Cuisine"]["StringValue"]
    print(cuisine)

    number_of_people = msg["NumberOfPeople"]["StringValue"]
    date = msg["Date"]["StringValue"]
    time = msg["Time"]["StringValue"]
    phoneNumber = msg["PhoneNumber"]["StringValue"]
    email = msg["Email"]["StringValue"]

    # query opensearch
    # response = find_cuisine_ids('Seafood')

    response = find_cuisine_ids(cuisine)
    chosendID = response['hits']['hits'][0]['_source']['ids'][0]

    # query the dynamoDB
    resp = table.query(KeyConditionExpression=Key('cuisine').eq(cuisine) & Key('id').eq(chosendID))

    # send the sms message to user's phone
    testSMS = SnsWrapper(sns_resource)
    message = formatMessage(resp['Items'], cuisine, date, time, number_of_people)

    testSMS.publish_text_message("+1" + phoneNumber, message)

    # store suggestion into database
    item = {"phone": phoneNumber, "message": message}
    storeRecommendation(item)

    delete_sqs(sqs_response)

    return {
        'dynamoDBmessage': resp['Items'],
        'item': message,
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


# Receive message from queue
def sqs():
    queue_url = "https://sqs.us-east-1.amazonaws.com/419065928887/Q1"
    sqs = boto3.client('sqs')
    # recived message from sqs queue
    sqs_response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=['SentTimestamp'],
        MaxNumberOfMessages=1,
        MessageAttributeNames=['All'],
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )
    return sqs_response


# Delete received message from queue
def delete_sqs(sqs_response):
    queue_url = "https://sqs.us-east-1.amazonaws.com/419065928887/Q1"
    sqs = boto3.client('sqs')

    message = sqs_response['Messages'][0]
    receipt_handle = message['ReceiptHandle']

    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)


def formatMessage(items, cuisine, date, time, number):
    message = "Hello! Here are my " + cuisine + " restaurant suggestion for " + number + " people at " + time + "," + date + " :" + "\n"
    for item in items:
        message += str(item['name']) + " locate at " + str(item['location']) + "\n"

    return message


def storeRecommendation(item):
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

    table = dynamodb.Table('StoredRecommendation')
    with table.batch_writer() as batch:
        batch.put_item(Item=item)