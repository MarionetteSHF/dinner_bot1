import logging
import boto3
from botocore.exceptions import ClientError
logger = logging.getLogger(__name__)

AWS_REGION = "us-east-1"
sns_resource = boto3.resource("sns", region_name=AWS_REGION)


class SnsWrapper:
    """Encapsulates Amazon SNS topic and subscription functions."""
    def __init__(self, sns_resource):
        """
        :param sns_resource: A Boto3 Amazon SNS resource.
        """
        self.sns_resource = sns_resource

    def publish_text_message(self, phone_number, message):


        try:
            response = self.sns_resource.meta.client.publish(
                PhoneNumber=phone_number, Message=message)
            message_id = response['MessageId']
            logger.info("Published message to %s.", phone_number)
        except ClientError:
            logger.exception("Couldn't publish message to %s.", phone_number)
            raise
        else:
            return message_id

# testSMS = SnsWrapper(sns_resource)
# testSMS.publish_text_message("+16462364476", 'hello hanfu')