import boto3
import json
import requests
from requests_aws4auth import AWS4Auth
from requests.auth import HTTPBasicAuth


region = 'us-east-1'  # For example, us-west-1
service = 'opensearchservice'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, 'es', session_token=credentials.token)

host = 'https://search-restaurant-jqknbds6jnertlkpyivzt4gjdu.us-east-1.es.amazonaws.com'  # The OpenSearch domain endpoint with https://
index = 'restaurants'
url = host + '/' + index + '/_search'
headers = {"Content-Type": "application/json"}

# Lambda execution starts here
def find_cuisine_ids(cuisine):
    query = {
        "query": {
            "match": {
                #  "year":"1998"
                "_id": cuisine
            }
        }
    }
    headers = {"Content-Type": "application/json"}

    # Make the signed HTTP request
    r = requests.get(url, headers=headers, data=json.dumps(query), auth=HTTPBasicAuth('hanfushi', '4a5s6d4A5S6D#'))
    return r.json()

# response = find_cuisine_ids('Korean')
# print(response['hits']['hits'][0]['_source']['ids'])

