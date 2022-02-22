
"""


Sample usage of the program:
`python sample.py --term="bars" --location="San Francisco, CA"`
"""
from __future__ import print_function


import requests
import sys

import datetime
import boto3


try:
    # For Python 3.0 and later
    from urllib.error import HTTPError
    from urllib.parse import quote
    from urllib.parse import urlencode
except ImportError:
    # Fall back to Python 2's urllib2 and urllib
    # from urllib2 import HTTPError
    from urllib import quote
    from urllib import urlencode


API_KEY = "EkFLnw2isyDf5SiRGwIIOKZctXODnWIYW8Zz1kqVgVYJgscyBHc56yv2otuo4ERWjahmygKHWImKkCSTK5Tvn6becDYEc9pkhDbvJVxFQm3MZznkK-lmncUJiYoFYnYx"
DEFAULT_TERM = ['Bars restaurants', 'Chinese restaurants', 'Chinese restaurants', 'American restaurants', 'Burgers', 'Seafood',
                'Korean restaurants', 'Japanese restaurants', 'Asian Fusion',
                'Mexican']
DEFAULT_LOCATION = 'Manhattan'

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('yelp-restaurants')

def request(term):
    headers = {'Authorization': 'Bearer %s' % API_KEY}
    url = 'https://api.yelp.com/v3/businesses/search'

    data = []

    for offset in range(0, 1000, 50):
        params = {
            'limit': 50,
            'location': DEFAULT_LOCATION.replace(' ', '+'),
            'term': term.replace(' ', '+'),
            'offset': offset
        }
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data += response.json()['businesses']

        elif response.status_code == 400:
            print('400 Bad Request')
            break
    return data



def query_api(term):

    response = request(term)

    if not response:
        print(u'No businesses for {0} in {1} found.'.format(term, DEFAULT_LOCATION))
        return
    for i in response:
        thisItem = {'id': i["id"], "name": i["name"], 'review_count': i['review_count'],
                    "cuisine": i["categories"][0]["title"], "rating": str(i['rating']), "coordinates": {"latitude":
            str(i['coordinates']["latitude"]), "longitude": str(i['coordinates']["longitude"])},
                    "location": i['location']["address1"], "zip_code": i['location']["zip_code"],
                    "insertedAtTimestamp": str(datetime.datetime.now())
                    }

        with table.batch_writer() as batch:
            batch.put_item(Item=thisItem)
        # print(thisItem)

def main():
    for term in DEFAULT_TERM:
        try:
            query_api(term)
        except HTTPError as error:
            sys.exit(
                'Encountered HTTP error {0} on {1}:\n {2}\nAbort program.'.format(
                    error.code,
                    error.url,
                    error.read(),
                )
            )


if __name__ == '__main__':
    main()
