import boto3
import json
from boto3.dynamodb.conditions import Key, Attr


dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('yelp-restaurants')

Items = table.scan()
idAndCusine = {}

for item in Items['Items']:
    if item['cuisine'] not in idAndCusine:
        idAndCusine[item['cuisine']] = []
    idAndCusine[item['cuisine']].append(item['id'])



with open('bulk_restaurants.json',"w") as f:
    for k, v in idAndCusine.items():
        newIndex = str({"index": {"_index": "restaurants", "_id": k}})+"\n"
        newIndex = newIndex.replace("'", "\"")
        newrecord =str({"ids": v})+"\n"
        newrecord = newrecord.replace("'", "\"")
        # json.dump(newIndex,f,indent=1)
        # json.dump(newrecord, f, indent=1)
        f.write(newIndex)
        f.write(newrecord)

#code to upload the file to opensearch:
#curl -XPOST -u 'hanfushi:4a5s6d4A5S6D#' 'https://search-restaurant-jqknbds6jnertlkpyivzt4gjdu.us-east-1.es.amazonaws.com/_bulk' --data-binary @bulk_restaurants.json -H 'Content-Type: application/json'

