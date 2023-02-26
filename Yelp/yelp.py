import json
from datetime import datetime
from urllib.parse import urlencode
import boto3
from decimal import Decimal
import time

from urllib.request import urlopen, Request


def insert_data(item, db=None, table='yelp-restaurants'):
    if not db:
        db = boto3.resource('dynamodb')

    table = db.Table(table)

    # overwrite if the same index is provided
    response = table.put_item(Item=item)
    print('@insert_data: response', response)
    return response


# add more restaurants term here
terms = ['chinese restaurants', 'italian restaurants', 'french restaurants', 'cuban restaurants', 'korean restaurants']
terms = ['chinese restaurants']

# use your own api key by signing up at yelp and creating a new app
# step 1: signup to yelp / create new account
# step 2: goto https://www.yelp.com/developers/v3/manage_app and create new app
# step 3: paste your api key here
api_key = 'vJl_2-7ouCTMBF-_nk6XpxSpaDVz2xHdwpZehGb_y6pirkgeJtTFUErAW6OsEP3VgwO45fqN2XIrxnh1g4GMc-UuRmZ_1JwNc27K_4g5lThckGyMoFcf0a5UI8f1Y3Yx'

headers = {
    'accept': 'application/json',
    'Authorization': f'Bearer {api_key}',
}

for term in terms:
    offset = 0

    # since each category needs at least 1000 businesses
    while offset <= 1000:
        query = {
            'location': 'Manhattan, NY',
            'term': term,
            'sort_by': 'best_match',
            'limit': 50,
            'offset': offset,
        }

        url = 'https://api.yelp.com/v3/businesses/search?' + urlencode(query)
        req = Request(url, headers=headers)
        response = urlopen(req)
        content = response.read().decode('utf8')
        data = json.loads(content)
        # response = requests.get(url, headers=headers)
        # data = response.json()
        businesses = data.get('businesses', [])

        business: dict
        for business in businesses:
            item = business.copy()

            # adding extra fields
            item['term'] = term
            item['insertedAtTimestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            item = json.loads(json.dumps(item), parse_float=Decimal)

            # insert into dynamo db here
            insert_data(item)

        offset += 50
        if len(businesses) == 0:
            break
        time.sleep(2)