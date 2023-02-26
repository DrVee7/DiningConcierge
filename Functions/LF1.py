import json
import os
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
REGION = 'us-east-1'
HOST = 'search-restaurants-dnvw5rl5rn4kfv67d3kevs3lq4.us-east-1.es.amazonaws.com'
INDEX = 'restaurants'
def lambda_handler(event, context):
    print('Received event: ' + json.dumps(event))
    #msg_from_user = event['messages'][0]['unstructured']['text']
    results = query(msg_from_user)
    restaurant_id="abcd"
    cusine=msg_from_user
    client = boto3.client('sqs')
    message = client.send_message(
        QueueUrl='https://sqs.us-east-1.amazonaws.com/703197594369/bot',
        MessageBody=("Restaurant ID = " + restaurant_id + " and cusine = " + cusine)
        )
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*',
        },
        'body': json.dumps({'results': results})
    }
def query(term):
    q = {'size': 5, 'query': {'multi_match': {'query': term}}}
    client = OpenSearch(hosts=[{
        'host': HOST,
        'port': 443
    }],
                        http_auth=get_awsauth(REGION, 'es'),
                        use_ssl=True,
                        verify_certs=True,
                        connection_class=RequestsHttpConnection)
    res = client.search(index=INDEX, body=q)
    print(res)
    hits = res['hits']['hits']
    results = []
    for hit in hits:
        results.append(hit['_source'])
    return results
def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)