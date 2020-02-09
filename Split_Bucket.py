import boto3
import uuid
from itertools import compress

s3_client = boto3.client('s3',
                aws_access_key_id='',
                aws_secret_access_key='', 
                region_name='us-east-2'
                )

def lambda_handler(event, context):
    for record in event['Records']:
        # Setting names
        # bucket = '2020-primary-election'
        # key = '2020/02/08/21/2020-primary-election-1-2020-02-08-21-26-25-681b95b6-19aa-4f05-92f8-69cf5b2aa542'
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        tmpkey = key.replace('/', '')
        # download_path = 'd:\\bonta\\Desktop\\{}'.format(tmpkey)
        download_path = '/tmp/{}{}'.format(uuid.uuid4(), tmpkey)
        
        # Reading JSON file
        s3_client.download_file(bucket, key, download_path)
        file = open(download_path, 'r')
        file_content = file.read()
        data = ['{' + x + '}' for x in file_content[1:-1].split('}{')]
        
        # Spliting the JSON objects
        keywords_group1 = ['2020-primary-election-group1',['provisional ballot', 'voting machine', 'ballot']]
        keywords_group2 = ['2020-primary-election-group2',['election fraud', 'election manipulation', 'illegal voters',
                           'illegal votes', 'dead voters', 'noncitizen voting',
                           'noncitizen votes', 'illegal voting', 'illegal vote',
                           'illegal ballot', 'illegal ballots', 'dirty voter rolls',
                           'vote illegally', 'voting illegally', 'voter intimidation',
                           'voter suppression', 'rigged election', 'vote rigging',
                           'voter fraud', 'voting fraud', 'vote buying', 'vote flipping',
                           'flipped votes', 'voter coercion', 'ballot stuffing',
                           'ballot box stuffing', 'ballot destruction', 'voting machine tampering',
                           'rigged voting machines', 'voter impersonation', 'election integrity',
                           'election rigging', 'duplicate voting', 'duplicate vote',
                           'ineligible voting', 'ineligible vote']]
        keywords_group3 = ['2020-primary-election-group3',['absentee ballot', 'mail ballot', 'vote by mail',
                           'voting by mail', 'early voting']]
        keywords_group4 = ['2020-primary-election-group4',['voter identification', 'voting identification', 'voter id']]
        keywords_group5 = ['2020-primary-election-group5',['polling place line', 'precinct line', 'pollworker', 'poll worker']]
        keywords_group6 = ['2020-primary-election-group6',['@OCRegistrar', '#ocvote2020', '#ocvotecenters2020', '#protect2020',
                           '#OrangeCounty', '#OCVotes', '@LACountyRRCC', '@lacountyrrcc',
                           '#VSAP', '#LAVotes', '#LACounty', '@CASOSvote',
                           '#CAPrimary', '#VoteCalifornia']]
        keywords_group7 = ['2020-primary-election-group7',['#SuperTuesday']]
        keywords = [keywords_group1,keywords_group2,keywords_group3,keywords_group4,keywords_group5,keywords_group6,keywords_group7]
        
        data_group = list()
        for group in keywords:
            data_group.append([group[0],[data[i] for i in list(compress(range(len(data)), [any([x in y for x in group[1]]) for y in data]))]])

        for item in data_group:
            path = '/tmp/{}{}'.format(uuid.uuid4(), item[0])
            file = open(path, 'w')
            file.write(''.join(item[1]))
            s3_client.upload_file(path, item[0], key[14:]+item[0][16:])
            file.close()
