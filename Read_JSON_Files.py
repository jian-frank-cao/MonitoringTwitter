import json

filename = ''

# Opening JSON file
file = open(filename, 'r')
file_content = file.read().decode('utf-8')

# Reading JSON file obtained from S3 bucket
data = json.loads('['+file_content.replace('}{','},{')+']')

# Reading JSON file obtained from Twitter REST API
# data = json.loads('['+file_content.replace('}\n{','},{')+']')