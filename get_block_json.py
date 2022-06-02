import requests
import pprint

block_height = 0

response = requests.get('https://blockchain.info/block-height/{}'.format(block_height))

pprint.PrettyPrinter().pprint(response.json())
