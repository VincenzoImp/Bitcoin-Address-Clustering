import requests
import pprint

block_height = 100000

response = requests.get('https://blockchain.info/block-height/{}'.format(block_height))

pprint.PrettyPrinter().pprint(response.json())
