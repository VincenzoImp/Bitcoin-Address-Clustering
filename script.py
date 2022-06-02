import requests
import csv
import pandas as pd 

start_block = 100000
end_block = 105000

v_columns = ['id', 'name', 'block_height', 'block_hash', 'fee', 'n_input', 'amount_input', 'n_output', 'amount_output']
e_columns = ['src', 'dst', 'src_position', 'dst_position', 'address', 'value']

with open('vertices-{}-{}.csv'.format(start_block, end_block), 'w', encoding='UTF8') as v_file:
    with open('edges-{}-{}.csv'.format(start_block, end_block), 'w', encoding='UTF8') as e_file:

        csv.writer(v_file).writerow(v_columns)
        csv.writer(e_file).writerow(e_columns)

        count_special = 0

        for block_height in range(start_block, end_block+1):

            response = requests.get('https://blockchain.info/block-height/{}'.format(block_height))
            block_hash = response.json()['blocks'][0]['hash']

            for tx in response.json()['blocks'][0]['tx']:

                tx_index = tx['tx_index']
                tx_hash = tx['hash']
                fee = tx['fee']
                n_input = 0
                amount_input = 0
                n_output = 0
                amount_output = 0

                for incoming_edge in tx['inputs']:

                    src_tx_index = incoming_edge['prev_out']['tx_index']
                    src_position = incoming_edge['prev_out']['n']
                    if src_tx_index == 0:
                        address = 'coinbase'
                    else:
                        try:
                            address = incoming_edge['prev_out']['addr']
                        except KeyError:
                            address = 'special' + str(count_special)
                            count_special += 1
                    value = incoming_edge['prev_out']['value']
                    dst_index = tx_index
                    dst_position = incoming_edge['index']
                    n_input += 1
                    amount_input += value

                    if src_tx_index != 0:
                        csv.writer(e_file).writerow([src_tx_index, tx_index, src_position, dst_position, address, value])

                for outgoing_edge in tx['out']:
                    
                    src_tx_index = tx_index
                    src_position = outgoing_edge['n']
                    try:
                        address = outgoing_edge['addr']
                    except KeyError:
                        address = 'special' + str(count_special)
                        count_special += 1
                    value = outgoing_edge['value']
                    if outgoing_edge['spending_outpoints'] == []:
                        dst_tx_index = -1 #'unspent'
                        dst_position = -1 #'unspent'
                    else:
                        dst_tx_index = outgoing_edge['spending_outpoints'][0]['tx_index']
                        dst_position = outgoing_edge['spending_outpoints'][0]['n']
                    n_output += 1
                    amount_output += value

                    if dst_tx_index != -1:
                        csv.writer(e_file).writerow([src_tx_index, tx_index, src_position, dst_position, address, value])

                csv.writer(v_file).writerow([tx_index, tx_hash, block_height, block_hash, fee, n_input, amount_input, n_output, amount_output])

v_df = pd.read_csv('vertices-{}-{}.csv'.format(start_block, end_block))
e_df = pd.read_csv('edges-{}-{}.csv'.format(start_block, end_block))

new_txs_indices = pd.concat([e_df['src'], e_df['dst'], v_df['id'], v_df['id']]).drop_duplicates(keep=False)

with open('vertices-{}-{}.csv'.format(start_block, end_block), 'a', encoding='UTF8') as v_file:

    for i, tx_index in new_txs_indices.iteritems():
        csv.writer(v_file).writerow([tx_index, 'unknown', -1, 'unknown', -1, -1, -1, -1, -1])

