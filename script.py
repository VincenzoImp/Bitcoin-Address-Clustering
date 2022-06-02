import requests
import csv

start_block = 200000
end_block = 200009

columns = ['block_height', 'block_hash', 'tx_index', 'tx_hash', 'fee', 'n_input', 'amount_input', 'src_txs_indices', 'src_positions', 'input_positions', 'input_addresses', 'values_input_addresses', 'n_output', 'amount_output', 'dst_txs_indices', 'dst_positions', 'output_positions', 'output_addresses', 'values_output_addresses']

with open('transactions-{}-{}.csv'.format(start_block, end_block), 'w', encoding='UTF8') as txs_file:
    csv.writer(txs_file).writerow(columns)
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
            src_txs_indices = []
            src_positions = []
            input_positions = []
            input_addresses = []
            values_input_addresses = []
            for incoming_edge in tx['inputs']:
                src_tx_index = incoming_edge['prev_out']['tx_index']
                src_position = incoming_edge['prev_out']['n']
                input_position = incoming_edge['index']
                if src_tx_index == 0:
                    addr = 'coinbase'
                else:
                    try:
                        addr = incoming_edge['prev_out']['addr']
                    except KeyError:
                        addr = 'special' + str(count_special)
                        count_special += 1
                value = incoming_edge['prev_out']['value']
                n_input += 1
                amount_input += value
                src_txs_indices.append(src_tx_index)
                src_positions.append(src_position)
                input_positions.append(input_position)
                input_addresses.append(addr)
                values_input_addresses.append(value)
            n_output = 0
            amount_output = 0
            dst_txs_indices = []
            dst_positions = []
            output_positions = []
            output_addresses = []
            values_output_addresses = []
            for outgoing_edge in tx['out']:
                if outgoing_edge['spending_outpoints'] == []:
                    dst_tx_index = -1 #'unspent'
                    dst_position = -1 #'unspent'
                else:
                    dst_tx_index = outgoing_edge['spending_outpoints'][0]['tx_index']
                    dst_position = outgoing_edge['spending_outpoints'][0]['n']
                output_position = outgoing_edge['n']
                try:
                    addr = outgoing_edge['addr']
                except KeyError:
                    addr = 'special' + str(count_special)
                    count_special += 1
                value = outgoing_edge['value']
                amount_output += value
                n_output += 1
                dst_txs_indices.append(dst_tx_index)
                dst_positions.append(dst_position)
                output_positions.append(output_position)
                output_addresses.append(addr)
                values_output_addresses.append(value)
            csv.writer(txs_file).writerow([block_height, block_hash, tx_index, tx_hash, fee, n_input, amount_input, src_txs_indices, src_positions, input_positions, input_addresses, values_input_addresses, n_output, amount_output, dst_txs_indices, dst_positions, output_positions, output_addresses, values_output_addresses])
