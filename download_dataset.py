import requests
import csv
import pandas as pd
import os
from urllib.error import HTTPError


def download_dataset(start_block, end_block, directory, debug=False):

    try:
        os.mkdir(directory)
    except FileExistsError:
        pass

    v_path = os.path.join(directory, 'vertices-{}-{}.csv'.format(start_block, end_block))
    e_path = os.path.join(directory, 'edges-{}-{}.csv'.format(start_block, end_block))

    if 'vertices-{}-{}.csv'.format(start_block, end_block) in os.listdir(directory) and 'edges-{}-{}.csv'.format(start_block, end_block) in os.listdir(directory):
        if debug: print('dataset is already in {}'.format(directory))
        return
    try:
        url = 'https://raw.githubusercontent.com/VincenzoImp/Bitcoin-Address-Clustering/master/dataset/'
        e_df = pd.read_csv(url + 'edges-{}-{}.csv'.format(start_block, end_block))
        v_df = pd.read_csv(url + 'vertices-{}-{}.csv'.format(start_block, end_block))
        e_df.to_csv(e_path, index=False)
        v_df.to_csv(v_path, index=False)
        if debug: print('dataset downloaded from https://github.com/VincenzoImp/Bitcoin-Address-Clustering/master/dataset/')
        return
    except HTTPError:
        pass

    if debug: print('dumping blocks...')

    v_columns = ['id', 'note', 'tx_hash', 'block_height', 'block_hash', 'fee', 'n_input', 'amount_input', 'n_output', 'amount_output']
    e_columns = ['src_id', 'dst_id', 'src_position', 'dst_position', 'address', 'value']

    with open(v_path, 'w', encoding='UTF8') as v_file:
        with open(e_path, 'w', encoding='UTF8') as e_file:

            csv.writer(v_file).writerow(v_columns)
            csv.writer(e_file).writerow(e_columns)

            count_UXTO = 0

            for block_height in range(start_block, end_block+1):

                if debug and block_height % 250 == 0:
                    if end_block < block_height+249:
                        print('from block {} to block {}'.format(block_height, end_block))
                    else:
                        print('from block {} to block {}'.format(block_height, block_height+249))

                block_reward = (5000000000 // 2**(block_height//210000))
                csv.writer(v_file).writerow(['coinbase'+str(block_height), 'coinbase', '', -1, '', 0, 0, 0, 1, block_reward])

                response = requests.get('https://blockchain.info/block-height/{}'.format(block_height))
                while(response.ok == False):
                    response = requests.get('https://blockchain.info/block-height/{}'.format(block_height))

                block_hash = response.json()['blocks'][0]['hash']

                for tx in response.json()['blocks'][0]['tx']:

                    tx_id = tx['tx_index']
                    tx_hash = tx['hash']
                    fee = tx['fee']
                    n_input = 0
                    amount_input = 0
                    n_output = 0
                    amount_output = 0

                    for incoming_edge in tx['inputs']:

                        src_id = incoming_edge['prev_out']['tx_index']
                        src_position = incoming_edge['prev_out']['n']

                        dst_id = tx_id
                        dst_position = incoming_edge['index']

                        if src_id == 0:
                            src_id = 'coinbase' + str(block_height)
                            address = 'coinbase' + str(block_height)
                            value = block_reward
                        else:
                            try:
                                address = incoming_edge['prev_out']['addr']
                            except KeyError:
                                continue
                            value = incoming_edge['prev_out']['value']

                        n_input += 1
                        amount_input += value

                        csv.writer(e_file).writerow([str(src_id), str(dst_id), src_position, dst_position, address, value])

                    for outgoing_edge in tx['out']:

                        src_id = tx_id
                        src_position = outgoing_edge['n']

                        if outgoing_edge['spending_outpoints'] == []:
                            dst_id = 'UTXO'+str(count_UXTO)
                            count_UXTO += 1
                            dst_position = -1
                        else:
                            dst_id = outgoing_edge['spending_outpoints'][0]['tx_index']
                            dst_position = outgoing_edge['spending_outpoints'][0]['n']

                        try:
                            address = outgoing_edge['addr']
                        except KeyError:
                            continue

                        value = outgoing_edge['value']

                        n_output += 1
                        amount_output += value

                        csv.writer(e_file).writerow([str(src_id), str(dst_id), src_position, dst_position, address, value])

                        if dst_id == 'UTXO'+str(count_UXTO-1):
                            csv.writer(v_file).writerow([str(dst_id), 'UTXO', '', -1, '', 0, 1, value, 0, 0])

                    csv.writer(v_file).writerow([str(tx_id), 'tx', tx_hash, block_height, block_hash, fee, n_input, amount_input, n_output, amount_output])

    if debug: print('dataset downloaded')

    return


if __name__ == "__main__":
    start_block = 100000
    end_block = 150000
    dir = './dataset/'
    download_dataset(start_block, end_block, dir, True)
