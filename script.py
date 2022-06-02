import pandas as pd
import csv

start_block = 200000
end_block = 200009

dataset = 'transactions-{}-{}.csv'.format(start_block, end_block)

df = pd.read_csv(dataset)

v_columns = ['block_height', 'block_hash', 'tx_index', 'tx_hash', 'fee', 'n_input', 'amount_input', 'n_output', 'amount_output']
e_columns = ['src_tx_index', 'src_position', 'input_address', 'value_input_address', 'dst_tx_index', 'dst_position']

v = df[v_columns]
e = pd.DataFrame(columns=e_columns)

with open('vertices-{}-{}.csv'.format(start_block, end_block), 'w', encoding='UTF8') as v_file:
    with open('edges-{}-{}.csv'.format(start_block, end_block), 'w', encoding='UTF8') as e_file:

        csv.writer(v_file).writerow(v_columns)
        csv.writer(e_file).writerow(e_columns)

        for _, row in df.iterrows():
            for x in row['src_txs_indices']:
                print(x)
            for src_tx_index, src_position, input_position, input_address, value_input_address in zip(row['src_txs_indices'], row['src_positions'], row['input_positions'], row['input_addresses'], row['values_input_addresses']):
                if input_address != 'coinbase':
                    if v[v['tx_index'] == src_tx_index].shape[0] == 0:
                        csv.writer(v_file).writerow([-1, 'unknown', src_tx_index, 'unknown', -1, -1, -1, -1, -1])
                    csv.writer(e_file).writerow([src_tx_index, src_position, input_address, value_input_address, row['tx_index'], input_position])

            for dst_tx_index, dst_position, output_position, output_address, value_output_address in zip(row['dst_txs_indices'], row['dst_positions'], row['output_positions'], row['output_addresses'], row['values_output_addresses']):
                if dst_tx_index != -1:
                    if v[v['tx_index'] == dst_tx_index].shape[0] == 0:
                        csv.writer(v_file).writerow([-1, 'unknown', dst_tx_index, 'unknown', -1, -1, -1, -1, -1])
                    csv.writer(e_file).writerow([row['tx_index'], output_position, output_address, value_output_address, dst_tx_index, dst_position])
