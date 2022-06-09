import requests
import csv
import pandas as pd
import os
import bz2
from urllib import request
from urllib.error import HTTPError
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf


def download_dataset(start_block, end_block, directory, spark_session, debug=False):

    if 'blocks-{}-{}'.format(start_block, end_block) in os.listdir(directory):

        d_path = os.path.join(directory, 'blocks-{}-{}'.format(start_block, end_block))
        v_path = os.path.join(d_path, 'vertices-{}-{}.csv.bz2'.format(start_block, end_block))
        e_path = os.path.join(d_path, 'edges-{}-{}.csv.bz2'.format(start_block, end_block))
        a_path = os.path.join(d_path, 'addresses-{}-{}.csv.bz2'.format(start_block, end_block))
        if debug: print('dataset is already in {}'.format(d_path))

    else:

        d_path = os.path.join(directory, 'blocks-{}-{}'.format(start_block, end_block))
        os.mkdir(d_path)
        v_path = os.path.join(d_path, 'vertices-{}-{}.csv.bz2'.format(start_block, end_block))
        e_path = os.path.join(d_path, 'edges-{}-{}.csv.bz2'.format(start_block, end_block))
        a_path = os.path.join(d_path, 'addresses-{}-{}.csv.bz2'.format(start_block, end_block))

        try:
            request.urlretrieve('https://raw.github.com/VincenzoImp/Bitcoin-Address-Clustering/master/dataset/blocks-{}-{}/vertices-{}-{}.csv.bz2'.format(start_block, end_block, start_block, end_block), v_path)
            request.urlretrieve('https://raw.github.com/VincenzoImp/Bitcoin-Address-Clustering/master/dataset/blocks-{}-{}/edges-{}-{}.csv.bz2'.format(start_block, end_block, start_block, end_block), e_path)
            request.urlretrieve('https://raw.github.com/VincenzoImp/Bitcoin-Address-Clustering/master/dataset/blocks-{}-{}/addresses-{}-{}.csv.bz2'.format(start_block, end_block, start_block, end_block), a_path)
            if debug: print('dataset downloaded from https://github.com/VincenzoImp/Bitcoin-Address-Clustering/master/dataset/blocks-{}-{}'.format(start_block, end_block))

        except:

            if debug: print('dumping blocks...')

            v_columns = ['id', 'note', 'tx_hash', 'block_height', 'block_hash', 'fee', 'n_input', 'amount_input', 'n_output', 'amount_output']
            e_columns = ['src_id', 'dst_id', 'src_position', 'dst_position', 'address', 'value']

            v_schema = StructType([
                                StructField('id', StringType(), True),
                                StructField('note', StringType(), True),
                                StructField('tx_hash', StringType(), True),
                                StructField('block_height', IntegerType(), True),
                                StructField('block_hash', StringType(), True),
                                StructField('fee', IntegerType(), True),
                                StructField('n_input', IntegerType(), True),
                                StructField('amount_input', LongType(), True),
                                StructField('n_output', IntegerType(), True),
                                StructField('amount_output', LongType(), True)
                                ])
            e_schema = StructType([
                                StructField('src_id', StringType(), True),
                                StructField('dst_id', StringType(), True),
                                StructField('src_position', StringType(), True),
                                StructField('dst_position', IntegerType(), True),
                                StructField('address', StringType(), True),
                                StructField('value', IntegerType(), True)
                                ])

            v_df = spark_session.createDataFrame(spark_session.sparkContext.emptyRDD(),v_schema)
            e_df = spark_session.createDataFrame(spark_session.sparkContext.emptyRDD(),e_schema)

            count_UXTO = 0

            for block_height in range(start_block, end_block+1):

                if debug and block_height % 250 == 0:
                    if end_block < block_height+249:
                        print('from block {} to block {}'.format(block_height, end_block))
                    else:
                        print('from block {} to block {}'.format(block_height, block_height+249))

                block_reward = (5000000000 // 2**(block_height//210000))
                newRow = spark_session.createDataFrame([('coinbase'+str(block_height), 'coinbase', '', -1, '', 0, 0, 0, 1, block_reward)], v_columns)
                v_df = v_df.union(newRow)

                response = requests.get('https://blockchain.info/block-height/{}'.format(block_height))
                while(response.status_code != 200):
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

                        newRow = spark_session.createDataFrame([(str(src_id), str(dst_id), src_position, dst_position, address, value)], e_columns)
                        e_df = e_df.union(newRow)

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

                        newRow = spark_session.createDataFrame([(str(src_id), str(dst_id), src_position, dst_position, address, value)], e_columns)
                        e_df = e_df.union(newRow)

                        if dst_id == 'UTXO'+str(count_UXTO-1):
                            newRow = spark_session.createDataFrame([(str(dst_id), 'UTXO', '', -1, '', 0, 1, value, 0, 0)], v_columns)
                            v_df = v_df.union(newRow)

                    newRow = spark_session.createDataFrame([(str(tx_id), 'tx', tx_hash, block_height, block_hash, fee, n_input, amount_input, n_output, amount_output)], v_columns)
                    v_df = v_df.union(newRow)

            e_df.createOrReplaceTempView('EDGES')
            a_df = e_df.select('address').subtract(spark.sql("select address from EDGES where address like 'coinbase%'"))

            v_df.toPandas().to_csv(v_path, index=False, compression='bz2')
            e_df.toPandas().to_csv(e_path, index=False, compression='bz2')
            a_df.toPandas().to_csv(a_path, index=False, compression='bz2')

            if debug: print('dataset downloaded')

    return v_path, e_path, a_path, d_path



if __name__ == "__main__":
    start_block = 100000
    end_block = 100000
    dir = './dataset/'
    spark = SparkSession.builder.getOrCreate()
    download_dataset(start_block, end_block, dir, spark, True)
