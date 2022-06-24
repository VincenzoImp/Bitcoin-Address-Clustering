import pandas as pd
import numpy as np
import pyspark
import requests
import csv
import os
import wget
from urllib import request
from pyvis.network import Network
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf

def download_dataset(start_block, end_block, directory, spark_session, debug=False):

    if 'blocks-{}-{}'.format(start_block, end_block) in os.listdir(directory):

        d_path = os.path.join(directory, 'blocks-{}-{}'.format(start_block, end_block))
        v_path = os.path.join(d_path, 'vertices-{}-{}'.format(start_block, end_block))
        e_path = os.path.join(d_path, 'edges-{}-{}'.format(start_block, end_block))
        a_path = os.path.join(d_path, 'addresses-{}-{}'.format(start_block, end_block))
        if debug: print('dataset is already in {}'.format(d_path))

    else:

        d_path = os.path.join(directory, 'blocks-{}-{}'.format(start_block, end_block))
        os.mkdir(d_path)

        try:
            v_path = os.path.join(d_path, 'vertices-{}-{}'.format(start_block, end_block))
            e_path = os.path.join(d_path, 'edges-{}-{}'.format(start_block, end_block))
            a_path = os.path.join(d_path, 'addresses-{}-{}'.format(start_block, end_block))
            wget.download('https://raw.github.com/VincenzoImp/Bitcoin-Address-Clustering/master/dataset/blocks-{}-{}/vertices-{}-{}.tar.gz'.format(start_block, end_block, start_block, end_block), out=d_path)
            wget.download('https://raw.github.com/VincenzoImp/Bitcoin-Address-Clustering/master/dataset/blocks-{}-{}/edges-{}-{}.tar.gz'.format(start_block, end_block, start_block, end_block), out=d_path)
            wget.download('https://raw.github.com/VincenzoImp/Bitcoin-Address-Clustering/master/dataset/blocks-{}-{}/addresses-{}-{}.tar.gz'.format(start_block, end_block, start_block, end_block), out=d_path)
            os.system('tar -xf '+v_path.replace(' ', '\ ')+'.tar.gz -C '+d_path.replace(' ', '\ '))
            os.system('tar -xf '+e_path.replace(' ', '\ ')+'.tar.gz -C '+d_path.replace(' ', '\ '))
            os.system('tar -xf '+a_path.replace(' ', '\ ')+'.tar.gz -C'+d_path.replace(' ', '\ '))
            os.remove(v_path+'.tar.gz')
            os.remove(e_path+'.tar.gz')
            os.remove(a_path+'.tar.gz')
            if debug: print('dataset downloaded from https://github.com/VincenzoImp/Bitcoin-Address-Clustering/master/dataset/blocks-{}-{}'.format(start_block, end_block))

        except:

            v_path = os.path.join(d_path, 'vertices-{}-{}.csv'.format(start_block, end_block))
            e_path = os.path.join(d_path, 'edges-{}-{}.csv'.format(start_block, end_block))

            if debug: print('dumping blocks...')

            v_columns = ['id', 'note', 'tx_hash', 'block_height', 'block_hash', 'fee', 'n_input', 'amount_input', 'n_output', 'amount_output', 'temporal_index']
            e_columns = ['src_id', 'dst_id', 'src_position', 'dst_position', 'address', 'value']


            temporal_index = 0
            count_UXTO = 0

            with open(v_path, 'w', encoding='UTF8') as v_file:
                with open(e_path, 'w', encoding='UTF8') as e_file:

                    csv.writer(v_file).writerow(v_columns)
                    csv.writer(e_file).writerow(e_columns)

                    for block_height in range(start_block, end_block+1):

                        if debug and block_height % 250 == 0:
                            if end_block < block_height+249:
                                print('from block {} to block {}'.format(block_height, end_block))
                            else:
                                print('from block {} to block {}'.format(block_height, block_height+249))

                        block_reward = (5000000000 // 2**(block_height//210000))
                        csv.writer(v_file).writerow(['coinbase'+str(block_height), 'coinbase', '', -1, '', 0, 0, 0, 1, block_reward, -1])

                        loop = True
                        while loop:
                            try:
                                response = requests.get('https://blockchain.info/block-height/{}'.format(block_height))
                                if response.status_code == 200: loop = False
                            except:
                                pass

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
                                    csv.writer(v_file).writerow([str(dst_id), 'UTXO', '', -1, '', 0, 1, value, 0, 0, -1])

                            csv.writer(v_file).writerow([str(tx_id), 'tx', tx_hash, block_height, block_hash, fee, n_input, amount_input, n_output, amount_output, temporal_index])
                            temporal_index += 1

            v_df = spark_session.read.load(v_path,
                                    format="csv",
                                    sep=",",
                                    inferSchema="true",
                                    header="true"
                                    ).distinct()
            e_df = spark_session.read.load(e_path,
                                    format="csv",
                                    sep=",",
                                    inferSchema="true",
                                    header="true"
                                    ).distinct()

            e_df.createOrReplaceTempView('EDGES')
            a_df = e_df.select('address').subtract(spark_session.sql("select address from EDGES where address like 'coinbase%'"))
            a_df = a_df.withColumn('cluster_id', monotonically_increasing_id())

            v_path = os.path.join(d_path, 'vertices-{}-{}'.format(start_block, end_block))
            e_path = os.path.join(d_path, 'edges-{}-{}'.format(start_block, end_block))
            a_path = os.path.join(d_path, 'addresses-{}-{}'.format(start_block, end_block))

            v_df.write.save(path=v_path, format='csv', header='True')
            e_df.write.save(path=e_path, format='csv', header='True')
            a_df.write.save(path=a_path, format='csv', header='True')

            os.remove(os.path.join(d_path, 'vertices-{}-{}.csv'.format(start_block, end_block)))
            os.remove(os.path.join(d_path, 'edges-{}-{}.csv'.format(start_block, end_block)))

            if debug: print('dataset downloaded')

    return v_path, e_path, a_path, d_path


if __name__ == "__main__":
    start_block = 0
    end_block = 120000
    dir = './dataset/'
    spark = SparkSession.builder \
    .master("local[*]")\
    .config('spark.executor.memory', '500G')\
    .config("spark.driver.memory","500G")\
    .config("spark.driver.cores", "10")\
    .config("spark.driver.maxResultSize", "500G") \
    .config("spark.kryoserializer.buffer.max", "2000M")\
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")\
    .getOrCreate()
    sc = spark.sparkContext
    download_dataset(start_block, end_block, dir, spark, True)
