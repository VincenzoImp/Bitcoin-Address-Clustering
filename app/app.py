
print('installing libraries')

import subprocess
import sys
try:
    import pyspark
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", 'pyspark'])
finally:
    import pyspark
try:
    import pyvis
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", 'pyvis'])
finally:
    import pyvis
try:
    import streamlit
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", 'streamlit'])
finally:
    import streamlit
try:
    import traitlets
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", 'traitlets'])
finally:
    import traitlets
try:
    import tornado
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", 'tornado'])
finally:
    import tornado
import pandas as pd
import numpy as np
import requests
import csv
import os
import networkx as nx
import pprint
import codecs
import streamlit as st
import streamlit.components.v1 as components
from urllib.error import HTTPError
from pyvis.network import Network
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf

print('all libraries are imported')


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


def generate_nx_graph(v_df, e_df):

    G = nx.MultiDiGraph()

    for _, node in v_df.toPandas().iterrows():
        if node.note == 'tx':
            G.add_node(
                node_for_adding=str(node.id),
                note=node.note,
                tx_hash=node.tx_hash,
                block_height=node.block_height,
                fee=node.fee/100000000,
                n_input=node.n_input,
                amount_input=node.amount_input/100000000,
                n_output=node.n_output,
                amount_output=node.amount_output/100000000
                )
        elif node.note == 'UTXO':
            G.add_node(
                node_for_adding=str(node.id),
                note=node.note,
                amount=node.amount_input/100000000
            )
        elif node.note == 'coinbase':
            G.add_node(
                node_for_adding=str(node.id),
                note=node.note,
                amount=node.amount_output/100000000
            )
        elif node.note == 'unknown_tx':
            G.add_node(
                node_for_adding=str(node.id),
                note=node.note
            )

    for i, edge in e_df.toPandas().iterrows():
        G.add_edge(
            u_for_edge=str(edge.src_id),
            v_for_edge=str(edge.dst_id),
            key=None,
            address=edge.address,
            value=edge.value/100000000,
        )

    return G


def generate_pyvis_graph(v_df, e_df):

    net = Network(
                height='100%',
                width='100%',
                directed=True
                )
    net.repulsion(
                node_distance=420,
                central_gravity=0.33,
                spring_length=110,
                spring_strength=0.10,
                damping=0.95
                )

    for _, node in v_df.toPandas().iterrows():
        if node.note == 'tx':
            title = 'tx_hash: {}\n'.format(node.tx_hash) + \
                'block_height: {}\n'.format(node.block_height) + \
                'n_input: {}\n'.format(node.n_input) + \
                'n_output: {}\n'.format(node.n_output) + \
                'amount_input: {} BTC\n'.format(node.amount_input/100000000) + \
                'amount_output: {} BTC\n'.format(node.amount_output/100000000) + \
                'fee: {} BTC'.format(node.fee/100000000)
            net.add_node(n_id=str(node.id), shape='dot', title=title)
        elif node.note == 'UTXO':
            title = '{}\n'.format(node.note) + \
                'amount: {} BTC'.format(node.amount_input/100000000)
            net.add_node(n_id=str(node.id), shape='square', title=title)
        elif node.note == 'coinbase':
            title = '{}\n'.format(node.note) + \
                'amount: {} BTC'.format(node.amount_output/100000000)
            net.add_node(n_id=str(node.id), shape='square', title=title)
        elif node.note == 'unknown_tx':
            title = 'unknown_tx\n' + 'tx_index: {}'.format(node.id)
            net.add_node(n_id=str(node.id), shape='square', title=title)

    for _, edge in e_df.toPandas().iterrows():
        title = 'address: {}\n'.format(edge.address) + \
                'value: {} BTC'.format(edge.value/100000000)
        net.add_edge(source=str(edge.src_id), to=str(edge.dst_id), title=title, value=edge.value/100000000)

    return net


def address_clustering(G, address, v_tx_df):
  ClusterList = {
    '15vScfMHNrXN4QvWe54q5hwfVoYwG79CS1',
    '1H8ANdafjpqYntniT3Ddxh4xPBMCSz33pj',
    '16FuTPaeRSPVxxCnwQmdyx2PQWxX6HWzhQ',
    '1HWqMzw1jfpXb3xyuUZ4uWXY4tqL2cW47J',
    '1JxDJCyWNakZ5kECKdCU9Zka6mh34mZ7B2',
    '1JqDybm2nWTENrHvMyafbSXXtTk5Uv5QAn',
    '1BNwxHGaFbeUBitpjy2AsKpJ29Ybxntqvb',
    '1Am9UTGfdnxabvcywYG2hvzr6qK8T3oUZT',
    '1EYTGtG4LnFfiMvjJdsU7GMGCQvsRSjYhx'
  }
  ClusterList = {'1XPTgDRhN8RFnzniWCddobD9iKZatrvH4'}
  return ClusterList


def main(start_block, end_block):

    print('executing main')

    DIR = './'
    spark = SparkSession.builder.getOrCreate()

    print('downloading dataset')

    download_dataset(start_block, end_block, DIR, True)

    print('loading dataset')

    v_path = os.path.join(DIR, 'vertices-{}-{}.csv'.format(start_block, end_block))
    e_path = os.path.join(DIR, "edges-{}-{}.csv".format(start_block, end_block))

    v_df = spark.read.load(v_path,
                            format="csv",
                            sep=",",
                            inferSchema="true",
                            header="true"
                            ).distinct()
    e_df = spark.read.load(e_path,
                            format="csv",
                            sep=",",
                            inferSchema="true",
                            header="true"
                            ).distinct()

    unknown_v_df = e_df.select('src_id').union(e_df.select('dst_id')).distinct().subtract(v_df.select('id')).withColumnRenamed('src_id', 'id')
    unknown_v_df = unknown_v_df.withColumn('note', lit('unknown_tx')) \
                            .withColumn('tx_hash', lit(None)) \
                            .withColumn('block_height', lit(-1)) \
                            .withColumn('block_hash', lit(None)) \
                            .withColumn('fee', lit(0)) \
                            .withColumn('n_input', lit(0)) \
                            .withColumn('amount_input', lit(0)) \
                            .withColumn('n_output', lit(0)) \
                            .withColumn('amount_output', lit(0))
    v_df = v_df.union(unknown_v_df)

    e_df.createOrReplaceTempView('EDGES')
    address_df = e_df.select('address').subtract(spark.sql("select address from EDGES where address like 'coinbase%'"))

    v_df.createOrReplaceTempView('VERTICES')
    known_tx_df = spark.sql("select * from VERTICES where note = 'tx'")

    print('dataset loaded')
    print('generating graph')

    nx_graph = generate_nx_graph(v_df, e_df)

    print('graph generated')
    print('waiting for an address to cluster')

    st.set_page_config(
        page_title="Bitcoin Address Clustering",
        layout="wide"
    )
    st.title('Bitcoin Address Clustering')
    st.subheader('''
            Enter a bitcoin address to be clustered using the following heuristics:\n
            – common-input-ownership heuristic\n
            – change address detection heuristic\n
            – Coinbase transaction mining address clustering heuristic\n
            – multiple mining pool address clustering heuristic\n
            – mixed transaction recognition heuristic\n
            – Louvain community detection algorithm\n'''.format(start_block, end_block)
            )
    with st.container():
        st1, st2 = st.columns(2)
        with st1.form(key='form'):
            address = st.text_input(label='Insert address to cluster')
            submit_button = st.form_submit_button(label='Start Clustering')

    if submit_button:
        with st.spinner("Loading..."):
            address_df.createOrReplaceTempView('ADDRESSES')
            if spark.sql("select * from ADDRESSES where address = '"+address+"'").rdd.isEmpty():
                print('invalid address')
                st.error('Invalid address')
            else:
                print('clustering from ' + address)

                ClusterList = address_clustering(nx_graph, address, known_tx_df)

                new_e_df = e_df.filter(e_df.address.isin(ClusterList))
                tmp_df = new_e_df.select(new_e_df.src_id).union(new_e_df.select(new_e_df.dst_id)).distinct().withColumnRenamed('src_id', 'tmp_id')
                new_v_df = v_df.join(tmp_df, v_df.id == tmp_df.tmp_id).drop('tmp_id')

                print('clustering done')
                print('generating cluster graphs')

                new_pyvis_graph = generate_pyvis_graph(new_v_df, new_e_df)
                new_pyvis_graph.height = '1200px'
                new_pyvis_graph.width = '2400px'
                new_pyvis_graph.write_html(os.path.join(DIR, 'cluster_graph-{}-{}.html'.format(start_block, end_block)))

                print('cluster graphs generated')
                print('display cluster')
                st.success("Done!")
                HtmlFile = open(os.path.join(DIR, 'cluster_graph-{}-{}.html'.format(start_block, end_block)), 'r', encoding='utf-8')
                components.html(HtmlFile.read(), height=1200, scrolling=True)

                print('cluster displayed')

                st.dataframe(new_v_df.toPandas())
                st.dataframe(new_e_df.toPandas())
                st.dataframe(ClusterList)

    print('end main')
    return

if __name__ == "__main__":
    start_block = 0
    end_block = 100000
    main(start_block, end_block)
