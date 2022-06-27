
print('installing libraries')

import streamlit as st
import streamlit.components.v1 as components
import pyspark
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
import requests
import wget
import csv
import os
from urllib import request
from urllib.error import HTTPError
from pyvis.network import Network

print('all libraries are imported')

def generate_pyvis_graph(v_df, e_df, a_df, cluster_id_selected):
    addr_clust = {row.address : row.cluster_id for row in a_df.rdd.collect()}
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
    for node in v_df.rdd.collect():
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
    for edge in e_df.rdd.collect():
        if edge.address[:4] != 'UTXO' and edge.address[:4] != 'coin':
            title = 'address: {}\n'.format(edge.address) + \
                    'value: {} BTC\n'.format(edge.value/100000000) + \
                    'cluster_id : {}'.format(addr_clust[edge.address])
            if addr_clust[edge.address] == cluster_id_selected:
                net.add_edge(source=str(edge.src_id), to=str(edge.dst_id), title=title, value=edge.value/100000000, color='red')
            else:
                net.add_edge(source=str(edge.src_id), to=str(edge.dst_id), title=title, value=edge.value/100000000)
        else:
            title = 'address: {}\n'.format(edge.address) + \
                    'value: {} BTC\n'.format(edge.value/100000000)
            net.add_edge(source=str(edge.src_id), to=str(edge.dst_id), title=title, value=edge.value/100000000)
    return net




def main(start_block, end_block):

    print('executing main')

    DIR = './'
    spark = SparkSession.builder.getOrCreate()

    d_path = os.path.join(DIR, 'blocks-{}-{}-clustered'.format(start_block, end_block))
    v_path = os.path.join(d_path, 'vertices-{}-{}'.format(start_block, end_block))
    e_path = os.path.join(d_path, 'edges-{}-{}'.format(start_block, end_block))
    a_path = os.path.join(d_path, 'addresses-{}-{}'.format(start_block, end_block))

    v_df = spark.read.load(v_path,
                            format="csv",
                            sep=",",
                            inferSchema="true",
                            header="true"
                            )
    e_df = spark.read.load(e_path,
                            format="csv",
                            sep=",",
                            inferSchema="true",
                            header="true"
                            )
    a_df = spark.read.load(a_path,
                            format="csv",
                            sep=",",
                            inferSchema="true",
                            header="true"
                            )
    unknown_v_df = e_df.select('src_id').union(e_df.select('dst_id')).distinct().subtract(v_df.select('id')).withColumnRenamed('src_id', 'id')
    unknown_v_df = unknown_v_df.withColumn('note', lit('unknown_tx')) \
                            .withColumn('tx_hash', lit(None)) \
                            .withColumn('block_height', lit(-1)) \
                            .withColumn('block_hash', lit(None)) \
                            .withColumn('fee', lit(0)) \
                            .withColumn('n_input', lit(0)) \
                            .withColumn('amount_input', lit(0)) \
                            .withColumn('n_output', lit(0)) \
                            .withColumn('amount_output', lit(0)) \
                            .withColumn('temporal_index', lit(-1))
    v_df = v_df.union(unknown_v_df)

    print('dataset loaded')

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

            a_df.createOrReplaceTempView('ADDRESSES')
            if spark.sql("select * from ADDRESSES where address = '"+address+"'").rdd.isEmpty():
                print('invalid address')
                st.error('Invalid address')
            else:
                a_df.createOrReplaceTempView('ADDRESSES')
                cluster_id = spark.sql("select cluster_id from ADDRESSES where address = '"+address+"'").head().cluster_id
                a_df.createOrReplaceTempView('ADDRESSES')
                ClusterList = spark.sql("select address from ADDRESSES where cluster_id = " + str(cluster_id)).withColumnRenamed('address', 'address_clustered')

                print('address {} is in cluster {}'.format(address, cluster_id))
                print('generating cluster graphs')

                new_e_df = e_df.join(ClusterList, e_df.address == ClusterList.address_clustered, 'inner').drop('address_clustered')
                tmp_df = new_e_df.select(new_e_df.src_id).union(new_e_df.select(new_e_df.dst_id)).withColumnRenamed('src_id', 'tmp_id').distinct()
                new_v_df = v_df.join(tmp_df, v_df.id == tmp_df.tmp_id, 'inner').drop('tmp_id')
                tmp_df = new_v_df.select('id')
                new_e_df = e_df.join(tmp_df, tmp_df.id == e_df.src_id, 'inner').union(e_df.join(tmp_df, tmp_df.id == e_df.dst_id, 'inner')).drop('id').distinct()
                tmp_df = new_e_df.select(new_e_df.src_id).union(new_e_df.select(new_e_df.dst_id)).distinct().withColumnRenamed('src_id', 'tmp_id')
                new_v_df = v_df.join(tmp_df, v_df.id == tmp_df.tmp_id).drop('tmp_id')

                new_pyvis_graph = generate_pyvis_graph(new_v_df, new_e_df, a_df, cluster_id)

                new_pyvis_graph.height = '1200px'
                new_pyvis_graph.width = '2400px'
                new_pyvis_graph.write_html(os.path.join(d_path, 'cluster_graph-{}-{}.html'.format(start_block, end_block)))

                print('cluster graphs generated')
                print('display cluster')
                st.success("Done!")
                HtmlFile = open(os.path.join(d_path, 'cluster_graph-{}-{}.html'.format(start_block, end_block)), 'r', encoding='utf-8')
                components.html(HtmlFile.read(), height=1200, scrolling=True)

                print('cluster displayed')

                st.dataframe(new_v_df.toPandas())
                st.dataframe(new_e_df.toPandas())
                st.dataframe(ClusterList)

    print('end main')
    return

if __name__ == "__main__":
    start_block = 0
    end_block = 120000
    main(start_block, end_block)
