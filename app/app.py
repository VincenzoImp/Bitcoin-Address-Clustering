import sys
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
import base64
from PIL import Image


def generate_pyvis_graph(v_df, e_df, a_df, cluster_id_selected):
    addr_clust = {row.address : row.cluster_id for row in a_df.rdd.collect()}
    net = Network(
                height='100%',
                width='80%',
                directed=True
                )
    net.show_buttons(filter_=["physics"])

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

def render_svg(svg_file):
    with open(svg_file, "r") as f:
        lines = f.readlines()
        svg = "".join(lines)
        b64 = base64.b64encode(svg.encode("utf-8")).decode("utf-8")
        html = r'<img src="data:image/svg+xml;base64,%s"/>' % b64
        html = '<p style="text-align:center;">' + html + '</p>'
        return html










def main(start_block, end_block):

    print('executing main')

    DIR = './'

    spark = SparkSession.builder.getOrCreate()
    d_path = os.path.join(DIR, 'blocks-{}-{}'.format(start_block, end_block))
    e_path = os.path.join(d_path, 'edges-{}-{}'.format(start_block, end_block))
    v_path = os.path.join(d_path, 'vertices-{}-{}'.format(start_block, end_block))
    a_path = os.path.join(d_path, 'addresses-{}-{}'.format(start_block, end_block))
    e_df = spark.read.load(e_path,
                            format="csv",
                            sep=",",
                            inferSchema="true",
                            header="true"
                            )
    v_df = spark.read.load(v_path,
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
    a_df = spark.read.load(a_path,
                            format="csv",
                            sep=",",
                            inferSchema="true",
                            header="true"
                            )

    img = Image.open('Bitcoin.png')
    st.set_page_config(
        page_title="Bitcoin Address Clustering",
        layout="wide",
        page_icon=img
    )

    st.title('Bitcoin Address Clustering\n')
    with st.container():
        c1, c2 = st.columns(2)
        c1.subheader('''
                Enter a bitcoin address to be clustered using the following heuristics:\n
                - Satoshi heuristic\n
                - Coinbase transaction mining address clustering heuristic\n
                - Common-input-ownership heuristic\n
                - Single input and single output heuristic\n
                - Consolidation transaction heuristic\n
                - Payment transaction with amount payed and change address heuristic\n
                - Change address detection heuristic\n
                \t- same address in input and output heuristic\n
                \t- address reuse heuristic\n
                \t- Unnecessary input heuristic\n
                \t- new address in output heuristic\n
                \t- round number heuristic\n
                - Mixed transaction recognition heuristic\n
                \t- taint analysis and coinjoin sudoku
                ''')

        c2.markdown(render_svg('bitcoin-img.svg'), unsafe_allow_html=True)
        c2.write('\n\n\n\n')

        with c2.form(key='form'):
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

                new_pyvis_graph.height = '800px'
                new_pyvis_graph.width = '2400px'
                new_pyvis_graph.write_html(os.path.join(d_path, 'cluster_graph-{}-{}.html'.format(start_block, end_block)))

                print('cluster graphs generated')
                print('display cluster')
                st.success("Done!")
                HtmlFile = open(os.path.join(d_path, 'cluster_graph-{}-{}.html'.format(start_block, end_block)), 'r', encoding='utf-8')
                components.html(HtmlFile.read(), height=800, scrolling=True)

                print('cluster displayed')

                st.subheader('Graph Nodes')
                st.dataframe(new_v_df.toPandas())
                st.subheader('Graph Edges')
                st.dataframe(new_e_df.toPandas())
                st.subheader('Clustered Addresses')
                st.dataframe(ClusterList.toPandas())

    print('end main')
    return

if __name__ == "__main__":
    start_block = int(sys.argv[1])
    end_block = int(sys.argv[2])
    main(start_block, end_block)
