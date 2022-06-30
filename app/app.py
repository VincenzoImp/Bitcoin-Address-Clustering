import sys
import os
os.system('pip install -U -q wget')
os.system('pip install -U -q pyspark')
os.system('pip install -U -q pyvis')
os.system('pip install -U -q streamlit')
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
from urllib import request
from urllib.error import HTTPError
from pyvis.network import Network
import base64
from PIL import Image
import pandas as pd




img = Image.open('Bitcoin.png')
st.set_page_config(
    page_title="Bitcoin Address Clustering",
    layout="wide",
    page_icon=img
)


@st.cache
def load_df(start_block, end_block, d_path):
    
    e_path = os.path.join(d_path, 'edges-{}-{}'.format(start_block, end_block))
    v_path = os.path.join(d_path, 'vertices-{}-{}'.format(start_block, end_block))
    a_path = os.path.join(d_path, 'addresses-{}-{}'.format(start_block, end_block))
    spark = SparkSession.builder.getOrCreate()
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
    return e_df.toPandas(), v_df.toPandas(), a_df.toPandas()




def generate_pyvis_graph(v_df, e_df, a_df, cluster_id_selected):

    net = Network(
                height='100%',
                width='80%',
                directed=True
                )
    net.show_buttons(filter_=["physics"])

    for _, node in v_df.iterrows():
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

    for _, edge in e_df.iterrows():
        try:
            if edge.address[:4] != 'UTXO' and edge.address[:4] != 'coin':
                cluster_id = a_df[a_df['address'] == edge.address].cluster_id.iloc[0]
                title = 'address: {}\n'.format(edge.address) + \
                        'value: {} BTC\n'.format(edge.value/100000000) + \
                        'cluster_id : {}'.format(cluster_id)
                if cluster_id == cluster_id_selected:
                    net.add_edge(source=str(edge.src_id), to=str(edge.dst_id), title=title, value=edge.value/100000000, color='red')
                else:
                    net.add_edge(source=str(edge.src_id), to=str(edge.dst_id), title=title, value=edge.value/100000000)
            else:
                title = 'address: {}\n'.format(edge.address) + \
                        'value: {} BTC\n'.format(edge.value/100000000)
                net.add_edge(source=str(edge.src_id), to=str(edge.dst_id), title=title, value=edge.value/100000000)
        except:
            pass
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
    d_path = os.path.join(DIR, 'blocks-{}-{}'.format(start_block, end_block))

    e_df, v_df, a_df = load_df(start_block, end_block, d_path)

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
            row = a_df[a_df.address == address]
            if row.empty:
                print('invalid address')
                st.error('Invalid address')
            else:
                # get cluster id of address entered
                cluster_id = row.cluster_id.iloc[0]
                # select all address in cluster id
                addrs_in_cluster_df = pd.DataFrame({'address' : a_df[a_df.cluster_id == cluster_id].address})

                print('address {} is in cluster {}'.format(address, cluster_id))
                print('generating cluster graphs')

                new_e_df = e_df[e_df.address.isin(addrs_in_cluster_df.address)]
                new_v_df = v_df[v_df.id.isin(new_e_df.src_id) | v_df.id.isin(new_e_df.dst_id)]
                new_e_df = e_df[e_df.src_id.isin(new_v_df.id) | e_df.dst_id.isin(new_v_df.id)]
                new_v_df = v_df[v_df.id.isin(new_e_df.src_id) | v_df.id.isin(new_e_df.dst_id)]

                n_v = new_v_df.shape[0]
                n_e = new_e_df.shape[0]
                print(n_v, n_e)

                if n_v + n_e > 1400:
                    st.error('Too large graph to plot: {} nodes, {} edges'.format(n_v, n_e))
                else:

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
                    st.dataframe(new_v_df.reset_index())
                    st.subheader('Graph Edges')
                    st.dataframe(new_e_df.reset_index())
                    st.subheader('Clustered Addresses')
                    st.dataframe(addrs_in_cluster_df.reset_index())

    print('end main')
    return

if __name__ == "__main__":
    start_block = int(sys.argv[1])
    end_block = int(sys.argv[2])
    main(start_block, end_block)
