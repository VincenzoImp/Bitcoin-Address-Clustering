# Bitcoin Address Clustering

This project extracts information from the Bitcoin blockchain to create transaction graphs and perform chain analysis through Bitcoin address clustering. The main goal is to group Bitcoin addresses that likely belong to the same entity using various heuristic methods, enabling better understanding of Bitcoin transaction flows and entity behaviors.

## Table of Contents

1. [Features](#features)
2. [Installation](#installation)
3. [Usage](#usage)
4. [Project Structure](#project-structure)
5. [Methodology](#methodology)
6. [Heuristics Implemented](#heuristics-implemented)
7. [Results](#results)
8. [Web Application](#web-application)
9. [Use Cases](#use-cases)
10. [Contributing](#contributing)
11. [License](#license)

## Features

- **Blockchain Data Collection**: Automated extraction of Bitcoin blockchain data from blocks 0 to 115,000
- **Transaction Graph Generation**: Creates directed graphs with transactions as nodes and UTXOs as edges
- **Multiple Heuristics**: Implements 10+ different clustering heuristics
- **Spark Integration**: Uses Apache Spark for distributed data processing
- **Interactive Visualization**: Web-based interface for exploring address clusters
- **Chain Analysis**: Tools for analyzing transaction flows and entity movements
- **Comprehensive Results**: Achieves 45-78% reduction in entity count through clustering

## Installation

### Prerequisites

- Python 3.7+
- Apache Spark
- Google Colab (for the provided notebook) or local Jupyter environment
- Sufficient storage space for blockchain data

### Dependencies

Install the required packages:

```bash
pip install pyspark
pip install PyDrive
pip install wget
pip install pyvis
pip install streamlit
pip install networkx
pip install matplotlib
```

### Setup

1. Clone the repository:
```bash
git clone https://github.com/VincenzoImp/Bitcoin-Address-Clustering.git
cd Bitcoin-Address-Clustering
```

2. Configure Spark context with appropriate memory settings:
```python
conf = SparkConf()\
    .set('spark.executor.memory', '50G')\
    .set('spark.driver.memory', '50G')\
    .set('spark.driver.maxResultSize', '50G')\
    .set("spark.driver.cores", "10")\
    .set("spark.sql.analyzer.maxIterations", "100000")
```

## Usage

### Basic Usage

1. **Set Global Constants**:
```python
start_block = 0
end_block = 115000
```

2. **Download Dataset**:
```python
v_path, e_path, a_path, d_path = download_dataset(start_block, end_block, DATA_DIR, spark, True)
```

3. **Generate Transaction Graph**:
```python
nx_graph = generate_nx_graph(v_df, e_df, graph_path, start_block, end_block, True)
```

4. **Apply Clustering Algorithm**:
```python
clustered_addresses = address_clustering(nx_graph, a_df, known_tx_df, spark, start_block, debug=True)
```

### Web Application

Launch the interactive web interface:

```bash
streamlit run app.py 0 115000
```

## Project Structure

```
Bitcoin-Address-Clustering/
├── dataset/
│   └── blocks-0-115000/
│       ├── vertices-0-115000/
│       ├── edges-0-115000/
│       └── addresses-0-115000/
├── app/
│   ├── app.py
│   ├── Bitcoin.png
│   └── bitcoin-img.svg
├── Bitcoin_Address_Clustering.ipynb
└── README.md
```

## Methodology

The project follows a systematic approach:

1. **Data Collection**: Extract transaction data from Bitcoin blockchain via Blockchain.info API
2. **Graph Construction**: Build directed graphs with transactions as nodes and UTXOs as edges
3. **Heuristic Application**: Apply multiple clustering heuristics in sequence
4. **Result Analysis**: Evaluate clustering effectiveness and entity reduction
5. **Visualization**: Generate interactive graphs for cluster exploration

## Heuristics Implemented

### Simple Heuristics

1. **Satoshi Heuristic**: Groups addresses from early coinbase transactions (blocks < 19,500) as likely belonging to Satoshi Nakamoto

2. **Coinbase Transaction Mining Address Clustering**: Assumes all output addresses from coinbase transactions belong to the same miner

3. **Common-Input-Ownership**: Groups all input addresses in multi-input transactions as belonging to the same entity

4. **Single Input/Output**: Treats single input, single output transactions as address movements within the same entity

5. **Consolidation Transaction**: Groups addresses in transactions with multiple inputs and single output

### Advanced Heuristics

6. **Payment Transaction Analysis**: Identifies payment transactions with change addresses

7. **Change Address Detection**: Uses multiple sub-heuristics:
   - Same address in input and output
   - Address reuse patterns
   - Unnecessary input analysis
   - New address identification
   - Round number detection

8. **Mixed Transaction Recognition**: Identifies and handles CoinJoin transactions using taint analysis

## Results

### Clustering Effectiveness

- **Initial Addresses**: ~1,000,000 unique addresses
- **After Clustering**: ~550,000 entities (45% reduction)
- **With Small Cluster Assumption**: ~220,000 entities (78% reduction)

### Entity Distribution

The clustering reveals a power-law distribution of entity sizes:
- Most entities contain 1-2 addresses
- Few large entities contain hundreds of addresses
- Largest clusters likely represent exchanges or major services

## Web Application

The project includes a Streamlit-based web interface that allows users to:

- Input Bitcoin addresses for clustering analysis
- Visualize transaction graphs with cluster highlighting
- Explore entity relationships and transaction flows
- Download clustering results and statistics

### Features:
- Interactive network visualization using PyVis
- Real-time address clustering
- Detailed transaction information
- Export capabilities

## Use Cases

### 1. Entity Movement Visualization
Track how funds move between addresses belonging to the same entity:

```python
address = '115uADbwcLhfKeWJzy7EHjSWjn3dpHK1vZ'
cluster_graph = visualize_entity_movements(address)
```

### 2. Chain Analysis Queries
Answer specific questions about blockchain activity:

- "How many unique miners were active before 2011?"
- "What's the largest entity by address count?"
- "Which entities show mixing behavior?"

### 3. Research Applications
- Academic research on Bitcoin privacy
- Compliance and AML investigations
- Cryptocurrency forensics
- Network analysis studies

## Data Sources

- **Blockchain Data**: Blockchain.info API
- **Block Range**: Genesis block (0) to block 115,000
- **Time Period**: January 2009 to February 2011
- **Transactions**: ~400,000 transactions analyzed

## Performance Considerations

- **Memory Requirements**: 50GB+ RAM recommended for full dataset
- **Processing Time**: Several hours for complete clustering
- **Storage**: ~10GB for preprocessed datasets
- **Scalability**: Designed for distributed processing with Spark

## Limitations

- **Privacy Techniques**: Advanced privacy methods (CoinJoin, mixers) can reduce clustering effectiveness
- **False Positives**: Heuristics may incorrectly group unrelated addresses
- **Temporal Scope**: Analysis limited to early Bitcoin history (2009-2011)
- **Data Availability**: Depends on external API availability

## Contributing

Contributions are welcome! Please feel free to submit pull requests, report bugs, or suggest new features.

### Development Guidelines

1. Follow PEP 8 style guidelines
2. Add comprehensive docstrings
3. Include unit tests for new heuristics
4. Update documentation for new features

## Future Enhancements

- **Extended Block Range**: Support for more recent blockchain data
- **Advanced Clustering**: Integration of machine learning approaches
- **Real-time Analysis**: Live blockchain monitoring capabilities
- **Privacy Metrics**: Quantitative privacy assessment tools

## References

- Satoshi Nakamoto's Bitcoin Whitepaper
- "A Fistful of Bitcoins" - Meiklejohn et al.
- "An Analysis of Anonymity in the Bitcoin System" - Reid & Harrigan
- Blockchain.info API Documentation

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Acknowledgments

- Bitcoin Core developers
- Apache Spark community
- Blockchain.info for API access
- Academic research community for heuristic development

---

**Note**: This tool is intended for research and educational purposes. Users should comply with applicable laws and regulations when analyzing blockchain data.
