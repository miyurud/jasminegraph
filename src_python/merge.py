"""Copyright 2020 JasmineGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import sys
import os
import logging
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s : [%(levelname)s]  %(message)s',
    handlers=[
        logging.FileHandler('merge.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logging.info('start executing merge.py')

arg_names = [
    'path_datafolder',
    'path_modelstore',
    'path_data',
    'graph_id',
    'partition_id',
]

FOLDER_PATH = 'data'
if os.path.exists(FOLDER_PATH):
    logging.info('Folder path \'%s\' exists', FOLDER_PATH)
else:
    os.makedirs(FOLDER_PATH)
    logging.info('Data folder created at %s', FOLDER_PATH)

args = dict(zip(arg_names, sys.argv[1:]))

path_attributes_localstore = args['path_datafolder'] + '/' + \
    args['graph_id'] + '_attributes_' + args['partition_id']
logging.info('Reading localstore node attributes from %s',
             path_attributes_localstore)
node_attributes_localstore = pd.read_csv(
    path_attributes_localstore, sep=r'\s+', lineterminator='\n', header=None)
node_attributes_localstore.set_index(0, inplace=True)

path_edges_localstore = args['path_modelstore'] + \
    '/' + args['graph_id'] + '_' + args['partition_id']
logging.info('Reading localstore edges from : %s', path_edges_localstore)
edges_localstore = pd.read_csv(
    path_edges_localstore, sep=r'\s+', lineterminator='\n', header=None)
edges_localstore.columns = ['source', 'target']


path_attributes_centralstore = args['path_datafolder'] + '/' + \
    args['graph_id'] + '_centralstore_attributes_' + args['partition_id']
logging.info('Reading centralstore node attributes from : %s',
             path_edges_localstore)
node_attributes_centralstore = pd.read_csv(
    path_attributes_centralstore, sep=r'\s+', lineterminator='\n', header=None)
node_attributes_centralstore.set_index(0, inplace=True)

path_edges_centralstore = args['path_modelstore'] + '/' + \
    args['graph_id'] + '_centralstore_' + args['partition_id']
logging.info('Reading centralstore edges from : %s', path_edges_localstore)
edges_centralstore = pd.read_csv(
    path_edges_centralstore, sep=r'\s+', lineterminator='\n', header=None)
edges_centralstore.columns = ['source', 'target']

# Reducing memory consumption
edges_centralstore = edges_centralstore.astype(
    {'source': 'uint32', 'target': 'uint32'})
edges_localstore = edges_localstore.astype(
    {'source': 'uint32', 'target': 'uint32'})
node_attributes_localstore = node_attributes_localstore.astype('float32')
node_attributes_centralstore = node_attributes_centralstore.astype('float32')

nodes = pd.concat([node_attributes_localstore, node_attributes_centralstore])
nodes = nodes.loc[~nodes.index.duplicated(keep='first')]
edges = pd.concat([edges_localstore, edges_centralstore], ignore_index=True)


path_nodes = args['path_data'] + args['graph_id'] + \
    '_nodes_' + args['partition_id'] + '.csv'
path_edges = args['path_data'] + args['graph_id'] + \
    '_edges_' + args['partition_id'] + '.csv'

logging.info('Writing nodes to : %s', path_nodes)
nodes.to_csv(path_nodes)

logging.info('Writing edges to : %s', path_nodes)
edges.to_csv(path_edges, index=False)

logging.info('complete executing merge.py')
