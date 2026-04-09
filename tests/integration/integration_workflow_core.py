"""Copyright 2026 JasmineGraph Team
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
import integration_common as common
from integration_workflow_cypher import HDFS_CONFIG_PATH


# Test fixture paths for graph and configuration data
POWERGRID_ROW = b'|1|powergrid|/var/tmp/data/powergrid.dl|op|'  # NOSONAR
POWERGRID_GRAPH_PATH = b'powergrid|/var/tmp/data/powergrid.dl'  # NOSONAR
CORA_GRAPH_PATH = b'cora|/var/tmp/data/cora/cora.cites|/var/tmp/data/cora/cora.content'  # NOSONAR
CORA_ROW = b'|2|cora|/var/tmp/data/cora/cora.cites|op|'  # NOSONAR


def run_core_workflow(sock):
    """Run core non-cypher integration test sequence."""
    print()
    common.logging.info('Testing lst')
    common.send_and_expect_response(sock, 'Initial lst', common.LIST, common.EMPTY)

    print()
    common.logging.info('Testing adgr')
    common.send_and_expect_response(sock, 'adgr', common.ADGR, common.SEND, exit_on_failure=True)
    common.send_and_expect_response(
        sock,
        'adgr',
        POWERGRID_GRAPH_PATH,
        common.DONE,
        exit_on_failure=True,
    )

    print()
    common.logging.info('Testing lst after adgr')
    common.send_and_expect_response(sock, 'lst after adgr', common.LIST, POWERGRID_ROW)

    print()
    common.logging.info('Testing ecnt')
    common.send_and_expect_response(sock, 'ecnt', common.ECNT, b'graphid-send')
    common.send_and_expect_response(sock, 'ecnt', b'1', b'6594')

    print()
    common.logging.info('Testing vcnt')
    common.send_and_expect_response(sock, 'vcnt', common.VCNT, b'graphid-send')
    common.send_and_expect_response(sock, 'vcnt', b'1', b'4941')

    print()
    common.logging.info('Testing trian')
    common.send_and_expect_response(sock, 'trian', common.TRIAN,
                                    b'graphid-send', exit_on_failure=True)
    common.send_and_expect_response(
        sock, 'trian', b'1', b'priority(>=1)', exit_on_failure=True)
    common.send_and_expect_response(sock, 'trian', b'1', b'651')

    print()
    common.logging.info('Testing pgrnk')
    common.send_and_expect_response(sock, 'pgrnk', common.PGRNK,
                                    b'grap', exit_on_failure=True)
    common.send_and_expect_response(
        sock, 'pgrnk', b'1|0.5|40', b'priority(>=1)', exit_on_failure=True)
    common.send_and_expect_response(sock, 'pgrnk', b'1', common.DONE, exit_on_failure=True)

    print()
    common.logging.info('Testing adgr-cust')
    common.send_and_expect_response(sock, 'adgr-cust', common.ADGR_CUST,
                                    b'Select a custom graph upload option' + common.LINE_END +
                                    b'1 : Graph with edge list + text attributes list'
                                    + common.LINE_END +
                                    b'2 : Graph with edge list + JSON attributes list'
                                    + common.LINE_END +
                                    b'3 : Graph with edge list + XML attributes list',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'adgr-cust',
                                    b'1',
                                    b'Send <name>|<path to edge list>|<path to attribute file>|' +
                                    b'(optional)<attribute data type: int8. int16, int32 or float>',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'adgr-cust',
                                    CORA_GRAPH_PATH,
                                    common.DONE, exit_on_failure=True)

    print()
    common.logging.info('Testing lst after adgr-cust')
    common.send_and_expect_response(sock, 'lst after adgr-cust', common.LIST,
                                    POWERGRID_ROW
                                    + common.LINE_END
                                    + CORA_ROW)

    print()
    common.logging.info('Testing merge')
    common.send_and_expect_response(sock, 'merge', common.MERGE,
                                    b'Available main flags:' + common.LINE_END +
                                    b'graph_id' + common.LINE_END +
                                    b'Send --<flag1> <value1>')
    common.send_and_expect_response(
        sock, 'merge', b'--graph_id 2', common.DONE, exit_on_failure=True)

    print()
    common.logging.info('Testing train')
    common.send_and_expect_response(sock, 'train', common.TRAIN,
                                    b'Available main flags:' + common.LINE_END +
                                    b'graph_id learning_rate batch_size validate_iter epochs' +
                                    common.LINE_END
                                    + b'Send --<flag1> <value1> --<flag2> <value2> ..',
                                    exit_on_failure=True)
    common.send_and_expect_response(
        sock, 'train', b'--graph_id 2', common.DONE, exit_on_failure=True)

    print()
    common.logging.info('Testing rmgr')
    common.send_and_expect_response(sock, 'rmgr', common.RMGR, common.SEND)
    common.send_and_expect_response(sock, 'rmgr', b'2', common.DONE)

    print()
    common.logging.info('Testing lst after rmgr')
    common.send_and_expect_response(sock, 'lst after rmgr', common.LIST, POWERGRID_ROW)
    
    print()
    common.logging.info('Testing sdhdfs')
    common.send_and_expect_response(sock, 'sdhdfs', common.SDHDFS, b'Graph ID:', exit_on_failure=True)
    common.send_and_expect_response(sock, 'sdhdfs', b'1',
                                b'Do you want to use the default HDFS server(y/n)?',
                                exit_on_failure=True)
    common.send_and_expect_response(sock, 'sdhdfs', b'n',
                                b'Send the file path to the HDFS configuration file.'
                                b' This file needs to be in some directory location '
                                b'that is accessible for JasmineGraph master',
                                exit_on_failure=True)
    common.send_and_expect_response(sock, 'sdhdfs', b'/var/tmp/config/hdfs_config.txt',
                                b'HDFS destination file path:',
                                exit_on_failure=True)
    common.send_and_expect_response(sock, 'sdhdfs', b'/home/powergrid.dl', common.DONE,
                                exit_on_failure=True)

    common.send_and_expect_response(sock, 'rmgr', common.RMGR, common.SEND)
    common.send_and_expect_response(sock, 'rmgr', b'1', common.DONE)

    print()
    common.logging.info('Testing adhdfs for custom HDFS server')
    common.send_and_expect_response(sock, 'adhdfs', common.ADHDFS,
                                    b'Do you want to use the default HDFS server(y/n)?',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'adhdfs', b'n',
                                    b'Send the file path to the HDFS configuration file.' +
                                    b' This file needs to be in some directory location ' +
                                    b'that is accessible for JasmineGraph master',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'adhdfs', HDFS_CONFIG_PATH,
                                    b'HDFS file path: ',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'adhdfs', b'/home/powergrid.dl',
                                    b'Is this an edge list type graph(y/n)?',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'adhdfs', b'y',
                                    b'Is this a directed graph(y/n)?',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'adhdfs', b'y', common.DONE, exit_on_failure=True)

    print()
    common.logging.info('Testing lst after adhdfs')
    common.send_and_expect_response(sock, 'lst after adhdfs', common.LIST,
                                    b'|1|/home/powergrid.dl|hdfs:/home/powergrid.dl|op|',
                                    exit_on_failure=True)
