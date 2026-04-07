"""Copyright 2023 JasmineGraph Team
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
import socket
import logging

import integration_common as common
from integration_workflow_core import run_core_workflow
from integration_workflow_cypher import run_cypher_workflow
from integration_workflow_intrapartition import run_intrapartition_workflow


def test(host, port):  # pylint: disable=too-many-branches
    """Test JasmineGraph server by running all integration workflows."""
    common.reset_state()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))

        run_core_workflow(sock)
        run_cypher_workflow(sock, host, port)
        run_intrapartition_workflow(sock)

        print()
        logging.info('Shutting down')
        sock.sendall(common.SHDN + common.LINE_END)
        if common.passed_all:
            print()
            logging.info('Passed all tests')
        else:
            print()
            logging.critical('Failed some tests')
            print(*common.failed_tests, sep='\n', file=sys.stderr)
            sys.exit(1)


if __name__ == '__main__':
    test(common.HOST, common.PORT)
