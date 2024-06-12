import sys

from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.graph_traversal import *
from gremlin_python.process.traversal import *
from gremlin_python.process.strategies import *
from gremlin_python.process.anonymous_traversal import traversal

VERTICES_BUCKET = f"gs://<path_to_vertex_bucket>"
EDGES_BUCKET = f"gs://<path_to_edge_bucket>"
IP_ADDRESS = "<ip_address>"


def start_load(g):
    bulk_load_id = g.with_("evaluationTimeout", 60 * 1000). \
        call("stream-load"). \
        with_("aerospike.graphloader.vertices", VERTICES_BUCKET). \
        with_("aerospike.graphloader.edges", EDGES_BUCKET). \
        with_("incremental", True). \
        next()
    print("Bulk load started with id " + str(bulk_load_id["id"]))


def load_status(g, bulk_load_id):
    output = g.with_("evaluationTimeout", 10 * 24 * 60 * 60 * 1000). \
        call("status").with_("id", bulk_load_id).next()
    print("Status: " + str(output))


def await_complete(g, bulk_load_id):
    g.with_("evaluationTimeout", 10 * 24 * 60 * 60 * 1000). \
        call("await").with_("id", bulk_load_id).iterate()

def usage(args):
    print("Usage: python3 incremental_load_example.py <mode> <bulk_load_id>")
    print("mode: start, status, await")
    print("bulk_load_id: Only applicable for <status> and <await>")
    print("Example of correct start usage: python3 incremental_load_example.py start")
    print("Example of correct status usage: python3 incremental_load_example.py status 123456")
    print("Example of correct await usage: python3 incremental_load_example.py await 123456")
    print("Arguments provided: " + str(args))
    sys.exit(1)


if __name__ == "__main__":
    g = traversal().with_remote(
        DriverRemoteConnection(f'ws://' + IP_ADDRESS + ':8182/gremlin', 'g'))
    try:
        if len(sys.argv) == 2:
            if sys.argv[1] == "start":
                start_load(g)
            else:
                usage(sys.argv)
        elif len(sys.argv) == 3:
            if sys.argv[1] == "status":
                load_status(g, sys.argv[2])
            elif sys.argv[1] == "await":
                await_complete(g, sys.argv[2])
            else:
                usage(sys.argv)
        else:
            usage(sys.argv)
    except Exception as e:
        print("Error", e)
        print(e)