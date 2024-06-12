This folder contains a simple script for using the stream loader (beta).

## Update the following in the incremental_load_example.py script before usage

```
VERTICES_BUCKET = f"gs://<path_to_vertex_bucket>"
EDGES_BUCKET = f"gs://<path_to_edge_bucket>"
IP_ADDRESS = "<ip_address>"
```

## Usage 
`python3 incremental_load_example.py <mode> <bulk_load_id>`
`mode`: `start`, `status`, `await`
`bulk_load_id`: Only applicable for `status` and `await`
Example of correct start usage: `python3 incremental_load_example.py start`
Example of correct status usage: `python3 incremental_load_example.py status <uuid>`
Example of correct await usage: `python3 incremental_load_example.py await <uuid>`

## Modes

### Start
Running with `start` will kick off a new load. The id of the load will be printed by the script.

### Status
Running with `status` will return the status of a running load for the provided id.

### Await
Running with `await` will cause the script to block until the load for the provided id completes.

### Example usage

```bash
lyndon@pop-os:~/github/firefly$ python3 ./incremental_load_example.py start
Bulk load started with id 06bb8ba7-77db-41ad-b75a-28f4119d30a4

lyndon@pop-os:~/github/firefly$ python3 ./incremental_load_example.py status 06bb8ba7-77db-41ad-b75a-28f4119d30a4
Status: {'STATE': 'RUNNING', 'DANGING_EDGES': 0, 'PERFORMANCE': {'UNIT': 'SECOND', 'INTERVAL_IO': 109, 'TYPE': 'vertex', 'TOTAL_IO': 3952, 'AVERAGE_IO': 109}}

lyndon@pop-os:~/github/firefly$ python3 ./incremental_load_example.py status 06bb8ba7-77db-41ad-b75a-28f4119d30a4
Status: {'STATE': 'RUNNING', 'DANGING_EDGES': 0, 'PERFORMANCE': {'UNIT': 'SECOND', 'INTERVAL_IO': 154, 'TYPE': 'vertex', 'TOTAL_IO': 4260, 'AVERAGE_IO': 109}}

lyndon@pop-os:~/github/firefly$ python3 ./incremental_load_example.py status 06bb8ba7-77db-41ad-b75a-28f4119d30a4
Status: {'STATE': 'RUNNING', 'DANGING_EDGES': 0, 'PERFORMANCE': {'UNIT': 'SECOND', 'INTERVAL_IO': 154, 'TYPE': 'vertex', 'TOTAL_IO': 4414, 'AVERAGE_IO': 110}}

lyndon@pop-os:~/github/firefly$ python3 ./incremental_load_example.py status 06bb8ba7-77db-41ad-b75a-28f4119d30a4
Status: {'STATE': 'RUNNING', 'DANGING_EDGES': 0, 'PERFORMANCE': {'UNIT': 'SECOND', 'INTERVAL_IO': 140, 'TYPE': 'vertex', 'TOTAL_IO': 4554, 'AVERAGE_IO': 108}}

lyndon@pop-os:~/github/firefly$ python3 ./incremental_load_example.py await 06bb8ba7-77db-41ad-b75a-28f4119d30a4
lyndon@pop-os:~/github/firefly$ 
```