# Neo4j-to-Gremlin Converter

The following script converts neo4j export format to a gremlin bulk load format
supported by Aerospike Graph.

The script expects that the neo4j dataset being exported has single labels or the 
labels will be combined with a ':' delimiter 

## Usage

To use the script, run the following command:

```bash
python3 neo4j-to-gremlin.py -i <input-directory> -o <output-directory>
```

The input-directory provided to the script should be a directory that contains neo4j csv files.
The script will recursively pick up then iterate over the csv files in the input directory.

The output-directory provided to the script is where all output gremlin csv files created and placed.


## Exporting from neo4j

Please refer to the documentation for [exporting neo4j to csv](https://neo4j.com/labs/apoc/4.1/export/csv/).

## Bulk Loading into Aerospike Graph

Please refer to the documentation for 
[bulk loading into Aerospike Graph](https://docs.aerospike.com/graph/usage/bulk-loader). 