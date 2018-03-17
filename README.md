# Query-Centric Semantic Partitioning (SPARTI)

* Adaptively partition based on query-workload
* Precompute Bloom join between the most frequent triples joins (MF-TJ) combinations
* Represent properties as semantic vectors to infer relatedness
* Partition related properties based on a greedy algorithm and a cost model
* An RDF metadata layer for skipping data
* Current version is implemented to run over <b>Apache Spark</b>

Usage:

	 SpartiStorage
	    "l", "localdb"   "Local database location (Contains Metadata)"
	    "r"  "hdfsdb"    "HDFS database location (Contains Parquet Files)"
	    "d"  "dataset"   "Create database for input dataset (NTriple Format Required"
	    "s"  "separator" "Separator used between Subject/Property/Object (default is tab)"
	    "t"  "storetype" "Store type (e.g. spark)"
	    "b" "benchmark"  "Specify the benchmark/dataset name (e.g., WatDiv,LUBM or Generic)"

     SpartiExecution
        "l"  "localdb" "Local database location (Contains Metadata)"
        "r"  "hdfsdb"  "HDFS database location (Contains Parquet Files)"
        "q"  "queryfile" "SPARQL Queries to execute");
        "b"  "benchmark" "Specify the benchmark/dataset name (e.g., WatDiv,LUBM or Generic)"