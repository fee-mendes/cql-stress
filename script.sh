
commands=(
"cargo run --release --bin cql-stress-cassandra-stress read duration=30m keysize=12 -node 172.31.21.150 shard-connection-count=1 -rate threads=16 fixed -schema 'replication(strategy=NetworkTopologyStrategy, factor=1)' keyspace=ks 'compaction(strategy=IncrementalCompactionStrategy, bloom_filter_fp_chance=0.1)' -col names=v 'size=fixed(1000)' -pop seq=1..60M"
"cargo run --release --bin cql-stress-cassandra-stress read duration=30m keysize=12 -node 172.31.21.150 shard-connection-count=1 -rate threads=16 fixed -schema 'replication(strategy=NetworkTopologyStrategy, factor=1)' keyspace=ks 'compaction(strategy=IncrementalCompactionStrategy, bloom_filter_fp_chance=0.1)' -col names=v 'size=fixed(1000)' -pop seq=1..60M"
"cargo run --release --bin cql-stress-cassandra-stress read duration=30m keysize=12 -node 172.31.21.150 shard-connection-count=1 -rate threads=16 fixed -schema 'replication(strategy=NetworkTopologyStrategy, factor=1)' keyspace=ks 'compaction(strategy=IncrementalCompactionStrategy, bloom_filter_fp_chance=0.1)' -col names=v 'size=fixed(1000)' -pop seq=1..60M"
"cargo run --release --bin cql-stress-cassandra-stress read duration=30m keysize=12 -node 172.31.21.150 shard-connection-count=1 -rate threads=16 fixed -schema 'replication(strategy=NetworkTopologyStrategy, factor=1)' keyspace=ks 'compaction(strategy=IncrementalCompactionStrategy, bloom_filter_fp_chance=0.1)' -col names=v 'size=fixed(1000)' -pop seq=1..60M"
)

parallel --ungroup -j 4 ::: "${commands[@]}"
