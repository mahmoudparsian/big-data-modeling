# bin

Shell scripts to format, start, and stop a local single-node Hadoop cluster.

## Contents

| Name | Type | Description |
|---|---|---|
| [`format-hadoop.sh`](format-hadoop.sh) | sh (245B) | Wipes local HDFS name/data/tmp dirs and formats the namenode (`hadoop namenode -format`) |
| [`start-hadoop.sh`](start-hadoop.sh) | sh (149B) | Starts HDFS and YARN (`start-dfs.sh` + `start-yarn.sh`) |
| [`stop-hadoop.sh`](stop-hadoop.sh) | sh (147B) | Stops HDFS and YARN (`stop-dfs.sh` + `stop-yarn.sh`) |
