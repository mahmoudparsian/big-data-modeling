% export SPARK_HOME=/Users/mparsian/spark-3.2.0 
% $SPARK_HOME/bin/pyspark --packages graphframes:graphframes:0.8.2-spark3.2-s_2.12
Python 3.8.9 (default, Mar 30 2022, 13:51:17)
[Clang 13.1.6 (clang-1316.0.21.2.3)] on darwin
Type "help", "copyright", "credits" or "license" for more information.
:: loading settings :: url = jar:file:/Users/mparsian/spark-3.2.0/jars/ivy-2.5.0.jar!/org/apache/ivy/core/settings/ivysettings.xml
Ivy Default Cache set to: /Users/mparsian/.ivy2/cache
The jars for the packages stored in: /Users/mparsian/.ivy2/jars
graphframes#graphframes added as a dependency
:: resolving dependencies :: org.apache.spark#spark-submit-parent-1cd4b9cb-3e00-4475-9f0e-ee9964a00285;1.0
	confs: [default]
	found graphframes#graphframes;0.8.2-spark3.2-s_2.12 in spark-packages
	found org.slf4j#slf4j-api;1.7.16 in central
downloading https://repos.spark-packages.org/graphframes/graphframes/0.8.2-spark3.2-s_2.12/graphframes-0.8.2-spark3.2-s_2.12.jar ...
	[SUCCESSFUL ] graphframes#graphframes;0.8.2-spark3.2-s_2.12!graphframes.jar (115ms)
downloading https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.16/slf4j-api-1.7.16.jar ...
	[SUCCESSFUL ] org.slf4j#slf4j-api;1.7.16!slf4j-api.jar (37ms)
:: resolution report :: resolve 4188ms :: artifacts dl 157ms
	:: modules in use:
	graphframes#graphframes;0.8.2-spark3.2-s_2.12 from spark-packages in [default]
	org.slf4j#slf4j-api;1.7.16 from central in [default]
	---------------------------------------------------------------------
	|                  |            modules            ||   artifacts   |
	|       conf       | number| search|dwnlded|evicted|| number|dwnlded|
	---------------------------------------------------------------------
	|      default     |   2   |   2   |   2   |   0   ||   2   |   2   |
	---------------------------------------------------------------------
:: retrieving :: org.apache.spark#spark-submit-parent-1cd4b9cb-3e00-4475-9f0e-ee9964a00285
	confs: [default]
	2 artifacts copied, 0 already retrieved (281kB/7ms)
22/04/22 16:33:09 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.2.0
      /_/

Using Python version 3.8.9 (default, Mar 30 2022 13:51:17)
Spark context Web UI available at http://10.0.0.234:4040
Spark context available as 'sc' (master = local[*], app id = local-1650670391027).
SparkSession available as 'spark'.
>>>
>>># create list of nodes
>>> vert_list = [("a", "Alice", 34),
...              ("b", "Bob", 36),
...              ("c", "Charlie", 30)]
>>>
>>># define column names for a node
>>> column_names_nodes = ["id", "name", "age"]
>>>
>>># create vertices_df as a Spark DataFrame
>>> vertices_df = spark.createDataFrame(
...      vert_list,
...      column_names_nodes
... )

>>>
>>># create list of edges
>>> edge_list = [("a", "b", "friend"),
...              ("b", "c", "follow"),
...              ("c", "b", "follow")]
>>>
>>># define column names for an edge
>>> column_names_edges = ["src", "dst", "relationship"]
>>>
>>># create edges_df as a Spark DataFrame
>>> edges_df = spark.createDataFrame(
...           edge_list,
...           column_names_edges
... )
>>>
>>># import required libriaries
>>> from graphframes import GraphFrame
>>>
>>># build a graph using GraphFrame library
>>> graph = GraphFrame(vertices_df, edges_df)
>>>
>>># examine built graph
>>> graph
GraphFrame(v:[id: string, name: string ... 1 more field], e:[src: string, dst: string ... 1 more field])
>>>
>>># access vertices of a graph
>>> graph.vertices.show()
+---+-------+---+
| id|   name|age|
+---+-------+---+
|  a|  Alice| 34|
|  b|    Bob| 36|
|  c|Charlie| 30|
+---+-------+---+

>>># access edges of a graph
>>> graph.edges.show()
+---+---+------------+
|src|dst|relationship|
+---+---+------------+
|  a|  b|      friend|
|  b|  c|      follow|
|  c|  b|      follow|
+---+---+------------+

>>>