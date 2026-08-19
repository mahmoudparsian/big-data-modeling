# MapReduce Examples

* Example-1: find average of gene values per gene 
  using `map()` and `reduce()`

* Example-2: find average of gene values per gene 
  using `map()` and `combine()` and `reduce()`

# Input 

Each record has the following format

	<gene_id><,><gene_value>

Sample Input:

~~~text
g1,2.1
g1,1.1
g1,1.3
g2,1.1
g2,3.1
g2,4.3
g1,1.0
g1,2.0
g2,1.6
g2,4.4
...

~~~

# MapReduce JOB

The goal of MR job is find average per `gene_id`


# Example-1: using `map()` and `reduce()`

~~~text
# key is a record number and ignored
# value is the entire record of input
map(key, value){ 
  # tokenize the given input record
  tokens = value.split(",")
  gene_id = tokens[0]
  gene_value = tokens[1]
  
  # generate output from mapper
  emit(gene_id, gene_value)
}
~~~


Output of Sort & Shuffle:

~~~text
(gene_id_1, [v1, v2, v3, ...])
(gene_id_2, [a1, a2, a3, ...])
...
~~~

~~~text
=======================
Reducer SHORTER VERSION
=======================
# key is a gene_id
# values : Iterable<Double>
reduce(key, values){ 
  total = sum(values)
  count = len(values)
  avg = total / count
  emit (key, avg)
}
~~~

# Example-2: using `map()` and `combine()` and `reduce()`

### Revised Mapper for Combiner:

~~~text
# key is a record number and ignored
# value is the entire record of input
map(key, value){ 
  # tokenize the given input record
  tokens = value.split(",")
  gene_id = tokens[0]
  gene_value = tokens[1]
  
  # generate output from mapper
  # output of mapper is (sum, count),
  # which is a monoid under addition function
  emit(gene_id, (gene_value, 1))
}
~~~

### Combiner 

~~~text
# key is a gene_id
# values : Iterable<(Double, Integer)>
# example: values: [(1.1, 1), (1.7, 1), (1.2, 1)]
combine(key, values){ 
  sum = 0
  count = 0
  for p in values {
     # p = (sum, count)
     sum += p[0]
     count += p[1]
  }
  
  emit (key, (sum, count))
}
~~~

Output of Sort & Shuffle:

~~~text
(gene_id_1, [(v1, c1), (v2, c2), (v3, c3), ...])
(gene_id_2, [(a1, d1), (a2, d2), (a3, d3), ...])
...
~~~

### Reducer 

~~~
# key is a gene_id
# values : Iterable<(Double, Integer)>
# example: values: [(4.0, 3), (7.2, 4), (9.3, 6)]
reduce(key, values){ 
  sum = 0
  count = 0
  for p in values {
     # p = (sum, count)
     sum += p[0]
     count += p[1]
  }
  #
  avg = sum / count
  emit (key, avg)
}
~~~
