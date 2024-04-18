# MapReduce Examples

* Solution-1: with map() and reduce()

* Solution-2: with map(), combine(), and reduce()


# Input

~~~text
<gene_id><,><gene_value>
~~~

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

# Expected Output

Find `(avg, min, max)` per gene.


~~~text
(gene_id, (avg, min, max))
~~~

# Solution-1: map() and reduce()

## Solution-1: Mapper function

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

## Solution-1: Sort & Shuffle OUTPUT:

~~~text
(gend_id_1, [a1, a2, a3, ...])
(gend_id_2, [b1, b2, b3, ...])
...
(gene_id_100000, [v1, v2, ...])
~~~

## Solution-1: Reducer SHORTER VERSION

~~~text
# key is a gene_id
# values : Iterable<Double>
reduce(key, values){ 
  total = sum(values)
  count = len(values)
  avg = total / count
  minimum = min(values)
  maximum = max(values)
  emit (key, (avg, minimum, minimum))
}
~~~


# Solution-2: map(), combine(), and reduce()

## Solution-2: Mapper function

~~~text
# key is a record number and ignored
# value is the entire record of input
map(key, value){ 
  # tokenize the given input record
  tokens = value.split(",")
  gene_id = tokens[0]
  gene_value = tokens[1]
  
  # generate output from mapper
  #                  sum     count min         max
  emit(gene_id, (gene_value, 1,    gene_value, gene_value))
}
~~~

## Solution-2: Sort & Shuffle OUTPUT:

~~~text
(gend_id_1, [
             (sum1, count1, min1, max1), 
             (sum2, count2, min2, max2), 
             ...
            ]
)
...
~~~

## Solution-2: Combiner function

~~~text
# key is a gene_id
# values : Iterable<(Double, Integer, Double, Double)>
# values : Iterable<(sum, count, min, max)>
# example: values: [(1.1, 1, 1.1, 1.1), 
#                   (1.7, 1, 1.7, 1.7), 
#                   (2.1, 1, 2.1, 2.1), ...]
combine(key, values){ 
  sum = 0
  count = 0
  minimum = NULL 
  maximum = NULL
  FIRST_TIME = True
  for p in values {
     # p = (p[0], p[1], p[2], p[3])
     # p = (sum, count, min, max)
     sum += p[0]
     count += p[1]
     if (FIRST_TIME) {
       minimum = p[2] 
       maximum = p[3]
       FIRST_TIME = False
     }
     else {
        minimum = min(minimum, p[2])
        maximum = max(maximum, p[3])
     }
  }
  #
  emit (key, (sum, count, minimum, maximum))
}
~~~

## Solution-2: Reducer function

~~~text
# key is a gene_id
# values : Iterable<(Double, Integer, Double, Double)>
# values : Iterable<(sum, count, min, max)>
# example: values: [(4.1, 2, 2.0, 2.1), 
#                   (1.7, 4, 0.5, 1.0), 
#                   (9.1, 7, 1.1, 4.1), 
#                   ...]
reduce(key, values){ 
  sum = 0
  count = 0
  minimum = NULL 
  maximum = NULL
  FIRST_TIME = True
  for p in values {
     # p = (p[0], p[1], p[2], p[3])
     # p = (sum, count, min, max)
     sum += p[0]
     count += p[1]
     if (FIRST_TIME) {
       minimum = p[2] 
       maximum = p[3]
       FIRST_TIME = False
     }
     else {
        minimum = min(minimum, p[2])
        maximum = max(maximum, p[3])
     }
  }
  # find average
  avg = sum / count
  emit (key, (avg, minimum, maximum))
}
~~~
