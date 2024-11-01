# Join Operation in RDDs and MapReduce

![](https://i.ytimg.com/vi/4JLkHtIxXlA/maxresdefault.jpg)

## Introduction

1. Spark supports join operation between RDDs
2. Spark supports join operation between DataFrames
3. MapReduce paradigm does not support the join operation,
   but join operation can be implemented between two
   data sets.
   
Spark RDDs supports the following join operations

* RDD.join() as an inner-join
* RDD.leftOuterJoin()
* RDD.rightOuterJoin()
* RDD.fullOuterJoin()

## RDD Joins in Action

~~~
% ./bin/pyspark
Python 3.12.0 (v3.12.0:0fb18b02c8, Oct  2 2023, 09:45:56) 
[Clang 13.0.0 (clang-1300.0.29.30)] on darwin
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.5.3
      /_/

Using Python version 3.12.0 (v3.12.0:0fb18b02c8, Oct  2 2023 09:45:56)
Spark context Web UI available at http://172.20.193.146:4040
Spark context available as 'sc' 
(master = local[*], app id = local-1730428868917).
SparkSession available as 'spark'.
~~~

## RDD.join() as Inner-Join
~~~
>>>
>>> data1 = [('A', 2), ('A', 3), ('B', 4), ('B', 5), 
>>>          ('C', 5), ('D', 6)]
>>> 
>>> data2 = [('A', 7), ('A', 8), ('B', 20), ('B', 30), 
>>>          ('E', 8), ('F', 9)]
>>>
>>> rdd1 = sc.parallelize(data1)
>>> rdd1.collect()
[('A', 2), ('A', 3), ('B', 4), ('B', 5), ('C', 5), ('D', 6)]
>>>
>>> rdd1.count()
6
>>> rdd2 = sc.parallelize(data2)
>>> rdd2.collect()
[('A', 7), ('A', 8), ('B', 20), ('B', 30), ('E', 8), ('F', 9)]
>>> rdd2.count()
6
>>>
>>> joined = rdd1.join(rdd2)
>>> joined.collect()
[
 ('A', (2, 7)), 
 ('A', (2, 8)), 
 ('A', (3, 7)), 
 ('A', (3, 8)), 
 ('B', (4, 20)),
 ('B', (4, 30)), 
 ('B', (5, 20)), 
 ('B', (5, 30))
]
>>>
>>> joined2 = rdd2.join(rdd1)
>>> joined2.collect()
[
 ('A', (7, 2)), 
 ('A', (7, 3)), 
 ('A', (8, 2)), 
 ('A', (8, 3)), 
 ('B', (20, 4)), 
 ('B', (20, 5)), 
 ('B', (30, 4)), 
 ('B', (30, 5))
]
~~~

## RDD.leftOuterJoin()
~~~
>>>
>>> # RDD.leftOuterJoin
>>> # For each element (k, v) in self, the 
>>> # resulting RDD will either contain all 
>>> # pairs (k, (v, w)) for w in other, or
>>> # the pair (k, (v, None)) if no elements 
>>> # in other have key k.
>>>
>>> left_join = rdd1.leftOuterJoin(rdd2)
>>> left_join.collect()
[
 ('A', (2, 7)), 
 ('A', (2, 8)), 
 ('A', (3, 7)), 
 ('A', (3, 8)), 
 ('B', (4, 20)), 
 ('B', (4, 30)), 
 ('B', (5, 20)), 
 ('B', (5, 30)), 
 ('D', (6, None)), 
 ('C', (5, None))
]
>>> # left = rdd1
>>> # right = rdd2

~~~

## rightOuterJoin()
~~~
>>> right_join = rdd1.rightOuterJoin(rdd2)
>>> right_join.collect()
[
 ('A', (2, 7)), 
 ('A', (2, 8)), 
 ('A', (3, 7)), 
 ('A', (3, 8)), 
 ('B', (4, 20)), 
 ('B', (4, 30)), 
 ('B', (5, 20)), 
 ('B', (5, 30)), 
 ('E', (None, 8)), 
 ('F', (None, 9))
]
>>>
~~~

## fullOuterJoin()
~~~
>>> full_join = rdd1.fullOuterJoin(rdd2)
>>> full_join.collect()
[
 ('A', (2, 7)), 
 ('A', (2, 8)), 
 ('A', (3, 7)), 
 ('A', (3, 8)), 
 ('B', (4, 20)), 
 ('B', (4, 30)),
 ('B', (5, 20)), 
 ('B', (5, 30)),
 ('C', (5, None)),
 ('D', (6, None)),
 ('E', (None, 8)),   
 ('F', (None, 9))  
]
>>>
~~~

## Join in MapReduce paradigm:

### Step-1:  Transformation1: map() for data set 1
~~~
D1:data1     map()
========  --->
('A', 2)  -> ('A', ('D1', 2))
('A', 3)  -> ('A', ('D1', 3))
('B', 4)  -> ('B', ('D1', 4)) 
('B', 5)  -> ('B', ('D1', 5))
('C', 5)  -> ('C', ('D1', 5))
('D', 6)  -> ('D', ('D1', 6))
~~~

### Step-2:  Transformation1: map() for data set 2
~~~
D2: data2
========
('A', 7)  -> ('A', ('D2', 7)) 
('A', 8)  -> ('A', ('D2', 8))
('B', 20) -> ('B', ('D2', 20)
('B', 30) -> ('B', ('D2', 30) 
('E', 8)  -> ('E', ('D2', 8))
('F', 9)  -> ('F', ('D2', 9))
~~~

### Step-3. Transformation: combine output of all mappers into a single location (as an input)

~~~
Add all elements (output of all mappers)
from both mappers to one location:

('A', ('D1', 2))
('A', ('D1', 3))
('B', ('D1', 4)) 
('B', ('D1', 5))
('C', ('D1', 5))
('D', ('D1', 6))
('A', ('D2', 7)) 
('A', ('D2', 8))
('B', ('D2', 20))
('B', ('D2', 30))
('E', ('D2', 8))
('F', ('D2', 9))
~~~

### Step-4. Transformation: Identity Mapper
~~~
# identity mapper
map(k, v) {
   emit(k, v)
}

output of identity mapper:

('A', ('D1', 2))
('A', ('D1', 3))
('B', ('D1', 4)) 
('B', ('D1', 5))
('C', ('D1', 5))
('D', ('D1', 6))
('A', ('D2', 7)) 
('A', ('D2', 8))
('B', ('D2', 20))
('B', ('D2', 30))
('E', ('D2', 8))
('F', ('D2', 9))

~~~

### Step-5. Transformation:  Sort & Shuffle
~~~
('A', [('D1', 2), ('D1', 3), ('D2', 7), ('D2', 8)])
('B', [('D1', 4), ('D1', 5), ('D2', 20), ('D2', 30)])
('C', [('D1', 5)])
('D', [('D1', 6)])
('E', [('D2', 8)])
('F', [('D2', 9)])
~~~

### Step-6. Transformation: reducer for inner-join

~~~
# key: 'A', 'B', 'C', 'D', 'E', 'F'
# values: Iterable<(v1, v2)>
# v1: data label: 'D1' or 'D2'
# v2: actual value for key
reduce(key, values) {
    size = len(values)
    if (size < 2) {
       # NO output for join
       return
    }
    
    D1_list =[]
    D2_list =[]
    for (v in values) {
       label = v[0]
       data = v[1]
       if (label == 'D1') {
         D1_list.append(data)
       }
       else {
         D2_list.append(data)
       }
       
       # if key = 'A'
       # D1_list = [2, 3]
       # D2_list = [7, 8]
       
       # if key = 'B'
       # D1_list = [4, 5]
       # D2_list = [20, 30] 
       
       if (len(D1_list) == 0) {
           return
       }
       
       if (len(D2_list) == 0) {
           return
       }  
       
       # we know that:
       # len(D1_list) > 0
       # len(D2_list) > 0
       
       for x in D1_list {
           for y in D2_list {
               emit (key, (x, y))
           }
       } 
       
       # if key = 'A'
       # D1_list = [2, 3]
       # D2_list = [7, 8]
       # output ('A', (2, 7)), ('A', (2, 8)), ('A', (3, 7)), ('A', (3, 8))

       # if key = 'B'
       # D1_list = [4, 5]
       # D2_list = [20, 30]  
       # output ('B', (4, 20)), ('B', (4, 30)), ('B', (5, 20)), ('B', (5, 30))
    
    }#end-for
}#end-reduce
~~~

output of reduce():

~~~
 ('A', (2, 7)), 
 ('A', (2, 8)), 
 ('A', (3, 7)), 
 ('A', (3, 8)), 
 ('B', (4, 20)),
 ('B', (4, 30)), 
 ('B', (5, 20)), 
 ('B', (5, 30))
~~~


## Homework:

1. implement leftOuterJoin in MapReduce paradigm
2. implement rightOuterJoin in MapReduce paradigm
3. implement fullOuterJoin in MapReduce paradigm


