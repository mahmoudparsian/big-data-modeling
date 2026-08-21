# Pros and Cons of MapReduce

## MapReduce Formalized

Let `[...]` denotes a list of objects

Then MapReduce  can be defined as:

```
map(k, v) -> [(k2, v2)]
reduce(k2, [v2]) -> [(k3, v3)]
```

# What does MapReduce Offer

MapReduce programming offers several benefits 
to help you gain valuable insights from your 
big data:

* Scalability. Businesses can process petabytes 
             of data stored in the Hadoop Distributed 
             File System (HDFS).
             
* Flexibility. Hadoop enables easier access to multiple 
             sources of data and multiple types of data.
             
* Speed. With parallel processing and minimal data movement, 
       Hadoop offers fast processing of massive amounts 
       of data.
       
* Simple. Developers can write code in a choice of languages, 
        including Java, C++ and Python.
        
* Fault Tolerance: Given that you have a replication for your
                 cluster, your MR job will complete if even
                 some of the nodes crash/die

## Pros and Cons of MapReduce

### Pros:

 1. Scalable (due to simple design)
	1.1 You can have a cluster of 10, 100, 1000, ... nodes
	1.2 Simple API: map(), combine(), reduce()
	
 2. Runs on cheap commodity hardware
 
 3. Procedural control (we can control of the execution of every step)
 
 4. Handles fault tolerance by data replication in worker nodes


### Cons:

1. It is not flexible i.e. the MapReduce framework is rigid

* There is no join operation
* There is no explicit filter API

2. It does not take advantage of memory/RAM (mostly uses disk I/O)

3. There is the only possible flow of execution: map() followed by reduce()

4. Explicit filtering is not easy (you have to implement filters 
by mappers and/or reducers)


## References

* [Advantages of Hadoop MapReduce Programming](http://web.archive.org/web/20190509134744/https://www.tutorialspoint.com/articles/advantages-of-hadoop-mapreduce-programming)
