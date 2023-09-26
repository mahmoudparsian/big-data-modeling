# Pros and Cons of MapReduce

## Pros:
     1. Scalable (due to simple design)
        1.1 You can have a cluster of 10, 100, 1000, ... nodes
        1.2 Simple API: map(), combine(), reduce()
        
     2. Runs on cheap commodity hardware
     
     3. Procedural control (we can control of the execution of every step)
     
     4. Handles fault tolerance by data replication in worker nodes

## Cons:
     1. It is not flexible i.e. the MapReduce framework is rigid
        1.1 There is no join operation
        1.2 There is no explicit filter API
        
     2. It does not take advantage of memory/RAM (mostly uses disk I/O)
     
     3. There is the only possible flow of execution: map() followed by reduce()
     
     4. Explicit filtering is not easy (you have to implement filters 
        by mappers and/or reducers)
     