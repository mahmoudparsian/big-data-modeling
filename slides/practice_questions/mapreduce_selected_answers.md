Question 1

Assume that we have about 100,000 gene_id(s).

Assume we have billions of records and consider 
the following input record format:

<gene_id><,><gene_value_as_float>

Sample records:

g1,1.0
g1,2.4
g2,7.0
g2,-1.5
g2,3.0
g3,2.3
g1,4.0
...

The goal is to find average and median of gene value(s) 
for each gene_id.

Write a MapReduce solution (mapper and reducer) to 
solve this problem.

The following rules must be implemented:

    R1: If a gene value is less than 0, 
    then that record is dropped
    
    R2: If a record does not have a proper format, 
    then that record is dropped
    
    R3: If average of a gene_id is less than 1.5, 
    then no output is created at all for that gene_id
    
    Can we write combiners? How? Show your work, 
    and justify your answer.
    
    Discuss the number of mappers and reducers

# key: record number and ignored
# value: actual input record
map(key, value) {
    if len(value) < 1 {
       return
    }
    
    tokens = value.split(",")
    # R2
    if len(tokens) != 2 {
       return 
    }
    
    gene_id = tokens[0]
    gene_value = double(tokens[1])
    
    # R1
    if (gene_value < 0.00) {
       return 
    }

    emit (gene_id, gene_value)
}


Sort & Shuffle:
(g1, [v1, v2, ...])
(g2, [t1, t2, ...])
....

#key: gene_id as a string
# values: Iterable<double>
reduce (key, values) {

   total = SUM(values)
   count = SIZE(values)
   avg = total / count
   
   # R3
   if (avg < 1.5) {
      return
   }

   # here avg >= 1.5
   median = median_function(values)
   
   emit (gene_id, (avg, median))
}

}

