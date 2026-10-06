# Question 1

Assume that we have about 100,000 `gene_id`(s).

Assume we have billions of records and consider 
the following input record format:

```
<gene_id><,><gene_value_as_float>
```


Sample records:

```
g1,1.0
g1,2.4
g2,7.0
g2,-1.5
g2,3.0
g3,2.3
g1,4.0
...
```

The goal is to find average and median of gene value(s) 
for each gene_id.

Write a MapReduce solution (mapper and reducer) to 
solve this problem.

The following rules/requirements must be implemented:

* R1: 

		If a gene value is less than 0, 
		then that record is dropped
    
* Implementation of R1:


		NOTE: The requirement R1 can be implemented 
		in a map() function, since it does not depend 
		on aggregated values for a gene_id
    
* R2: 

		If a record does not have a proper format, 
		then that record is dropped

* Implementation of R2:
   
		NOTE: The requirement R2 can be implemented 
		in  a map() function, since it depends on 
		the actual value of a record

        
* R3: 

		If average of a gene_id is less than 1.5, 
		then no output is created at all for that gene_id

* Implementation of R3:

		NOTE: The requirement R3 can be implemented 
		in a reduce() function, since it depends 
		on the aggregated value of a gene_id
   

# Combiner
Can we write combiners? How? Show your work, 
and justify your answer.

Since we do need all of the values for a `gene_id`,
and the values has to be sorted for finding the median,
there is no function, which can find the medial and
be commutative and associative.

In MapReduce, a **Combiner** acts as a local mini-reducer on individual Map output nodes to reduce data traffic before the Shuffle phase. However, a function can only safely be used as a Combiner if it is **commutative and associative**.

Finding the **median** fails this requirement because it is a non-associative, non-local operation.

---

### 1. The Mathematical Reason: Non-Associativity

For a operation $f$ to be associative, grouping inputs in different subsets must yield the same result:

$$f(A, B, C) = f(f(A, B), C)$$

Median does not satisfy this property.

#### Example:

Suppose a mapper node receives six numbers for `gene_id_1`: **[1, 2, 3, 10, 20, 30]**.
The true median of the entire dataset is **$(3 + 10) / 2 = 6.5$**.

Now, suppose the local mapper splits this data across two chunks before running a Combiner:

* **Chunk A:** [1, 2, 3] $\rightarrow$ Local Median = **2**
* **Chunk B:** [10, 20, 30] $\rightarrow$ Local Median = **20**

If the Combiner runs locally on each chunk and emits `(gene_id_1, 2)` and `(gene_id_1, 20)`, the Reducer will compute the median of `[2, 20]`, which gives **11**.

Since $11 \neq 6.5$, computing local medians alters the global outcome.

---

    
   


# Mapper

```
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
```

# Sort & Shuffle output:

```
(g1, [v1, v2, ...])
(g2, [t1, t2, ...])
....
```

# Reducer

```
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
```

# Discuss the number of mappers and reducers

This depends on the volume of data and available cluster size

In MapReduce, sizing the number of **Mappers** and **Reducers** correctly is crucial for balance: too few leads to long execution times and node underutilization, while too many leads to high startup overhead, memory strain, and networking bottlenecks.

---

## 1. Map Phase Sizing

In MapReduce, the number of **Mappers** is driven primarily by **Input Data Size** and **Input Split Size**, rather than direct manual assignment or total cluster size.

### Primary Drivers

* **Input Split Size:** MapReduce divides input files into logical splits. By default, **1 Input Split = 1 HDFS Block Size** (typically 128 MB or 256 MB in modern Hadoop clusters).
* **Formula:**

$$\text{Number of Mappers} = \frac{\text{Total Input Size}}{\text{Input Split Size}}$$


* **Role of Cluster Size:** Cluster size determines how many Mappers can run **concurrently** (total Map slots / container vCPUs available across all worker nodes). If total mappers exceed cluster capacity, tasks run in sequential waves.

### Guidelines

* Aim for split sizes that allow each Mapper to run for **1 to 3 minutes**. Very short tasks waste time in container spawn overhead; overly long tasks risk high retry penalties on failure.
* For small files, use `CombineFileInputFormat` to merge small files into larger splits to avoid launching thousands of short-lived Mappers.

---

## 2. Reduce Phase Sizing

Unlike Mappers, the number of **Reducers** is set explicitly by the developer or job configuration (e.g., `job.setNumReduceTasks(N)`).

### Primary Drivers

* **Cluster Capacity:** Reducer count depends heavily on total available cluster CPU and memory slots.
* **Volume of Intermediate Data:** The total key-value pairs output by Mappers after local filtering/combining.
* **Partitioning & Data Skew:** Ensure keys distribute evenly across Reducers to prevent stragglers.

### Standard Heuristic Rules of Thumb

1. **Capacity-Based Rule:**

$$\text{Number of Reducers} \approx 0.95 \text{ to } 1.75 \times (\text{Nodes} \times \text{Maximum Reducer Containers per Node})$$


* **$0.95$ factor:** All Reducers launch immediately in a single wave as Map tasks finish, maximizing parallelism.
* **$1.75$ factor:** Faster nodes finish their first Reducer and take on a second wave, balancing load across heterogeneous hardware.


2. **Data-Volume Rule:**
* Target **1 GB to 5 GB** of intermediate output data per Reducer task.



---

## 3. Concrete Example

### Scenario Setup

* **Cluster:** 20 Worker Nodes
* **Node Specs:** Each node has capacity to run 8 Concurrent Mappers and 4 Concurrent Reducers simultaneously.
* *Total Cluster Map Capacity:* $20 \times 8 = 160$ concurrent Map containers.
* *Total Cluster Reduce Capacity:* $20 \times 4 = 80$ concurrent Reduce containers.


* **Input Data:** 500 GB single uncompressed log file set.
* **HDFS Block / Split Size:** 128 MB ($0.125\text{ GB}$).

---

### Step-by-Step Calculation

#### 1. Calculating Mappers

$$\text{Number of Mappers} = \frac{500\text{ GB}}{0.125\text{ GB}} = 4,000\text{ Mappers}$$

* **Execution Flow:**
* Total required Map tasks = $4,000$.
* Max cluster concurrency = $160$ tasks at a time.
* The cluster executes the Map phase in $\frac{4000}{160} = 25$ sequential waves of 160 tasks.



#### 2. Calculating Reducers

Using the capacity heuristic ($0.95 \times \text{Total Capacity}$):


$$\text{Reducers} = 0.95 \times (20 \text{ nodes} \times 4 \text{ reduce slots/node}) = 0.95 \times 80 = 76 \text{ Reducers}$$

* **Execution Flow:**
* All 76 Reducers start shuffling intermediate key-value partitions concurrently as Map tasks begin finishing.
* On average, each Reducer will process $\frac{\text{Total Intermediate Data}}{76}$. If intermediate output after map-side filtering is ~150 GB, each Reducer handles $\approx 2\text{ GB}$, fitting within ideal operational limits.



---
