# The Top-10 Design Pattern <br> in MapReduce

The **Top-10 design pattern** in MapReduce is an 
optimized algorithmic pattern used to find the 10 
largest or smallest records in a massive dataset 
without performing a full, expensive global sort. 

Instead of passing all data through the network, 
this pattern filters records aggressively at the 
source by utilizing **in-memory bounded data structures** 
inside both the Mappers and the Reducers.

---

### Algorithmic Workflow

| Phase | Core Responsibility | Data Structure & Logic |
| :--- | :--- | :--- |
| **1. Map** | Computes a **local Top-10** for each individual data split. | Processes input records one by one, inserting them into a local min-heap or `TreeMap` bounded at size 10. |
| **2. Cleanup (Mapper)** | Emits only the **10 local winners** to the Reducer. | Once a data split is completely read, the `cleanup()` method runs *once* to emit the 10 remaining elements to a single reducer. |
| **3. Reduce** | Computes the **global Top-10**. | A single reducer collects the 10 local winners from *every* mapper, populates its own bounded heap of size 10, and outputs the final true Top-10. |

---

### Why It's Efficient: Avoiding the Network Bottleneck

A naive approach would emit every single key-value pair from the Mappers, letting Hadoop's underlying **Shuffle and Sort** phase sort the entire dataset before the Reducer picks the first 10. For a terabyte-scale dataset, this creates massive network congestion and disk I/O bottlenecks. 

By filtering down to a maximum of 10 records per Mapper *before* sending anything over the network, network traffic drops by over **99.99%**, making the execution highly scalable.

### Pseudocode Blueprint

```python
class Top10Mapper:
    def setup(self):
        # Initialize a min-heap bounded to 10 elements
        # this method is run once per partition before any mapping
        self.local_top10 = MinHeap(max_size=10)

    def map(self, key, record):
        # Compute the scoring metric (e.g., views, sales)
        # runs once for every record of partition
        score = record.get_score()
        self.local_top10.insert((score, record))
        
        # If heap exceeds 10 elements, drop the smallest element
        if len(self.local_top10) > 10:
            self.local_top10.pop_min()

    def cleanup(self):
        # Emit only the 10 local winners to the reducer
        # this method is run once after all mappings are done
        for score, record in self.local_top10:
            emit(NullWritable.get(), record)

class Top10Reducer:
    def setup(self):
        self.global_top10 = MinHeap(max_size=10)

    def reduce(self, key, records):
        # Combine local winners from all mappers
        for record in records:
            score = record.get_score()
            self.global_top10.insert((score, record))
            if len(self.global_top10) > 10:
                self.global_top10.pop_min()

    def cleanup(self):
        # Output the final global top 10 records
        for score, record in self.global_top10.sort_descending():
            emit(score, record)
```

For specific implementations, engineers typically use a Java `TreeMap` or custom heap structures within the Hadoop framework.

# Worked Example: Top-3

Here is an expanded, end-to-end trace of the 
Top-3 Design Pattern using a larger dataset 
distributed across 3 independent Mappers.

## The Expanded Dataset
We want to track the Top-3 most viewed items 
on an e-commerce site. The raw data is distributed 
into 3 parallel server blocks.

* Mapper 1 Data:
	* {"item": "laptop", "views": 500}
   * {"item": "mouse", "views": 150}
   * {"item": "keyboard", "views": 300}
   * {"item": "monitor", "views": 850}
   * {"item": "desk", "views": 400}
* Mapper 2 Data:
	* {"item": "phone", "views": 950}
   * {"item": "case", "views": 200}
   * {"item": "charger", "views": 350}
   * {"item": "headphones", "views": 700}
   * {"item": "watch", "views": 600}
* Mapper 3 Data:
	* {"item": "tablet", "views": 650}
   * {"item": "stylus", "views": 100}
   * {"item": "stand", "views": 250}
   * {"item": "speaker", "views": 900}
   * {"item": "cable", "views": 120}

---

## Step 1: The Map Phase <br> (Parallel Local Filtering)
Each Mapper runs concurrently on its own server. 
It streams records one by one into an internal 
Min-Heap bounded at a maximum size of 3. Whenever 
the size hits 4, the item with the lowest score is 
immediately popped and evicted from memory.

## Mapper 1 Execution

   1. Read laptop (500) $\rightarrow$ Heap: [(500, laptop)]
   2. Read mouse (150) $\rightarrow$ Heap: [(150, mouse), (500, laptop)]
   3. Read keyboard (300) $\rightarrow$ Heap grows to 3: [(150, mouse), (300, keyboard), (500, laptop)]
   4. Read monitor (850) $\rightarrow$ Heap grows to 4: [(150, mouse), (300, keyboard), (500, laptop), (850, monitor)]
   * Eviction: Size > 3. Evict minimum entry (150, mouse).
      * Heap: [(300, keyboard), (500, laptop), (850, monitor)]
   5. Read desk (400) $\rightarrow$ Heap grows to 4: [(300, keyboard), (400, desk), (500, laptop), (850, monitor)]
   * Eviction: Size > 3. Evict minimum entry (300, keyboard).
      * Final Local Top-3 for Mapper 1: [(400, desk), (500, laptop), (850, monitor)]
   
## Mapper 2 Execution

   1. Read phone (950) $\rightarrow$ Heap: [(950, phone)]
   2. Read case (200) $\rightarrow$ Heap: [(200, case), (950, phone)]
   3. Read charger (350) $\rightarrow$ Heap: [(200, case), (350, charger), (950, phone)] 
   4. Read headphones (700) $\rightarrow$ Heap grows to 4: [(200, case), (350, charger), (700, headphones), (950, phone)]
   * Eviction: Size > 3. Evict minimum entry (200, case).
      * Heap: [(350, charger), (700, headphones), (950, phone)]
   5. Read watch (600) $\rightarrow$ Heap grows to 4: [(350, charger), (600, watch), (700, headphones), (950, phone)]
   * Eviction: Size > 3. Evict minimum entry (350, charger).
      * Final Local Top-3 for Mapper 2: [(600, watch), (700, headphones), (950, phone)]
   
## Mapper 3 Execution

   1. Read tablet (650) $\rightarrow$ Heap: [(650, tablet)]
   2. Read stylus (100) $\rightarrow$ Heap: [(100, stylus), (650, tablet)]
   3. Read stand (250) $\rightarrow$ Heap: [(100, stylus), (250, stand), (650, tablet)]
   4. Read speaker (900) $\rightarrow$ Heap grows to 4: [(100, stylus), (250, stand), (650, tablet), (900, speaker)]
   * Eviction: Size > 3. Evict minimum entry (100, stylus).
      * Heap: [(250, stand), (650, tablet), (900, speaker)]
   5. Read cable (120) $\rightarrow$ Heap grows to 4: [(120, cable), (250, stand), (650, tablet), (900, speaker)]
   * Eviction: Size > 3. Evict incoming entry (120, cable) since it is smaller than the current minimum (250, stand).
      * Final Local Top-3 for Mapper 3: [(250, stand), (650, tablet), (900, speaker)]
   
---

## Step 2: The Network Shuffle Phase
When processing finishes, the `cleanup()` method of 
each Mapper runs exactly once. Instead of routing all 
15 raw records across the network cluster, they 
serialize and emit only their final internal heaps 
to a single Reducer.

* Mapper 1 transmits: 

```
(Null, [400, desk]), 
(Null, [500, laptop]), 
(Null, [850, monitor])
```

* Mapper 2 transmits: 

```
(Null, [600, watch]), 
(Null, [700, headphones]), 
(Null, [950, phone])
```

* Mapper 3 transmits: 

```
(Null, [250, stand]), 
(Null, [650, tablet]), 
(Null, [900, speaker])
```

---

## Step 3: The Reduce Phase <br> (Global Consolidation)

A single centralized Reducer gathers the 9 total 
candidate records streaming across the network 
and handles them sequentially using its own global 
bounded Min-Heap of size 3.

   1. Receive (400, desk) $\rightarrow$ Heap: [(400, desk)]
   2. Receive (500, laptop) $\rightarrow$ Heap: [(400, desk), (500, laptop)]
   3. Receive (850, monitor) $\rightarrow$ Heap grows to 3: [(400, desk), (500, laptop), (850, monitor)]
   4. Receive (600, watch) $\rightarrow$ Heap grows to 4: [(400, desk), (500, laptop), (600, watch), (850, monitor)]
   * Eviction: Size > 3. Evict minimum entry (400, desk).
      * Heap: [(500, laptop), (600, watch), (850, monitor)]
   5. Receive (700, headphones) $\rightarrow$ Heap grows to 4: [(500, laptop), (600, watch), (700, headphones), (850, monitor)]
   * Eviction: Size > 3. Evict minimum entry (500, laptop).
      * Heap: [(600, watch), (700, headphones), (850, monitor)]
   6. Receive (950, phone) $\rightarrow$ Heap grows to 4: [(600, watch), (700, headphones), (850, monitor), (950, phone)]
   * Eviction: Size > 3. Evict minimum entry (600, watch).
      * Heap: [(700, headphones), (850, monitor), (950, phone)]
   7. Receive (250, stand) $\rightarrow$ Since 250 is less than the current heap minimum (700), it is ignored.
   8. Receive (650, tablet) $\rightarrow$ Since 650 is less than the current heap minimum (700), it is ignored.
   9. Receive (900, speaker) $\rightarrow$ Heap grows to 4: [(700, headphones), (850, monitor), (900, speaker), (950, phone)]
   * Eviction: Size > 3. Evict minimum entry (700, headphones).
      * Final Global Heap State: [(850, monitor), (900, speaker), (950, phone)]
   
---

## Step 4: Final Sorting & Output
During its final `cleanup()` loop, the Reducer 
extracts the remaining 3 records from the Min-Heap, 
reverses them to sort in descending order, and 
saves the final result.

```
Rank 1: phone    - 950 views
Rank 2: speaker  - 900 views
Rank 3: monitor  - 850 views
```

