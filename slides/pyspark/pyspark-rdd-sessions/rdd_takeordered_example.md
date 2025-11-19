# PySpark Example: RDD.takeOrdered()
# Top(10)
# Bottom(10)

-------

## 🔑 What is `takeOrdered()`?
- **`rdd.takeOrdered(n, key=...)`** returns the first **`n` elements** of the RDD **ordered by a key**.  

- Unlike `take(n)`, which just grabs the first `n` elements as they appear in partitions,  
  `takeOrdered(n)` performs a **global sort** (but optimized, not a full shuffle).  
- Default ordering = natural ascending order.  
- You can use `key=lambda x: ...` to customize sorting.

---

## ✅ Example with Sample Data
We’ll use an RDD of type **`(String, Integer)`**, e.g. employee names with their salaries.

```python

# let sc be a SparkContext object

# Sample data: (employee_name, salary)
data = [
    ("Alice", 95000),
    ("Bob", 120000),
    ("Carol", 70000),
    ("David", 105000),
    ("Eva", 60000),
    ("Frank", 85000)
]

rdd = sc.parallelize(data)
```

---

## 1️⃣ Smallest 3 salaries (ascending order by default)
```python
smallest_salaries = rdd.takeOrdered(3, key=lambda x: x[1])
print("Lowest 3 salaries:\n", smallest_salaries)
```

**Output:**

```
Lowest 3 salaries: 
[('Eva', 60000), ('Carol', 70000), ('Frank', 85000)]
```

👉 Explanation:
- We passed `key=lambda x: x[1]` → sort by salary (2nd element).  
- `takeOrdered(3, ...)` picks the **3 smallest** salaries.

---

## 2️⃣ Top 3 highest salaries
We can flip the sort order by using `-x[1]`.

```python
highest_salaries = rdd.takeOrdered(3, key=lambda x: -x[1])
print("Top 3 highest salaries:\n", highest_salaries)
```

**Output:**
```
Top 3 highest salaries: 
[('Bob', 120000), ('David', 105000), ('Alice', 95000)]
```

👉 Explanation:
- `-x[1]` sorts salaries descending.  
- We get the 3 highest-paid employees.

---

## 3️⃣ Alphabetically first 3 employees
Here we sort by name (string).

```python
alphabetical_first3 = rdd.takeOrdered(3, key=lambda x: x[0])
print("Alphabetically first 3 employees:\n", alphabetical_first3)
```

**Output:**
```
Alphabetically first 3 employees: 
[('Alice', 95000), ('Bob', 120000), ('Carol', 70000)]
```

👉 Explanation:
- `key=lambda x: x[0]` sorts by name.  
- Picks first 3 alphabetically.

---

## 🔎 Summary
- `takeOrdered(n)` is very useful when you want a **global “top N” query**.  
- Common use cases:
  - Lowest / highest values (`min`, `max`, but for N elements).  
  - Lexicographic (string) ordering.  
  - Efficient sampling with order guarantee.

---
