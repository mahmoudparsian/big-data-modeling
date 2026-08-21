# PySpark Example: Using `reduceByKey()` to Find Maximum or Average Salary

## 🧩 Given
An RDD of the form `(name, salary)`:

```python
rdd = sc.parallelize([
    ("Alice", 8000),
    ("Bob", 12000),
    ("Alice", 10000),
    ("Bob", 9000),
    ("Carol", 9500),
])
```

---

## ✅ Step 1: Find Maximum Salary by Name using `reduceByKey()`

`reduceByKey()` combines values for the same key using a **reduction function** — here, we use `max()`.

```python
max_salary_by_name = rdd.reduceByKey(lambda a, b: max(a, b))
```

**Explanation:**
- For each `name`, Spark applies the `max()` function pairwise to all salaries.
- The result gives the **maximum salary per name**.

**Example Output:**
```
[
  ("Alice", 10000),
  ("Bob", 12000),
  ("Carol", 9500)
]
```

---

## ⚠️ Step 2: Drop Elements Where the Maximum Salary is More than 10,000

If you want to **remove entries where the max salary > 10,000**, apply a filter:

```python
max_salary_le_10k = max_salary_by_name.filter(lambda kv: kv[1] <= 10000)
```

**Output:**
```
[
  ("Alice", 10000),
  ("Carol", 9500)
]
```

---

## 🧠 If You Actually Meant "Average Salary"

To find the **average salary** per name and then remove entries with an average > 10,000:

```python
# Step 1: Create (sum, count) pairs per key
sum_count = rdd.combineByKey(
    lambda v: (v, 1),
    lambda acc, v: (acc[0] + v, acc[1] + 1),
    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])
)

# Step 2: Compute the average
avg_salary_by_name = sum_count.mapValues(lambda x: x[0] / x[1])

# Step 3: Filter where average <= 10,000
avg_salary_le_10k = avg_salary_by_name.filter(lambda kv: kv[1] <= 10000)
```

**Output:**
```
[
  ("Alice", 9000.0),
  ("Carol", 9500.0)
]
```

---

## ✅ Summary

| Step | Goal | Transformation | Example Output |
|------|------|----------------|----------------|
| 1 | Max salary per name | `reduceByKey(lambda a,b: max(a,b))` | ("Bob", 12000), ("Alice", 10000), ("Carol", 9500) |
| 2 | Drop salaries > 10,000 | `filter(lambda kv: kv[1] <= 10000)` | ("Alice", 10000), ("Carol", 9500) |
| 3 | (Optional) Average salary per name | `combineByKey()` + `mapValues()` | ("Alice", 9000.0), ("Carol", 9500.0) |

---

## 💡 Notes
- `reduceByKey()` is **efficient** for aggregation — it reduces data before shuffling.
- `combineByKey()` gives more flexibility (for averages, sums, etc.).
- `filter()` is element-wise and easily removes unwanted entries.
