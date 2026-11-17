The main difference between `RANK()` and `DENSE_RANK()` is **how they handle ties and gaps in numbers**:
* **`RANK()`** assigns the same rank to tied values, then **skips** the next rank number(s) equal to the number of ties (e.g., `1, 1, 3`).
* **`DENSE_RANK()`** assigns the same rank to tied values, then **does not skip** any numbers, continuing consecutively (e.g., `1, 1, 2`).

---

### Example Data & Comparison

Imagine a simple `Scores` dataset sorted in descending order:

| Name | Score | `RANK()` | `DENSE_RANK()` | `ROW_NUMBER()` (for comparison) |
| :--- | :---: | :---: | :---: | :---: |
| Alice | 90 | **1** | **1** | 1 |
| Bob | 85 | **2** | **2** | 2 |
| Charlie | 85 | **2** | **2** | 3 |
| Diana | 70 | **4** | **3** | 4 |
| Evan | 60 | **5** | **4** | 5 |

### Breakdown of What Happens

1. **At score 90 (Alice):** Both functions start at rank **1**.
2. **At score 85 (Bob and Charlie):** They tie for second place. Both functions assign them rank **2**.
3. **At score 70 (Diana):**
   * **`RANK()`** sees that two people took rank 2, so it skips rank 3 and assigns Diana **4** (leaving a gap).
   * **`DENSE_RANK()`** doesn't care about the tie count and moves straight to the next consecutive integer, assigning Diana **3** (no gap).
4. **At score 60 (Evan):** Continues as **5** for `RANK()` and **4** for `DENSE_RANK()`.

---

### SQL Syntax Example

```sql
SELECT 
    Name,
    Score,
    RANK() OVER (ORDER BY Score DESC) AS rnk,
    DENSE_RANK() OVER (ORDER BY Score DESC) AS dense_rnk
FROM Scores;
```
