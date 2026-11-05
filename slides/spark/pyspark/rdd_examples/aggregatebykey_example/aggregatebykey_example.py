"""
## Scenario: Customer E-Commerce Analytics

Suppose you have an incoming stream/log of transactions represented as `(customer_id, order_amount)`. 

For every customer, we want to compute three metrics simultaneously:
1. **Total Spent**
2. **Total Order Count** (to calculate Average Order Value)
3. **Maximum Single Purchase**

Here, input values are single numbers (`float`), 
but our accumulator structure needs to be a 3-element tuple: 
`(total_amount, count, max_amount)`.
"""

## Code Implementation

from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext.getOrCreate()

# 1. Sample Data: (customer_id, order_amount)
transactions = sc.parallelize([
    ("cust_101", 150.0),
    ("cust_102", 45.0),
    ("cust_101", 200.0),
    ("cust_103", 89.0),
    ("cust_102", 120.0),
    ("cust_101", 50.0),
    ("cust_103", 310.0),
])

print("transactions.count()=", transactions.count())
print("transactions.collect()=", transactions.collect())

# 2. Zero Value (Initial state for each key on each partition)
# Pattern: (accumulated_total, count, max_single_order)
zero_value = (0.0, 0, 0.0)

# 3. Sequence Operation (seqOp)
# Combines an incoming value (float) with 
# the current accumulator tuple WITHIN 
# the same partition.
def seq_op(accumulator, current_value):
    total, count, current_max = accumulator
    new_total = total + current_value
    new_count = count + 1
    new_max = max(current_max, current_value)
    return (new_total, new_count, new_max)

# 4. Combine Operation (combOp)
# Merges two accumulator tuples from 
# DIFFERENT partitions during the shuffle step.
def comb_op(acc1, acc2):
    total1, count1, max1 = acc1
    total2, count2, max2 = acc2
    return (total1 + total2, count1 + count2, max(max1, max2))

# 5. Execute aggregateByKey
aggregated_rdd = transactions.aggregateByKey(
    zeroValue=zero_value, 
    seqFunc=seq_op, 
    combFunc=comb_op
)
print("aggregated_rdd.count()=", aggregated_rdd.count())
print("aggregated_rdd.collect()=", aggregated_rdd.collect())


# 6. Post-processing: Calculate final Average Order Value (AOV)
final_result = aggregated_rdd.mapValues(
    lambda acc: {
        "avg_order_value": round(acc[0] / acc[1], 2),
        "total_orders": acc[1],
        "max_order_value": acc[2]
    }
)

# Output results
for customer, stats in final_result.collect():
    print(f"{customer}: {stats}")
