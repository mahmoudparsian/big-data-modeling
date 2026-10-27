# A Real-Life Case for `combineByKey()`: Building Customer Profiles

## 1. The scenario

An online coffee shop keeps a log of every order. The marketing team asks:

> "For every customer, give us a **profile**: how many orders they placed, how
> much they spent in total, the date of their first and last order, and their
> **favorite product** (the one they bought most often)."

They will use it to send "We miss you!" emails to customers whose last order
was long ago, and coupons for each customer's favorite product.

Each order is a `(customer, (date, product, amount))` pair. The log is **not**
sorted by date (orders arrive from many web servers), and one customer's
orders are spread over many partitions.

For example, here are **all four** of `alice`'s orders in the log (in the
order they arrive, which is not date order), and the **one** profile record
we want for her:

| Input: one record per **order** | Output: one record per **customer** |
|---|---|
| `('alice', ('2026-01-05', 'coffee', 12.0))` | `('alice', (4, 63.0, '2026-01-05', '2026-03-15', 'coffee'))` |
| `('alice', ('2026-02-11', 'mug', 15.0))` | |
| `('alice', ('2026-01-20', 'coffee', 12.0))` | |
| `('alice', ('2026-03-15', 'coffee', 24.0))` | |

Each field of the output comes from all four of `alice`'s orders:

| Output field | Value | How it is computed from `alice`'s 4 orders |
|---|---|---|
| number of orders | `4` | count the orders: 4 |
| total spent | `63.0` | add the amounts: `12.0 + 15.0 + 12.0 + 24.0 = 63.0` |
| first order | `'2026-01-05'` | the **earliest** date: `min('2026-01-05', '2026-02-11', '2026-01-20', '2026-03-15')` |
| last order | `'2026-03-15'` | the **latest** date: `max(...)` of the same four dates |
| favorite product | `'coffee'` | the product bought most often: `coffee` 3 times, `mug` once |

So many input records (one per order) are **reduced** to one output record
(one per customer). That is a job for a reducer by key, with the customer
name as the key. The full data set in section 3 has two more customers
(`bob` and `carol`), and each one gets a profile record the same way.

Notice the two different types:

* the **value** `V` is **one order**: `(date, product, amount)`
* the **result** `C` is a **customer profile**: orders, total, first date,
  last date, and how many times each product was bought

`V` and `C` are completely different things. This is exactly what
`combineByKey()` was designed for.


## 2. How a person would build a profile

Ask anyone in the marketing team how to build a profile by hand, from a pile
of order slips, and they will describe three steps:

1. **Start a profile** from a customer's first order slip:
   1 order, total = its amount, first date = last date = its date, and that
   product was bought once.
2. **Add an order** to an existing profile: one more order, add the amount,
   maybe move the first or last date, and count the product once more.
3. If two people each built part of the pile, **merge their two profiles**
   of the same customer into one.

These three steps are **exactly** the three functions of `combineByKey()`:

| Business rule | `combineByKey()` function | Type |
|---|---|---|
| 1. Start a profile from one order | `createCombiner` | `V → C` |
| 2. Add one order to a profile | `mergeValue` | `(C, V) → C` |
| 3. Merge two partial profiles | `mergeCombiners` | `(C, C) → C` |

So the code is a direct translation of the business rules, as the next
section shows.


## 3. Sample data

Start the PySpark shell (`./bin/pyspark`). We use **2 partitions** so the
output is the same on every machine:

~~~python
>>> sc.version
'4.2.0'

>>> orders = [
...     ('alice', ('2026-01-05', 'coffee',  12.0)),
...     ('bob',   ('2026-01-07', 'tea',      8.0)),
...     ('alice', ('2026-02-11', 'mug',     15.0)),
...     ('alice', ('2026-01-20', 'coffee',  12.0)),
...     ('bob',   ('2026-03-02', 'tea',      8.0)),
...     ('alice', ('2026-03-15', 'coffee',  24.0)),
...     ('carol', ('2026-02-28', 'teapot',  30.0)),
...     ('bob',   ('2026-02-14', 'cookies',  5.0)),
... ]

>>> # orders_rdd : RDD[(customer, (date, product, amount))]
>>> orders_rdd = sc.parallelize(orders, 2)
>>> for i, part in enumerate(orders_rdd.glom().collect()):
...     print("partition", i)
...     for order in part:
...         print("   ", order)
...
partition 0
    ('alice', ('2026-01-05', 'coffee', 12.0))
    ('bob', ('2026-01-07', 'tea', 8.0))
    ('alice', ('2026-02-11', 'mug', 15.0))
    ('alice', ('2026-01-20', 'coffee', 12.0))
partition 1
    ('bob', ('2026-03-02', 'tea', 8.0))
    ('alice', ('2026-03-15', 'coffee', 24.0))
    ('carol', ('2026-02-28', 'teapot', 30.0))
    ('bob', ('2026-02-14', 'cookies', 5.0))
~~~

`alice` and `bob` have orders in **both** partitions; `carol` only in one.
Dates are ISO strings (`'YYYY-MM-DD'`), so `min()` and `max()` on the strings
give the earliest and latest date.


## 4. The `combineByKey()` solution

A profile `C` is a Python dictionary:

```python
{'orders': 4, 'total': 63.0, 'first': '2026-01-05', 'last': '2026-03-15',
 'products': {'coffee': 3, 'mug': 1}}
```

Each function below is one business rule from section 2:

~~~python
>>> # Rule 1: start a profile from a customer's first order (V -> C)
>>> def start_profile(order):
...     date, product, amount = order
...     return {'orders': 1, 'total': amount,
...             'first': date, 'last': date,
...             'products': {product: 1}}
...

>>> # Rule 2: add one more order to a profile ((C, V) -> C)
>>> def add_order(profile, order):
...     date, product, amount = order
...     profile['orders'] += 1
...     profile['total'] += amount
...     profile['first'] = min(profile['first'], date)
...     profile['last'] = max(profile['last'], date)
...     profile['products'][product] = profile['products'].get(product, 0) + 1
...     return profile
...

>>> # Rule 3: merge two partial profiles of the same customer ((C, C) -> C)
>>> def merge_profiles(p1, p2):
...     p1['orders'] += p2['orders']
...     p1['total'] += p2['total']
...     p1['first'] = min(p1['first'], p2['first'])
...     p1['last'] = max(p1['last'], p2['last'])
...     for product, n in p2['products'].items():
...         p1['products'][product] = p1['products'].get(product, 0) + n
...     return p1
...

>>> # profiles : RDD[(customer, profile)]
>>> profiles = orders_rdd.combineByKey(start_profile, add_order, merge_profiles)
~~~

That's the whole aggregation. Now turn each profile into the report the
marketing team asked for. The favorite product is the one with the highest
count (ties are broken by product name, so the answer is always the same):

~~~python
>>> def favorite(products):
...     return min(products, key=lambda p: (-products[p], p))
...
>>> def report(profile):
...     return (profile['orders'], profile['total'],
...             profile['first'], profile['last'],
...             favorite(profile['products']))
...

>>> # (orders, total, first order, last order, favorite product)
>>> for customer, row in sorted(profiles.mapValues(report).collect()):
...     print(customer, row)
...
alice (4, 63.0, '2026-01-05', '2026-03-15', 'coffee')
bob (3, 21.0, '2026-01-07', '2026-03-02', 'tea')
carol (1, 30.0, '2026-02-28', '2026-02-28', 'teapot')
~~~

And here is the "We miss you!" list: customers with no order since
March 1st:

~~~python
>>> sorted(profiles.filter(lambda kv: kv[1]['last'] < '2026-03-01').keys().collect())
['carol']
~~~


## 5. Step by step, for `alice`

`alice` has 3 orders in partition 0 and 1 order in partition 1:

| Step | Partition 0 | Partition 1 |
|---|---|---|
| `start_profile` (first order) | `('2026-01-05', 'coffee', 12.0)` → 1 order, 12.0, first = last = `01-05`, `{coffee: 1}` | `('2026-03-15', 'coffee', 24.0)` → 1 order, 24.0, first = last = `03-15`, `{coffee: 1}` |
| `add_order` | `+ mug 15.0` → 2 orders, 27.0, last = `02-11`, `{coffee: 1, mug: 1}` | (no more orders) |
| `add_order` | `+ coffee 12.0` (on `01-20`) → 3 orders, 39.0, first and last stay `01-05` / `02-11`, `{coffee: 2, mug: 1}` | |
| `merge_profiles` (after the shuffle) | 4 orders, 63.0, first = `01-05`, last = `03-15`, `{coffee: 3, mug: 1}` | |

The order dated `2026-01-20` arrives **after** the one dated `2026-02-11`, so
`add_order` must use `min()`/`max()`, not "replace the last date". Real logs
are rarely sorted.


## 6. Can't we just use `map()` + `reduceByKey()`?

We can, but look at what it forces us to do. `reduceByKey(f)` needs
`f: (C, C) → C`, so its input must **already be profiles**. An order is not a
profile, so first we must turn **every** order into a "profile of a customer
who placed exactly one order":

~~~python
>>> one_order_profiles = orders_rdd.mapValues(start_profile)
>>> one_order_profiles.first()
('alice', {'orders': 1, 'total': 12.0, 'first': '2026-01-05', 'last': '2026-01-05', 'products': {'coffee': 1}})

>>> profiles2 = one_order_profiles.reduceByKey(merge_profiles)
>>> sorted(profiles2.mapValues(report).collect()) == sorted(profiles.mapValues(report).collect())
True
~~~

The answer is the same, but:

1. **There is no place for rule 2 ("add an order").** The most natural
   business rule disappears. Every order is first dressed up as a fake
   one-order customer profile, and then profiles are merged with profiles.
   That is not how anyone thinks about the problem.
2. **The types tell a confusing story.** After `mapValues()`, the RDD holds
   8 "profiles" for 3 customers. Each one describes a customer as if they
   placed only one order. A student reading the code has to understand that
   these are not real profiles, just a trick to satisfy `reduceByKey()`.
3. **It only works because we already wrote `start_profile` and
   `merge_profiles`.** These are two of the three `combineByKey()` functions
   anyway. `map()` + `reduceByKey()` doesn't save any work; it just hides
   the step that matters.
4. **It wastes memory.** One profile dictionary (with its own inner
   `products` dictionary) is built for **every order**. A billion orders
   means a billion temporary dictionaries. `combineByKey()` builds only one
   profile per customer per partition.

In short: when the **value** (one order) and the **result** (a customer
profile) are different things, `combineByKey()` lets you write the code the
way you would explain the problem: *start* a profile, *add* an order,
*merge* two profiles.


## 7. And `aggregateByKey()`?

`aggregateByKey(zeroValue, seqFunc, combFunc)` also accepts `V ≠ C`, and its
`seqFunc` is our `add_order`. But it needs a `zeroValue`: an **empty**
profile to start from. What should its `'first'` and `'last'` dates be?

```python
# we would have to invent fake dates that are "later than everything"
# and "earlier than everything", just so that min() and max() still work:
zero = {'orders': 0, 'total': 0.0, 'first': '9999-12-31', 'last': '0000-01-01',
        'products': {}}
```

A profile with 0 orders and a first order in the year 9999 makes no sense in
the business. With `combineByKey()` we never need it: the customer's **first
real order** starts the profile. In general, whenever the result has a
"first", "last", "minimum", or "maximum" that has no sensible empty value,
`combineByKey()` is the cleaner choice.


## 8. Takeaways

| Use ... | When ... |
|---|---|
| `reduceByKey()` | The value and the result are the same kind of thing (a count, a sum, a maximum). |
| `aggregateByKey()` | The result is different from the value, **and** there is a natural empty result to start from (like `(0, 0)` for `(sum, count)`). |
| `combineByKey()` | The result is different from the value, and the natural way to describe the job is: *start* a result from the first value, *add* one value, *merge* two results. This works even if there is no sensible empty result. |

Other real-life jobs with the same shape as the customer profile:

* **Patient summary** from hospital visits: number of visits, first and last
  visit, most frequent diagnosis.
* **Delivery truck summary** from GPS pings: first and last ping of the day,
  number of stops, distinct cities visited.
* **Student summary** from exam records: courses taken, best and worst
  grade, first and last term enrolled.

Two rules to remember:

* `add_order` and `merge_profiles` may **change and return their first
  argument** (the PySpark docs allow this, to avoid building new objects),
  but `start_profile` must always build a **new** profile.
* Spark combines values and profiles in **any order**, so the functions must
  give the same answer in every order. That's why we use `min()`/`max()` for
  dates instead of "the latest order seen so far".

For the opposite case, where the result **does** have a natural empty value
and `aggregateByKey()` is the better fit, see
[Star Ratings on Product Pages](aggregatebykey_product_ratings.md).

For the basics of all four reducers on a simpler problem, see
[Find Sum, Count, and Average Per Key](find_sum_count_avg_per_key.md).
