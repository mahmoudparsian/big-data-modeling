# MapReduce Examples

Worked MapReduce examples:

* word count (basic) and sales revenue by region/category (intermediate), traced phase by phase
* average temperature per city
* total sales, order count, and largest order per store
* classic word count
* palindromes
* finding (mutual) friends
* people you may know

## Contents

| Name | Type | Description |
|---|---|---|
| [`MapReduce_2_Examples.md`](MapReduce_2_Examples.md) | md | MapReduce 2 Worked Examples |
| [`MapReduce_Find_Average_Temperature.md`](MapReduce_Find_Average_Temperature.md) | md | MapReduce Example: Average Temperature per City |
| [`MapReduce_Finding_Friends.html`](MapReduce_Finding_Friends.html) | html | Worked example: finding common/mutual friends with MapReduce (saved copy of an external blog post) |
| [`MapReduce_Finding_Friends.pdf`](MapReduce_Finding_Friends.pdf) | pdf | Same "Finding Friends" example, PDF export |
| [`MapReduce_People_You_May_Know.md`](MapReduce_People_You_May_Know.md) | md | Companion to Finding Friends: mutual friends for pairs who are *not* already friends (PYMK-style recommendation) — runnable pure-Python/PySpark implementations at [`slides/spark/pyspark/people_you_may_know/`](../../spark/pyspark/people_you_may_know/README.md) |
| [`MapReduce_Total_Sales_per_Store.md`](MapReduce_Total_Sales_per_Store.md) | md | Complete map/combine/reduce worked example: total revenue, order count, and largest order per store, traced with and without a combiner over two mapper partitions |
| [`MapReduce_Word_Count.md`](MapReduce_Word_Count.md) | md | Classic Word Count |
| [`MapReduce_of_Palindromes.md`](MapReduce_of_Palindromes.md) | md | MapReduce of finding Palindromes |

---

## Classic MapReduce: 10 Examples

The MapReduce paradigm is best suited for large-scale, batch-processing
problems where data can be split into independent chunks, processed in
parallel, and combined based on a shared key.

Unlike the fully worked, hand-traced examples listed above, the 10
problems below are a quick-reference survey of the *shapes* of problem
that fit the MapReduce model well — each with just its map/reduce
key-value contract, not a full sample-data walkthrough. Examples 6–10
name specific companies only to make the use case concrete; they are
illustrative scenarios inspired by publicly known use cases, not
descriptions of any company's actual production pipeline.

Each entry follows the same `Map: input → (K, V)` / `Reduce: (K, list(V)) → (K, V')`
shape — only the key/value types and the aggregation logic change:

1. [Inverted Index Construction](#1-inverted-index-construction)
2. [Word Count and Frequency Analysis](#2-word-count-and-frequency-analysis)
3. [Distributed Grep (Pattern Matching)](#3-distributed-grep-pattern-matching)
4. [URL Access Frequency / Log Aggregation](#4-url-access-frequency--log-aggregation)
5. [Social Network Graph Analysis (Common Friends)](#5-social-network-graph-analysis-common-friends)
6. [Ride-Sharing: Trip Data Aggregation for Surge Pricing & Taxes](#6-ride-sharing-trip-data-aggregation-for-surge-pricing--taxes)
7. [Streaming Video: User Engagement Batch Analysis for Content Recommendations](#7-streaming-video-user-engagement-batch-analysis-for-content-recommendations)
8. [Credit Card Networks: Monthly Statement Generation & Fraud Audits](#8-credit-card-networks-monthly-statement-generation--fraud-audits)
9. [E-Commerce Platforms: Daily Inventory and Price Synchronization](#9-e-commerce-platforms-daily-inventory-and-price-synchronization)
10. [Cybersecurity Systems: Distributed IP Blacklist Generation](#10-cybersecurity-systems-distributed-ip-blacklist-generation)

### 1. Inverted Index Construction

Used by search engines to map words to the documents they appear in.

| Phase | What it does | Emits |
|---|---|---|
| **Map** | Parses a document, emitting each word as a key with the document ID as the value | `(word, doc_ID)` |
| **Reduce** | Groups all document IDs by word to generate the final index | `(word, list(doc_ID))` |

### 2. Word Count and Frequency Analysis

The classic MapReduce benchmark used for analyzing logs or text
repositories. See [`MapReduce_Word_Count.md`](MapReduce_Word_Count.md)
and Part 1 of [`MapReduce_2_Examples.md`](MapReduce_2_Examples.md) for
this same example traced mapper-by-mapper, reducer-by-reducer.

| Phase | What it does | Emits |
|---|---|---|
| **Map** | Processes lines of text and emits each encountered word with a count of one | `(word, 1)` |
| **Reduce** | Sums the counts for identical words to produce total frequencies | `(word, total_count)` |

### 3. Distributed Grep (Pattern Matching)

Used to search for specific text patterns or error codes across massive
server log files.

| Phase | What it does | Emits |
|---|---|---|
| **Map** | Evaluates each line against a regular expression; if it matches, emits the pattern and the line | `(pattern, line)` |
| **Reduce** | Passes through, collecting all matched lines for each pattern into a unified result | `(pattern, list(line))` |

### 4. URL Access Frequency / Log Aggregation

Used to track website traffic metrics and identify popular web pages.

| Phase | What it does | Emits |
|---|---|---|
| **Map** | Parses web server logs and extracts the requested URL with a counter | `(URL, 1)` |
| **Reduce** | Aggregates the counts for each unique URL to calculate total hits | `(URL, total_views)` |

### 5. Social Network Graph Analysis (Common Friends)

Used by platforms to recommend connections by finding mutual
relationships. See [`MapReduce_Finding_Friends.html`](MapReduce_Finding_Friends.html)
and [`MapReduce_People_You_May_Know.md`](MapReduce_People_You_May_Know.md)
for this same idea worked all the way through with sample data.

| Phase | What it does | Emits |
|---|---|---|
| **Map** | For each user's friend list, emits every friend pair on it as the key, with the user as the value | `((Friend_A, Friend_B), User)` |
| **Reduce** | Groups by friend pair to reveal which users they share as mutual connections | `((Friend_A, Friend_B), list(mutual_friend))` |

### 6. Ride-Sharing: Trip Data Aggregation for Surge Pricing & Taxes

A ride-sharing platform processes ride receipts continuously. To compute
city-wide metrics, driver tax summaries, or longer-term demand trends,
it can run a nightly batch aggregation over the day's trips.

| Phase | What it does | Emits |
|---|---|---|
| **Map** | Reads individual trip receipts from a given day, extracting the city code and the fare | `(city_ID, fare_amount)` |
| **Reduce** | Groups fares by city to compute total daily revenue and average fare, feeding regional supply-and-demand baselines | `(city_ID, (total_revenue, average_fare))` |

### 7. Streaming Video: User Engagement Batch Analysis for Content Recommendations

Streaming platforms typically pair real-time signals (clicks, pauses)
with heavier batch jobs that analyze full watch histories to update
recommendation models.

| Phase | What it does | Emits |
|---|---|---|
| **Map** | Parses user viewing logs to extract the user ID and the genre of any video watched for more than 15 minutes | `(user_ID, genre)` |
| **Reduce** | Tallies genre frequency per user to build a per-user interest profile | `(user_ID, list((genre, watch_count)))` |

### 8. Credit Card Networks: Monthly Statement Generation & Fraud Audits

Card networks process large volumes of transactions. At the end of a
billing cycle, a MapReduce job can aggregate these transactions to
generate individual statements.

| Phase | What it does | Emits |
|---|---|---|
| **Map** | Scans the transaction ledger for a given month and emits the account number with the transaction details | `(account_no, transaction_details)` |
| **Reduce** | Orders all transactions chronologically by account and flags any that look anomalous | `(account_no, monthly_statement)` |

### 9. E-Commerce Platforms: Daily Inventory and Price Synchronization

Retail platforms with third-party sellers, multiple warehouses, and
global regions need end-of-day reconciliation of product listings.

| Phase | What it does | Emits |
|---|---|---|
| **Map** | Scans vendor inventory feeds and emits each product SKU with a stock change or price update | `(SKU, price_or_stock_update)` |
| **Reduce** | Resolves conflicting updates for the same SKU (e.g., keeps the latest-timestamped one) | `(SKU, final_synchronized_value)` |

### 10. Cybersecurity Systems: Distributed IP Blacklist Generation

Firewalls and cloud network defenses analyze large volumes of access
logs to identify and block malicious traffic sources, including
distributed denial-of-service (DDoS) attempts.

| Phase | What it does | Emits |
|---|---|---|
| **Map** | Scans network/firewall logs and filters for failed or suspicious connection attempts, emitting the source IP with a counter | `(source_IP, 1)` |
| **Reduce** | Sums suspicious requests per IP; if the count exceeds a threat threshold, adds the IP to the blacklist | `(source_IP, block_status)` |
