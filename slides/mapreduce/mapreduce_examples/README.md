# MapReduce Examples

Worked MapReduce examples: 

* average temperature per city
* total sales, order count, and largest order per store
* classic word count 
* palindromes 
* finding friends
* people-you-may-know

# Contents

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

# Classic MapReduce: 10 Examples

The MapReduce paradigm is best suited for large-scale, 
batch-processing problems where data can be split into 
independent chunks, processed in parallel, and combined 
based on a shared key. 

10  core problems that perfectly fit this model 
include:

## 1. Inverted Index Construction
Used by search engines to map words to the documents 
they appear in. 

**Map:** Parses a document, emitting each word as a key 
and the document ID as the value: `(word, doc_ID)`.

**Reduce:** Groups all document IDs by word to generate 
the final index: `(word, list(doc_ID))`.


## 2. Word Count and Frequency Analysis
The classic MapReduce benchmark used for analyzing 
logs or text repositories. 

**Map:** Processes lines of text and emits each encountered 
word with a count of one: `(word, 1)`.

**Reduce:** Sums up the counts for identical words to 
produce total frequencies: `(word, total_count)`. 


## 3. Distributed Grep (Pattern Matching)
Used to search for specific text patterns or 
error codes across massive server log files.

**Map:** Evaluates line-by-line against a regular expression. 
If a line matches the pattern, it emits the match: 
`(pattern, line)`.

**Reduce:** Acts as a pass-through or aggregates the matched 
lines into a unified result file.

## 4. URL Access Frequency / Log Aggregation
Used to track website traffic metrics and identify 
popular web pages.

**Map:** Parses web server logs and extracts the requested 
URL, emitting it with a counter: `(URL, 1)`.

**Reduce:** Aggregates the numbers for each unique URL to 
calculate total hits: `(URL, total_views)`.


## 5. Social Network Graph Analysis (Common Friends)
Used by platforms to recommend connections by 
finding mutual relationships. 

**Map:** Examines a user's friend list and emits pairs 
of friends as the key, with the user as the value: 
`((Friend_A, Friend_B), User)`.

**Reduce:** Groups by the friend pairs to reveal which 
mutual connections they share: 
`((Friend_A, Friend_B), list(Common_Friends))`.


## 6. Uber: Trip Data Aggregation for Surge Pricing & Taxes
Uber processes millions of rides daily. To 
calculate city-wide metrics, driver tax forms, 
or long-term demand trends, they run batch 
aggregations.

**Map:** Reads individual trip receipts from a 
specific day, extracting the city code and the fare: 
`(city_ID, fare_amount)`.

**Reduce:** Groups fares by city to compute total 
daily revenue, average trip lengths, or regional 
supply-and-demand baselines: `(city_ID, average_fare)`.

## 7. Netflix: User Engagement Batch Analysis for Content Recommendations
While Netflix uses real-time stream processing for 
immediate clicks, it relies on heavy batch processing 
to analyze overall user watch histories and update 
recommendation algorithms.

**Map:** Parses user viewing logs to extract the 
user ID and genres of videos watched for more than 
15 minutes: `(user_ID, genre)`.

**Reduce:** Aggregates the genres per user to 
determine their top interests, creating a user 
profile matrix: `(user_ID, list(favorite_genres))`.

## 8. Credit Card Networks: Monthly Statement Generation & Fraud Audits
Financial networks process billions of credit 
card transactions. At the end of a billing cycle, 
MapReduce  jobs aggregate these transactions to 
generate individual statements.

**Map:** Scans the global transaction ledger for a 
given month and emits the customer account number 
as the key, and the transaction details as the value: 
`(account_no, transaction_details)`.

**Reduce:** Groups all transactions chronologically 
by account number to construct the finalized monthly 
statement and flags accounts with unusual spending 
patterns: `(account_no, monthly_statement_pdf_data)`.

## 9. E-Commerce Platforms: Daily Inventory and Price Synchronization
Giant retail platforms scale product listings 
across third-party sellers, warehouses, and 
global regions, requiring massive end-of-day 
reconciliation.

**Map:** Scans thousands of vendor inventory sheets and 
emits product SKUs paired with stock changes or updated 
price feeds: `(SKU, price_update)`.

**Reduce:** Resolves conflicting price inputs, 
takes the latest timestamped data, and updates 
the core global catalog database: 
`(SKU, final_synchronized_price)`.

## 10. Cybersecurity Systems: Distributed IP Blacklist Generation
Large-scale firewalls and cloud networks analyze 
distributed denial-of-service (DDoS) attempts by 
processing firewalls logs to identify and block 
malicious traffic sources.

**Map:** Evaluates global network traffic logs 
and filters for failed or suspicious connection 
requests, emitting the source IP: `(source_IP, 1)`.

**Reduce:** Aggregates the total number of malicious 
requests per IP. If the count exceeds a specific threat 
threshold, the IP is pushed to a global network blacklist: 
`(source_IP, block_status)`.

