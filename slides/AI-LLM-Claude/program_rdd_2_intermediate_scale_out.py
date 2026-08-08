% python3 program_rdd_2_intermediate_scale_out.py
✅ ANTHROPIC_API_KEY  : sk-ant-api03...
   ANTHROPIC_MODEL    : claude-sonnet-4-6
   LLM_MAX_TOKENS     : 300
   LLM_BATCH_SIZE     : 5  (records per Claude call)
   LLM_RATE_LIMIT_RPM : 50  → sleep 6.00s/batch
   LLM_MAX_CHARS      : 800
   SPARK_MASTER       : local[*]
   SPARK_PARTITIONS   : 4
WARNING: Using incubator modules: jdk.incubator.vector
Using Spark's default log4j profile: org/apache/spark/log4j2-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
26/08/07 11:51:16 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
✅  Spark 4.2.0  |  master: local[*]
   Accumulator 'dead_letter_count' registered
RDD: 18 reviews  |  4 partitions

Partition layout (via glom):
  Partition 0: 0 records  |  IDs []  |  categories []
  Partition 1: 10 records  |  IDs [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]  |  categories ['Clothing', 'Electronics', 'Furniture']
  Partition 2: 8 records  |  IDs [11, 12, 13, 14, 15, 16, 17, 18]  |  categories ['Clothing', 'Electronics', 'Furniture']
  Partition 3: 0 records  |  IDs []  |  categories []
============================================================
  COST COMPARISON  (18 rows, batch_size=5)
============================================================
                          API calls     Est. cost
  -------------------- ------------  ------------
  Unbatched (1/row)              18  $     0.0405
  Batched (this)                  4  $     0.0364
  Savings                        14  $     0.0040
============================================================

  At larger scales (batch_size=5):
         1,000 rows →        200 API calls
       100,000 rows →     20,000 API calls
     1,000,000 rows →    200,000 API calls
Local batch test (2 reviews in 1 call):
      [test] OK — 2 reviews → 2 results
  review 1: {"sentiment": "POSITIVE", "rating": 5, "summary": "Customer loves the headphones and is very impressed with the sound quality.", "follow_up": false}
  review 2: {"sentiment": "NEGATIVE", "rating": 1, "summary": "Chair broke after just two days, customer considers it absolute rubbish.", "follow_up": true}
✅  enrich_partition_batched() defined
   Batch size  : 5 records per Claude call
   Output tuple: (id, category, text, sentiment, rating, summary, follow_up)
   Accumulator : dead_letter_count tracks failures
   [partition] empty — skipping
   [partition] empty — skipping
   [partition] received 10 records: IDs [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
   [partition] received 8 records: IDs [11, 12, 13, 14, 15, 16, 17, 18]
      Sending [batch 1, 5 reviews]: IDs [1, 2, 3, 4, 5]
      Sending [batch 1, 5 reviews]: IDs [11, 12, 13, 14, 15]
      [batch 1, 5 reviews] OK — 5 reviews → 5 results
      Sending [batch 2, 5 reviews]: IDs [6, 7, 8, 9, 10]
      [batch 1, 5 reviews] OK — 5 reviews → 5 results
      Sending [batch 2, 3 reviews]: IDs [16, 17, 18]
      [batch 2, 3 reviews] OK — 3 reviews → 3 results
   [partition] done: 8 records → 2 API call(s) (batch_size=5), 8 OK, 0 dead-letter
      [batch 2, 5 reviews] OK — 5 reviews → 5 results
   [partition] done: 10 records → 2 API call(s) (batch_size=5), 10 OK, 0 dead-letter

===========================================================================
  ENRICHED REVIEWS (18 records)
===========================================================================
   ID  Category      Sentiment    ★  FU  Summary
  ---  ------------  ----------  --  --  --------------------------------------
    1  Electronics   POSITIVE     5      Excellent headphones with outstanding so
    2  Electronics   NEGATIVE     1  ⚠️  Device failed after one week due to char
    3  Electronics   NEUTRAL      3      Webcam works adequately but suffered fro
    4  Furniture     NEUTRAL      3  ⚠️  Chair arrived damaged but customer servi
    5  Furniture     POSITIVE     5      Outstanding solid oak desk with easy ass
    6  Furniture     NEUTRAL      3  ⚠️  Comfortable for the price but wobbles on
    7  Clothing      POSITIVE     5      Perfect fit and fabric prompted customer
    8  Clothing      NEGATIVE     2  ⚠️  Sizes run very small, medium fits like a
    9  Clothing      POSITIVE     4      Brief but positive note praising the clo
   10  Electronics   POSITIVE     5      Keyboard feels premium and makes daily t
   11  Electronics   NEGATIVE     1  ⚠️  Monitor has a defective bright spot and
   12  Furniture     POSITIVE     5      Standing desk converter eliminated back
   13  Clothing      NEGATIVE     2  ⚠️  Colour faded after one wash, indicating
   14  Electronics   POSITIVE     5      SSD delivers advertised speeds with no i
   15  Furniture     NEUTRAL      3  ⚠️  Assembly requires two people and instruc
   16  Clothing      POSITIVE     5      Customer loves the hoodie for its incred
   17  Electronics   POSITIVE     4      Outstanding camera for the price despite
   18  Furniture     NEGATIVE     1  ⚠️  Wood veneer peeled after three days, res
=======================================================
  Enrichment Report
=======================================================
  Total records  : 18
  Successful     : 18  (100%)
  Dead letters   : 0   (accumulator value)
  API calls made : ~4  (batch_size=5)
  Without batching: 18 calls would have been needed
=======================================================
=== Average Rating per Category ===
  Clothing        3.60  ★★★★☆
  Electronics     3.43  ★★★☆☆
  Furniture       3.33  ★★★☆☆
=== Sentiment Distribution ===
  POSITIVE     9  (50%)  █████████
  NEGATIVE     5  (28%)  █████
  NEUTRAL      4  (22%)  ████
=== Review IDs per Category (groupByKey) ===
  Clothing        IDs: [7, 8, 9, 13, 16]
  Electronics     IDs: [1, 2, 3, 10, 11, 14, 17]
  Furniture       IDs: [4, 5, 6, 12, 15, 18]
=== Reviews Flagged for Follow-Up (8) ===
  [ 2] ★1  Electronics   Device failed after one week due to charging
  [11] ★1  Electronics   Monitor has a defective bright spot and is be
  [18] ★1  Furniture     Wood veneer peeled after three days, resultin
  [ 8] ★2  Clothing      Sizes run very small, medium fits like an ext
  [13] ★2  Clothing      Colour faded after one wash, indicating very
  [ 4] ★3  Furniture     Chair arrived damaged but customer service re
  [ 6] ★3  Furniture     Comfortable for the price but wobbles on unev
  [15] ★3  Furniture     Assembly requires two people and instructions
After partitionBy(3) on sentiment key:
  Partition 0: sentiments=[]  IDs=[]  (0 records)
  Partition 1: sentiments=['NEUTRAL']  IDs=[3, 4, 6, 15]  (4 records)
  Partition 2: sentiments=['NEGATIVE', 'POSITIVE']  IDs=[1, 2, 5, 7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 18]  (14 records)
Joined Dataset (review + category metadata):

   ID  Category      Dept      Returnd   ★  Sentiment
  -----------------------------------------------------------------
    1  Electronics   Tech          30    5  POSITIVE
    2  Electronics   Tech          30    1  NEGATIVE
    3  Electronics   Tech          30    3  NEUTRAL
    4  Furniture     Home          60    3  NEUTRAL
    5  Furniture     Home          60    5  POSITIVE
    6  Furniture     Home          60    3  NEUTRAL
    7  Clothing      Fashion       14    5  POSITIVE
    8  Clothing      Fashion       14    2  NEGATIVE
    9  Clothing      Fashion       14    4  POSITIVE
   10  Electronics   Tech          30    5  POSITIVE
   11  Electronics   Tech          30    1  NEGATIVE
   12  Furniture     Home          60    5  POSITIVE
   13  Clothing      Fashion       14    2  NEGATIVE
   14  Electronics   Tech          30    5  POSITIVE
   15  Furniture     Home          60    3  NEUTRAL
   16  Clothing      Fashion       14    5  POSITIVE
   17  Electronics   Tech          30    4  POSITIVE
   18  Furniture     Home          60    1  NEGATIVE
✅  Saved (partitioned by category + sentiment) → /var/folders/_5/wx3bhmf91dq3fv1bjwmpdlzc0000gn/T/reviews_enriched_rdd2_scaleout
+-----------+---------+-----+
|   category|sentiment|count|
+-----------+---------+-----+
|   Clothing| NEGATIVE|    2|
|   Clothing| POSITIVE|    3|
|Electronics| NEGATIVE|    2|
|Electronics|  NEUTRAL|    1|
|Electronics| POSITIVE|    4|
|  Furniture| NEGATIVE|    1|
|  Furniture|  NEUTRAL|    3|
|  Furniture| POSITIVE|    2|
+-----------+---------+-----+
