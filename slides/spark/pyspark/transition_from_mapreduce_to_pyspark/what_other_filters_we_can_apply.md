# Filters Discussion

Here are several common transformations 
and filters you can apply to clean and 
refine word count data in PySpark:

---

### 1. Data Cleaning Filters (Preprocessing)

Applied **before** `reduceByKey` (right after `flatMap`):

* **Remove Punctuation & Special Characters:**
Strip unwanted symbols like commas, periods, or quotes so `"spark."` and `"spark"` are counted together.

```python
import re
# Strip punctuation, keeping only alphanumeric characters
.map(lambda word: re.sub(r'[^\w\s]', '', word))
# Remove any empty strings resulting from stripped punctuation
.filter(lambda word: len(word) > 0)
```


* **Normalize Case (Case Insensitivity):**
Convert all words to lowercase so `"Spark"` and `"spark"` are grouped together.

```python
.map(lambda word: word.lower())
```


* **Filter Out Stop Words:**
Remove common, low-meaning noise words like *"the"*, *"is"*, *"at"*, *"which"*.

```python
STOP_WORDS = {"the", "is", "at", "which", "on", "and", "a", "an", "in", "to", "for", "of", "or"}

.filter(lambda word: word.lower() not in STOP_WORDS)
```


* **Filter Non-Alphabetic Tokens (Numbers & Mixed Characters):**
Discard pure numbers (e.g., `"123"`) or strings containing digits (e.g., `"v2"`).

```python
# Keep only words made purely of alphabetic characters
.filter(lambda word: word.isalpha())

```

---

### 2. Post-Aggregation Filters & Sorting

Applied **after** `reduceByKey`:

* **Filter Out Extremely High Frequencies (Max Threshold):**
Useful for removing dominant header/footer terms or structural boilerplates.

```python
# Keep words with count between 4 and 10,000
.filter(lambda item: 4 <= item[1] <= 10000)

```


* **Filter Specific Target Keywords (Whitelist / Blacklist):**
Filter output based on predefined lists or patterns (e.g., specific domain terms or vulgarities).

```python
BLACKLIST = {"draft", "temp", "null", "undefined"}

.filter(lambda item: item[0] not in BLACKLIST)

```


* **Filter by Regex Patterns (e.g., Specific Prefixes/Suffixes):**
Keep only words starting or ending with a specific pattern (e.g., hashtags, handles, or prefixes).

```python
# Keep only words starting with 'spark'
.filter(lambda item: item[0].startswith("spark"))

```



---

### 3. Example Pipeline

Combining these into a cleaned pipeline:

```python
import re

STOP_WORDS = {"the", "and", "for", "with", "that", "this"}

word_counts = (
    lines.flatMap(lambda x: x.split(" "))
    .map(lambda word: re.sub(r"[^\w\s]", "", word).lower())  # Remove punctuation & lowercase
    .filter(lambda word: word.isalpha())                     # Pure letters only
    .filter(lambda word: len(word) >= 3)                     # Length >= 3
    .filter(lambda word: word not in STOP_WORDS)             # Exclude stop words
    .map(lambda word: (word, 1))
    .reduceByKey(lambda a, b: a + b)
    .filter(lambda item: item[1] >= 4)                       # Min frequency >= 4
    .sortBy(lambda item: item[1], ascending=False)           # Sort by frequency descending
)

```
