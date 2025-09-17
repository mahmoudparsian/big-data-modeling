""" 
Example 1: Text Enrichment in a Spark DataFrame using GPT

Use Case:
          Enhance or summarize text fields 
          (e.g., customer reviews, support 
          tickets) using GPT.
"""

# 1. import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from openai import OpenAI
#import opener

# 2. create a SparkSession object
spark = SparkSession.builder.appName("GPTTextEnrichment").getOrCreate()


# 3. create OpenAI client 
def create_client(openai_api_key):
    return OpenAI(api_key=openai_api_key) 
#end-def



# 4. Create a Sample DataFrame
def create_sample_dataFrame():
    data = [(1, "The headphone is a greate product and works as expected."),
            (2, "The product is okay but arrived late."),
            (3, "This computer is crashing all of the times."),
            (4, "Excellent quality and fast delivery.")]
    column_names = ["id", "review"]
    return spark.createDataFrame(data, column_names)
#end-def

# 5. UDF to summarize text using GPT
def summarize_review(text_as_string : str) -> str:
    
    #openai_api_key = "your-openai-api-key"
    openai_api_key = "xxxxxxxxxxxxxxxxxx"
    
    # create a client to OpenAI
    client = create_client(openai_api_key)
    
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": f"Summarize this review: {text_as_string}"}],
        max_tokens=50
    )
    
    return response.choices[0].message.content
    # return response['choices'][0]['message']['content'].strip()
#end-def

#-------------------
# Driver program...
#-------------------

# create a UDF for PySpark
summarize_udf = udf(summarize_review, StringType())


# test API for a single string of review:
sample_text = "The headphone is a greate product and works as expected."
print("Text Enrichment=", summarize_review(sample_text))

# Create a Sample DataFrame
df = create_sample_dataFrame()
df.show(truncate=False)

# apply API to a DataFrame
df_enriched = df.withColumn("summary", summarize_udf(col('review')))
df_enriched.show(truncate=False)