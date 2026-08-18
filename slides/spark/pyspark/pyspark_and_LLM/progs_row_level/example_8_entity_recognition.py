""" 
Example 3: Named Entity Recognition (NER) using GPT in a Data Pipeline

Use Case:
          Extract named entities (people, companies, locations) 
          from large text fields.
"""

# 1. import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from openai import OpenAI


# 2. create a SparkSession object
spark = SparkSession.builder.appName("extract-named-entities").getOrCreate()


# 3. create OpenAI client 
def create_client(openai_api_key):
    return OpenAI(api_key=openai_api_key) 
#end-def


# 4. Create a Sample DataFrame
def create_sample_dataFrame():
    documents = [(1, "James Duncan Farley, CEO of Ford Motor Company announced a new Ford factory in Germany."),
                 (2, "Microsoft and OpenAI signed a strategic partnership."),
                 (3, "Your new monthly deposit will be added to any existing balance on your EBT card. This allows you to save money for larger purchases, if needed."),
                 (4, "Ford is undergoing a massive shift, investing billions to launch a new, affordable midsize electric pickup truck in 2027")]
    #
    column_names = ["id", "document"]
    return spark.createDataFrame(documents, column_names)
#end-def


# 5. UDF to classify text using GPT
def extract_entities(text_as_string : str) -> str:
    
    # define openai_api_key 
    openai_api_key = "xxxxxxxxxxxxxxxxxx"
    
    # create a client to OpenAI
    client = create_client(openai_api_key)
    
    prompt = f"Extract named entities from the following text and return as a comma-separated list:\n\n{text_as_string}"

    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=50
    )
    
    return response.choices[0].message.content.strip()
    # return response['choices'][0]['message']['content'].strip()
#end-def

#-------------------
# Driver program...
#-------------------

# create a UDF for PySpark
extract_entities_udf = udf(extract_entities, StringType())


# test API for a single string of review:
sample_text = "James Duncan Farley, CEO of Ford Motor Company announced a new Ford factory in Germany."
print("entities=", extract_entities(sample_text))

# Create a Sample DataFrame
df = create_sample_dataFrame()
df.show(truncate=False)

# apply API to a DataFrame
named_entities_df = df.withColumn("entities", extract_entities_udf(col('document')))
named_entities_df.show(truncate=False)