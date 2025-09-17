""" 
Example 2: Content Classification with GPT inside PySpark

Use Case:
          Classify textual data using GPT 
          (e.g., categorize support requests).
"""

# 1. import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from openai import OpenAI


# 2. create a SparkSession object
spark = SparkSession.builder.appName("classify-textual-data").getOrCreate()


# 3. create OpenAI client 
def create_client(openai_api_key):
    return OpenAI(api_key=openai_api_key) 
#end-def



# 4. Create a Sample DataFrame
def create_sample_dataFrame():
    support_data = [(1, "I need help updating my payment method."),
                    (2, "My app keeps crashing when I open it.",),
                    (3, "Mr. Smith's account is overdue, they have not paid for months",),
                    (4, "My phone does not charge after 8 pm",)]
    column_names = ["id", "message"]
    return spark.createDataFrame(support_data, column_names)
#end-def


# 5. UDF to classify text using GPT
def classify_text(text_as_string : str) -> str:
    
    # define openai_api_key 
    openai_api_key = "xxxxxxxxxxxxxxxxxx"
    
    # create a client to OpenAI
    client = create_client(openai_api_key)
    
    prompt = f"Classify the following support message into one category (Billing, Technical, Account):\n\n{text_as_string}"

    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=20
    )
    
    return response.choices[0].message.content.strip()
    # return response['choices'][0]['message']['content'].strip()
#end-def

#-------------------
# Driver program...
#-------------------

# create a UDF for PySpark
classify_text_udf = udf(classify_text, StringType())


# test API for a single string of review:
sample_text = "My phone does not charge after 8 pm"
print("Text Enrichment=", classify_text(sample_text))

# Create a Sample DataFrame
df = create_sample_dataFrame()
df.show(truncate=False)

# apply API to a DataFrame
classified_df = df.withColumn("classification", classify_text_udf(col('message')))
classified_df.show(truncate=False)