# 1. import required libraries
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from openai import OpenAI
import openai

"""
installing required libraries:

   % pip install openai
   % pip install pyspark
   
"""

# 2. create OpenAI client 
def create_client(openai_api_key):
    return OpenAI(api_key=openai_api_key) 
#end-def

# 3. define a UDF as a summarize function
# call LLM API
@udf(returnType=StringType())
def summarize(client, text):

    response = client.chat.completions.create(
        model="gpt-4o",  # or another chat model like "gpt-3.5-turbo"
        messages=[
            {
             "role": "user", 
             "content": f"Summarize this: {text}"
            }
        ]
    )

    print(response.choices[0].message.content)
    return response.choices[0].message.content
#end-def

# 4. create a SparkSession object
spark = SparkSession.builder.getOrCreate()

# 5. create a DataFrame('review_id', 'review_text', …)
data = {
    'review_id': [1, 2, 3],
    'review_text': [
        "This product is awesome, I would recommend everyone to buy one.",
        "The quality of the product is so bad that I will ask for a refund from the manufacturer",
        "It's okay, nothing special."
    ]
}
panda_df = pd.DataFrame(data)
df = spark.createDataFrame(panda_df)
df.show(truncate=False)

# 6. create OpenAI client
openai_api_key="your-openai-api-key"
client = create_client(openai_api_key)

# 7. add a new column to df
df = df.withColumn('summary', summarize(df.review_text))
df.show(truncate=False)


