""" 
Example 4: 
          Translating Data at Scale using GPT in PySpark

Use Case:
          Translate text in a multilingual dataset using GPT.
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
    foreign_texts = [(1, "Hola, necesito ayuda con mi pedido."),
                     (2, "Bonjour, j’ai un problème avec mon compte.")]
    #           
    column_names = ["id", "original_text"]
    return spark.createDataFrame(foreign_texts, column_names)
#end-def


# 5. UDF to translate text to English using GPT
def translate_to_english(text_as_string : str) -> str:
    
    # define openai_api_key 
    openai_api_key = "xxxxxxxxxxxxxxxxxx"
    
    # create a client to OpenAI
    client = create_client(openai_api_key)
    
    prompt = f"Translate this text to English:\n\n{text_as_string}"

    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=100
    )
    
    return response.choices[0].message.content.strip()
    # return response['choices'][0]['message']['content'].strip()
#end-def

#-------------------
# Driver program...
#-------------------

# create a UDF for PySpark
translate_udf = udf(translate_to_english, StringType())

# test API for a single string of review:
sample_text = "Hola, necesito ayuda con mi pedido."
print("English-Translation=", translate_to_english(sample_text))

# Create a Sample DataFrame
foreign_df = create_sample_dataFrame()
foreign_df.show(truncate=False)

# apply API to a DataFrame
translated_df = foreign_df.withColumn("translated", translate_udf("original_text"))
translated_df.show(truncate=False)