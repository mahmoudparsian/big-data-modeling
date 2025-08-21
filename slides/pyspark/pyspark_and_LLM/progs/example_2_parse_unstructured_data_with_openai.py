"""
Prompt

SYSTEM
       You will be provided with unstructured data, and 
       your task is to parse it into CSV format.

USER
       There are many fruits that were found on the recently 
       discovered planet Goocrux. There are neoskizzles that 
       grow there, which are purple and taste like candy. There 
       are also loheckles, which are a grayish blue fruit and are 
       very tart, a little bit like a lemon. Pounits are a bright 
       green color and are more savory than sweet. There are also 
       plenty of loopnovas which are a neon pink flavor and taste 
       like cotton candy. Finally, there are fruits called glowls, 
       which have a very sour and bitter taste which is acidic and 
       caustic, and a pale orange tinge to them.
"""

# 1. import required libraries
from openai import OpenAI

# 2. create OpenAI client 
client = OpenAI(api_key="your-openai-key") 

# 3. create response
response = client.responses.create(
  model="gpt-4o",
  input=[
    {
      "role": "system",
      "content": [
        {
          "type": "input_text",
          "text": "You will be provided with unstructured data, and your task is to parse it into CSV format."
        }
      ]
    },
    {
      "role": "user",
      "content": [
        {
          "type": "input_text",
          "text": "There are many fruits that were found on the recently discovered planet Goocrux. There are neoskizzles that grow there, which are purple and taste like candy. There are also loheckles, which are a grayish blue fruit and are very tart, a little bit like a lemon. Pounits are a bright green color and are more savory than sweet. There are also plenty of loopnovas which are a neon pink flavor and taste like cotton candy. Finally, there are fruits called glowls, which have a very sour and bitter taste which is acidic and caustic, and a pale orange tinge to them."
        }
      ]
    }
  ],
  temperature=1,
  max_output_tokens=256
)

print("response=", response)
