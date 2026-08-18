"""
Prompt
	SYSTEM
			You will be provided with a tweet, and your task is to 
			classify its sentiment as positive, neutral, or negative.
	USER
			I loved the new Batman movie!
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
          "text": "You will be provided with a tweet, and your task is to classify its sentiment as positive, neutral, or negative."
        }
      ]
    },
    {
      "role": "user",
      "content": [
        {
          "type": "input_text",
          "text": "I loved the new Batman movie!"
        }
      ]
    }
  ],
  temperature=1,
  max_output_tokens=256
)

print("response=", response)
