"""
Natural language to SQL

Prompt

SYSTEM
	Given the following SQL tables, your job is
	to write queries given a user’s request.
    
    CREATE TABLE Orders (
      OrderID int,
      CustomerID int,
      OrderDate datetime,
      OrderTime varchar(8),
      PRIMARY KEY (OrderID)
    );
    
    CREATE TABLE OrderDetails (
      OrderDetailID int,
      OrderID int,
      ProductID int,
      Quantity int,
      PRIMARY KEY (OrderDetailID)
    );
    
    CREATE TABLE Products (
      ProductID int,
      ProductName varchar(50),
      Category varchar(50),
      UnitPrice decimal(10, 2),
      Stock int,
      PRIMARY KEY (ProductID)
    );
    
    CREATE TABLE Customers (
      CustomerID int,
      FirstName varchar(50),
      LastName varchar(50),
      Email varchar(100),
      Phone varchar(20),
      PRIMARY KEY (CustomerID)
    );

USER
	Write a SQL query which computes 
	the average total order value 
	for all orders on 2023-04-01.

Sample response

To compute the average total order value for all orders 
on 2023-04-01, we need to join the Orders, OrderDetails, 
and Products tables. We will sum the product of Quantity 
and UnitPrice for each order, and then compute the average 
of these sums.

Here is the SQL query:

	This query first computes the total value of 
	each order in the subquery, and then computes 
	the average of these values in the outer query.


	SELECT AVG(TotalOrderValue) AS AverageOrderValue
	FROM (
	  SELECT Orders.OrderID, 
			 SUM(OrderDetails.Quantity * Products.UnitPrice) AS TotalOrderValue
	  FROM Orders
	  JOIN OrderDetails ON Orders.OrderID = OrderDetails.OrderID
	  JOIN Products ON OrderDetails.ProductID = Products.ProductID
	  WHERE Orders.OrderDate = '2023-04-01'
	  GROUP BY Orders.OrderID
	) AS OrderValues;

"""

# 1. import required libraries
from openai import OpenAI

# 2. create OpenAI client 
client = OpenAI(api_key="your-openai-key") 

# 3. create response using API request
response = client.responses.create(
  model="gpt-4o",
  input=[
    {
      "role": "system",
      "content": [
        {
          "type": "input_text",
          "text": "Given the following SQL tables, your job is to write queries given a user’s request.\n    \n    CREATE TABLE Orders (\n      OrderID int,\n      CustomerID int,\n      OrderDate datetime,\n      OrderTime varchar(8),\n      PRIMARY KEY (OrderID)\n    );\n    \n    CREATE TABLE OrderDetails (\n      OrderDetailID int,\n      OrderID int,\n      ProductID int,\n      Quantity int,\n      PRIMARY KEY (OrderDetailID)\n    );\n    \n    CREATE TABLE Products (\n      ProductID int,\n      ProductName varchar(50),\n      Category varchar(50),\n      UnitPrice decimal(10, 2),\n      Stock int,\n      PRIMARY KEY (ProductID)\n    );\n    \n    CREATE TABLE Customers (\n      CustomerID int,\n      FirstName varchar(50),\n      LastName varchar(50),\n      Email varchar(100),\n      Phone varchar(20),\n      PRIMARY KEY (CustomerID)\n    );"
        }
      ]
    },
    {
      "role": "user",
      "content": [
        {
          "type": "input_text",
          "text": "Write a SQL query which computes the average total order value for all orders on 2023-04-01."
        }
      ]
    }
  ],
  temperature=1,
  max_output_tokens=2048
)

print("response=", response)