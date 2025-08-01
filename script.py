import sqlite3
import pandas as pd
import os

# output folder 
os.makedirs("output", exist_ok=True)

# Connect to SQLite DB
conn = sqlite3.connect("db/xyz_sales.db")

# SQL Approach
sql = """
SELECT 
    c.CustomerID AS Customer,
    c.Age,
    si.Item,
    SUM(si.Quantity) AS Quantity
FROM Customer c
JOIN Sales s ON c.CustomerID = s.CustomerID
JOIN SaleItems si ON s.SaleID = si.SaleID
WHERE c.Age BETWEEN 18 AND 35
  AND si.Quantity IS NOT NULL
GROUP BY c.CustomerID, c.Age, si.Item
HAVING SUM(si.Quantity) > 0
ORDER BY c.CustomerID, si.Item;
"""

df_sql = pd.read_sql_query(sql, conn)
df_sql.to_csv("output/sales_summary_sql.csv", index=False, sep=';')

# Pandas Approach
df_customer = pd.read_sql("SELECT * FROM Customer", conn)
df_sales = pd.read_sql("SELECT * FROM Sales", conn)
df_items = pd.read_sql("SELECT * FROM SaleItems", conn)

# Merge and filter data
df = df_sales.merge(df_customer, on="CustomerID")
df = df.merge(df_items, on="SaleID")
df = df[(df["Age"].between(18, 35)) & (df["Quantity"].notnull())]

# Group and sum
df = df.groupby(["CustomerID", "Age", "Item"], as_index=False)["Quantity"].sum()
df = df[df["Quantity"] > 0]
df.rename(columns={"CustomerID": "Customer"}, inplace=True)
df.to_csv("output/sales_summary_pandas.csv", index=False, sep=';')

conn.close()
