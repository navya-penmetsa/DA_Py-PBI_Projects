# -*- coding: utf-8 -*-
"""

COFFEE SHOP SALES ANALYSIS - DATA PREPROCESSING

"""


# Import Libraries

import pandas as pd
from dateutil import parser

import seaborn as sns
import matplotlib.pyplot as plt

from sqlalchemy import create_engine
from urllib.parse import quote_plus

# loading the excel file
coffeeshop_data = pd.read_excel(r"E:\ArborAcademy\Project_CS\coffee_shop_dataset.xlsx")

# Credentials to connect to Database
user = 'navya'  # user name
pw = 'mysql@12'  # password
pw = quote_plus(pw) # to be used in case of error
db = 'arbor'  # database name
engine = create_engine(f"mysql+pymysql://{user}:{pw}@localhost/{db}")

# to_sql() - function to push the dataframe onto a SQL table.
coffeeshop_data.to_sql('coffeeshop', con = engine, if_exists = 'replace', chunksize = 1000, index = False)

# Importing the data
sql = 'select * from coffeeshop;'
df = pd.read_sql_query(sql, engine)

# Reading the data
df.head()
df.info()
df.describe()
df.shape
df.dtypes


#------ IMPUTATION ------#
# Checking for missing values
df.isna().sum()


#------ TYPECASTING ------#
# Converting 'transaction_date' column to datetime format
def parse_dates(date_str):
    try:
        return parser.parse(date_str, dayfirst=False)  # Tries to detect the format
    except Exception:
        return pd.NaT
    
df['transaction_date'] = df['transaction_date'].apply(parse_dates)

# Converting 'transaction_time' to datetime.time Format
df['Transaction Time'] = (pd.Timestamp("00:00:00") + df['transaction_time']).dt.time


#------ HANDLING DUPLICATES ------#
# Detecting duplicate rows
duplicate = df.duplicated()
count = 0
for i in duplicate: 
    if i == True:
        count += 1
print("Total duplicate rows are: ", count)

# Duplicates in Columns
df.corr(numeric_only = True)


#------ OUTLIERS TREATMENT ------#
# Detecting Outliers in 'Quantities' & 'unit_price'
sns.boxplot(df["Quantities"], whis = 2.5)
sns.boxplot(df["unit_price"], whis = 2.5)

# Visualizing Skewness Using a Histogram
sns.histplot(df['Total Revenue'], bins=10, kde=True)
plt.title('Revenue Distribution')
plt.show()

sns.histplot(df['Quantities'], bins=10, kde=True)
plt.title('Quantities Distribution')
plt.show() # two peaks suggests bimodal distribution


#------ DISCRETIZATION ------#
# Discretizing Quantity column, consdering bins based on bimodal
df["quantity_size"] = pd.cut(df["Quantities"], bins=[0, 2, 5, 8], labels=["Small", "Medium", "Large"])

print(df[["Quantities", "quantity_size"]].head())


#------ ENCODING ------#

# Frequency encoding on 'product_type'
print(df['product_type'].value_counts())

product_counts = df['product_type'].value_counts()
df['Product Type Encoded'] = df['product_type'].map(product_counts)


#------ FEATURE ENGINEERING ------#
df.columns
# 'Profit Margin' will help analyze profitable categories/stores
df['Profit Margin %'] = ((df['Total Revenue'] - df['unit_price'] * df['Quantities']) / df['Total Revenue']) * 100

# Extracting "Hour" column from "transaction_time"
df['Hour'] = (pd.Timestamp("00:00:00") + df['transaction_time']).dt.hour

# Count Transactions Per Hour
hourly_sales = df.groupby('Hour')['transaction_id'].count().reset_index()
hourly_sales.columns = ['Hour', 'Transaction Count']

# Finding Peak Hours (Hours with Most Transactions)
peak_hours = hourly_sales[hourly_sales['Transaction Count'] == hourly_sales['Transaction Count'].max()]['Hour'].tolist()
print("Peak Hours:", peak_hours)

df['Is_Peak_Hour'] = df['Hour'].apply(lambda x: 1 if x in peak_hours else 0)


#------ VISUALIZATION ------#
# Visualizing Transactions per Hour
plt.figure(figsize=(10, 5))
sns.barplot(data=hourly_sales, x='Hour', y='Transaction Count', palette='Blues')
plt.xlabel('Hour of Day')
plt.ylabel('Number of Transactions')
plt.title('Transactions Per Hour')
plt.xticks(range(0, 24))  # Show all 24 hours
plt.show()

# Sales distribution plot
plt.figure(figsize=(8, 5))
sns.boxplot(x=df["Total Revenue"])
plt.title("Sales Distribution")
plt.show()

# Sales trend over Time
daily_sales = df.groupby("transaction_date")["Total Revenue"].sum().reset_index()

plt.figure(figsize=(12,6))
sns.lineplot(data=daily_sales, x="transaction_date", y="Total Revenue", marker="o", color="b")
plt.title("Total Revenue Trend Over Time")
plt.xticks(rotation=45)
plt.show()

# Best & Worst selling products
top_products = df.groupby("product_type")["Total Revenue"].sum().sort_values(ascending=False).head(10)
low_products = df.groupby("product_type")["Total Revenue"].sum().sort_values(ascending=True).head(10)

# Top 10
plt.figure(figsize=(14,6))
sns.barplot(y=top_products.index, x=top_products.values, palette="Greens_r")
plt.title("Top 10 Best Selling Products")
plt.xlabel("Total Revenue")
plt.show()

# Bottom 10
plt.figure(figsize=(14,6))
sns.barplot(y=low_products.index, x=low_products.values, palette="Reds_r")
plt.title("Bottom 10 Underperforming Products")
plt.xlabel("Total Revenue")
plt.show()

# Boxplot for Profit Margin % by Product Category
plt.figure(figsize=(12,6))
sns.boxplot(data=df, x="product_category", y="Profit Margin %", palette="coolwarm")
plt.xticks(rotation=45)
plt.title("Profit Margin % Distribution by Product Category")
plt.show()

# Heatmap for Relationship Between Revenue & Other Factors
corr_matrix = df[["Total Revenue", "Profit Margin %", "Quantities"]].corr()

# Heatmap
plt.figure(figsize=(8,5))
sns.heatmap(corr_matrix, annot=True, cmap="coolwarm", fmt=".2f", linewidths=0.5)
plt.title("Correlation Between Revenue, Profit & Quantity")
plt.show()


# Saving processed data
df.to_sql('coffeeshop_processed', con = engine, if_exists = 'replace', chunksize = 1000, index = False)

df.to_csv("coffeeshop_processed.csv", index = False)
print("Processed file saved successfully!")

