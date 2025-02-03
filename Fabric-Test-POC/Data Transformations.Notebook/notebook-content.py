# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0ed56039-e5a3-4695-ab7f-6f2e5348311a",
# META       "default_lakehouse_name": "Fabriclakehouse",
# META       "default_lakehouse_workspace_id": "0ca398a8-3031-4fe7-80e8-cf6b2f8cf84e",
# META       "known_lakehouses": [
# META         {
# META           "id": "0ed56039-e5a3-4695-ab7f-6f2e5348311a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/fabricdemo/BigMart Sales.csv")
# df now is a Spark DataFrame containing CSV data from "Files/fabricdemo/BigMart Sales.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

json_df = spark.read.option("multiline", "false").json("Files/fabricdemo/drivers.json")
# df now is a Spark DataFrame containing JSON data from "Files/fabricdemo/drivers.json".
display(json_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

my_ddl_schema = '''
    Item_Identifier STRING,
    Item_Weight DOUBLE,
    Item_Fat_Content STRING,
    Item_Visibility DOUBLE,
    Item_Type STRING,
    Item_MRP DOUBLE,
    Outlet_Identifier STRING,
    Outlet_Establishment_Year STRING,
    Outlet_Size STRING,
    Outlet_Location_Type STRING,
    Outlet_Type STRING,
    Item_Outlet_Sales DOUBLE
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df= spark.read.format('csv')\
              .schema(my_ddl_schema)\
              .option('header','true')\
              .load('Files/fabricdemo/BigMart Sales.csv')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import*
from pyspark.sql.functions import*


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

my_struct_schema= StructType([
                               StructField('Item_Identifier',StringType(),True),
                               StructField('Item_Weight',StringType(),True),
                               StructField('Item_Fat_Content',StringType(),True),
                               StructField('Item_Visibility',DoubleType(),True),
                               StructField('Item_Type',StringType(),True),
                               StructField('Item_MRP',StringType(),True),
                               StructField('Outlet_Identifier',StringType(),True),
                               StructField('Outlet_Establishment_Year',StringType(),True),
                               StructField('Outlet_Size',StringType(),True),
                               StructField('Outlet_Location_Type',StringType(),True),
                               StructField('Outlet_Type',StringType(),True),
                               StructField('Item_Outlet_Sales',StringType(),True)
                            ])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df= spark.read.format('csv')\
              .schema(my_struct_schema)\
              .option('header','true')\
              .load('Files/fabricdemo/BigMart Sales.csv')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### SELECT TRANSFORMATION
# - create a variable store the dataframe and display
# - use show() method

# CELL ********************

select_df=df.select('Item_Identifier','Item_Weight','Item_Fat_Content').show()
display(select_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

select_df=df.select(col('Item_Identifier'),col('Item_Weight'),col('Item_Fat_Content')).show()
display(select_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ### ALIAS

# CELL ********************

alias_df=df.select(col('Item_Identifier').alias('Item_ID'))
display(alias_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Filter Scenario 1

# CELL ********************

filter1_df=df.filter(col('Item_Fat_Content')=='Regular')
display(filter1_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Filter Scenario 2

# CELL ********************

filter2_df= df.filter( (col('Item_Type')=='Soft Drinks') & (col('Item_Weight')<10))
display(filter2_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Filter Scenario 3
#  **.isin()**
#  
#  **.isNull**

# CELL ********************

filter3_df=df.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1','Tier 2')))
display(filter3_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### withColumnRenamed at the DataFrame Level
# PySpark DataFrames are designed to be immutable, meaning that operations like withColumnRenamed() return a new DataFrame with the changes, but do not modify the original DataFrame. This is similar to the behavior of many functional programming languages, where immutability is often used to avoid side effects.

# CELL ********************

df = df.withColumnRenamed('Item_Identifier','Item_ID')
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# List of old column names and new column names
columns_to_rename = [("Item_Identifier", "Item_ID"), ("Item_weight", "Item_Weight")]

# Chain the renaming operations
for old_name, new_name in columns_to_rename:
    df = df.withColumnRenamed(old_name, new_name)

display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### withColumn
# A method used to add a new column or modify an existing column in a DataFrame. The method takes two parameters:
# - The name of the new or existing column.
# - The expression or transformation that defines the new column's values.
#   - Creating a New Column
#   - Modifying an Existing Column
#   - Using Expressions on Columns
#   - lit() to store a constant value
#   - regexp_replace()
#   


# CELL ********************

df= df.withColumn('flag',lit('New'))
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df= df.withColumn('multiply',col('Item_Weight')*col('Item_MRP'))
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),'Regular','Reg'))\
       .withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),'Low Fat','Lf'))
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Type Casting

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
