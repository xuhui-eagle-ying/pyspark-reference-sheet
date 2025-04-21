
# 🔥 PySpark Syntax Reference Sheet

A quick reference for performing common data operations in **PySpark** (DataFrame API).

---

## 📌 Table of Contents

- [Initialize Spark](#initialize-spark)
- [Load Data](#load-data)
- [Inspect Data](#inspect-data)
- [Select & Filter](#select--filter)
- [Sort Data](#sort-data)
- [Group & Aggregate](#group--aggregate)
- [Join & Merge](#join--merge)
- [Column Operations](#column-operations)
- [Missing Data](#missing-data)
- [Write Data](#write-data)

---

## Initialize Spark

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("App").getOrCreate()
```

## Load Data

```python
df = spark.read.csv('data.csv', header=True, inferSchema=True)
df = spark.read.json('data.json')
```

## Inspect Data

```python
df.show()
df.printSchema()
df.columns
df.describe().show()
```

## Select & Filter

```python
df.select('col')
df.select('col1', 'col2')
df.filter(df['age'] > 30)
df.filter((df['age'] > 30) & (df['gender'] == 'M'))
```

## Sort Data

```python
df.orderBy('age')
df.orderBy(df['age'].desc())
```

## Group & Aggregate

```python
df.groupBy('department').count()
df.groupBy('department').agg({'salary': 'avg'})
df.groupBy('dept', 'gender').agg({'salary': 'max'})
```

## Join & Merge

```python
df1.join(df2, on='id', how='inner')  # options: left, right, outer
```

## Column Operations

```python
from pyspark.sql.functions import col

df = df.withColumn('new_col', col('salary') * 1.1)
df = df.withColumnRenamed('old', 'new')
df = df.drop('unwanted')
```

## Missing Data

```python
df.dropna()
df.fillna(0)
df.filter(df['col'].isNotNull())
```

## Write Data

```python
df.write.csv('output.csv', header=True)
df.write.json('output.json')
```