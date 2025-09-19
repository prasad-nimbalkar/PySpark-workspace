from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Test") \
    .master("local[*]") \
    .getOrCreate()

data = [('Alice', 'Badminton, Tennis'), ('Bob', 'Tennis, Cricket'), ('Julis', 'Cricket, Carrom')]
columns = ["Name", "Hobbies"]

df = spark.createDataFrame(data, columns)

print(df.show())

# Optional Pandas conversion
try:
    pdf = df.toPandas()
    print(pdf)
except Exception as e:
    print("Error converting to Pandas:", e)

