from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import (
    StructType,
    StructField,
    ArrayType,
    IntegerType,
    StringType,
)

spark = SparkContext("local", "Test App").getOrCreate()

# Sample data
data = [
    ([1, 2, 3], [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}], 100),
    (
        [4, 5, 6],
        [{"name": "Charlie", "age": 22}, {"name": "David", "age": 28}],
        200,
    ),
    ([7, 8, 9], [{"name": "Eve", "age": 26}, {"name": "Frank", "age": 33}], 300),
]

# schema
schema = StructType(
    [
        StructField(
            "A",
            StructType(
                [StructField("array_of_ints", ArrayType(IntegerType()), True)]
            ),
            True,
        ),
        StructField(
            "B",
            ArrayType(
                StructType(
                    [
                        StructField("name", StringType(), True),
                        StructField("age", IntegerType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField("C", IntegerType(), True),
    ]
)

# Create the DataFrame
df = spark.createDataFrame(data, schema=schema)
df.show(truncate=False)