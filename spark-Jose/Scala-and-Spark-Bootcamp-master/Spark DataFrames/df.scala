import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

// create a DataFrame as a SparkSession
// DataFrame will treat every field as a string
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("Sales.csv")

// df.groupBy("Company").sum().show()
// df.select(sum("Sales")).show()
// df.select(sumDistinct("Sales")).show()

df.show()
df.orderBy($"Sales".desc).show()
