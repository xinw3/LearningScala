import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

// create a DataFrame as a SparkSession
// DataFrame will treat every field as a string
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("CitiGroup2006_2008")

// df.head(5)

// compute the statistics for numeric and string columns
// df.describe().show()

val df2 = df.withColumn("HighPlusLow", df("High") + df("Low"))
df2.select(df2("HighPlusLow").as("HPL"), df2("Close")).show

df2.printSchema()
