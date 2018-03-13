import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

// create a DataFrame as a SparkSession
// DataFrame will treat every field as a string
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("CitiGroup2006_2008")
// Spark has lots of APIs dealing with dates and timestamps info

// Return a new dataset by adding a column or
// replacing the existing column that has the same name
val df2 = df.withColumn("Year", year(df("Date")))
// df2.show()

// val dfavgs = df2.groupBy("Year").mean()
// dfavgs.show()

val dfmins = df2.groupBy("Year").min()
dfmins.show()
