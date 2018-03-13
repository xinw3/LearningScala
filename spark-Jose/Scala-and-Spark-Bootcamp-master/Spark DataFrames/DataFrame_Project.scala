// DATAFRAME PROJECT
// Use the Netflix_2011_2016.csv file to Answer and complete
// the commented tasks below!

// Start a simple Spark Session
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()
// Load the Netflix Stock CSV File, have Spark infer the data types.
// NOTE: load is always at the last
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("Netflix_2011_2016.csv")
// What are the column names?
// Date, Open, High, Low, Close, Volume, Adj Close
println("Column Names")
df.columns

// What does the Schema look like?
println("Schema:")
df.printSchema()
// Print out the first 5 columns.
println("Print out the First 5 Columns:")
df.select("Date", "Open", "High", "Low", "Close").show()

// Print out the first 5 rows
println("First 5 Rows:")
df.show(5)

// Use describe() to learn about the DataFrame.
println("Describe the DataFrame:")
df.describe().show()
// Create a new dataframe with a column called HV Ratio that
// is the ratio of the High Price versus volume of stock traded
// for a day.
println("Add a new column \"HV Ratio\":")
val df2 = df.withColumn("HV Ratio", df("High") / df("Volume"))
df2.show(5)
import org.apache.spark.sql.functions._


// What day had the Peak High in Price?
println("The day had the Peak High in Price:")
df.orderBy($"High".desc).select("Date").head()

// What is the mean of the Close column?
println("Mean of the Close Column:")
df.select(mean("Close")).show()
// What is the max and min of the Volume column?
println("Max and min of the Volume column:")
df.agg(max("Volume"), min("Volume")).show()

// For Scala/Spark $ Syntax
// NOTE: this import is used to use the $-notation
import spark.implicits._
// How many days was the Close lower than $ 600?
println("How many days was the Close lower than $600:")
df.filter($"Close" < 600).count()
// What percentage of the time was the High greater than $500 ?
println("What percentage of the time was the High greater than $500?")
df.filter($"High" > 500).count().toFloat / df.count()
// What is the Pearson correlation between High and Volume?
println("What is the Pearson correlation between High and Volume?")
df.select(corr("High", "Volume")).show()
// What is the max High per year?
df.select(max("High")).show()
// What is the average Close for each Calender Month?
val df_add_month = df.withColumn("Month", month(df("Date")))
df_add_month.groupBy("Month").mean("Close").orderBy("Month").show()
