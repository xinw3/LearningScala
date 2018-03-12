import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

// create a DataFrame as a SparkSession
// DataFrame will treat every field as a string
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("CitiGroup2006_2008")
df.printSchema()

// two main ways to filter out the data
// one way is to use spark sql syntax, no need to import anything
// the other way is to use scala syntax, $, need to import

import spark.implicits._
// Scala notation
// df.filter($"Close" < 480 && $"High" < 480).show()

// sql notation
// collect() is the way to transform dataframe to Array objects
// val ch_low = df.filter("Close < 480 AND High < 480").collect()

// val ch_low = df.filter("Close < 480 AND High < 480").count()

// triple equal signs for determining equal
// df.filter($"High" === 484.40).show()

// or use sql notation
// df.filter("High = 484.40").show()

df.select(corr("High", "Low")).show()
