import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

// create a DataFrame as a SparkSession
// DataFrame will treat every field as a string
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("ContainsNull.csv")

df.printSchema()

df.show()

// Look through the columns, find the same datatype
// and fill in null values with the value we set
// df.na.fill(100).show()

// df.na.fill("Missing Name").show()

// Instead of fill in any null columns with the same datatype,
// we could specify the column names as an array and only fill in these columns
df.na.fill("New Name", Array("Name")).show()
