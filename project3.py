import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType

def jaccard_similarity(list1, list2):
    s1 = set(list1[0])
    s2 = set(list2[0])
    return float(len(s1.intersection(s2))) / float(len(s1.union(s2)))

class project3:
    def run(self, inputpath, outputpath, t):
        # Create a SparkSession
        spark = SparkSession.builder.master("local").appName("project3").getOrCreate()

        # Register the Jaccard similarity function as a UDF
        jaccard_similarity_udf = udf(jaccard_similarity, DoubleType())

        # Read the CSV file into a DataFrame
        df = spark.read \
                  .csv(inputpath) \
                  .toDF("InvoiceNo", "Description", "Quantity", "InvoiceDate", "UnitPrice")

        # Split the InvoiceDate column into date and time
        split_col = split(df['InvoiceDate'], ' ')
        df = df.withColumn('Date', split_col.getItem(0))

        # Split the Date column into day, month, and year
        split_col = split(df['Date'], '/')
        df = df.withColumn('Year', split_col.getItem(2).cast("integer")) \
               .withColumn("InvoiceNo", col("InvoiceNo").cast("integer"))

        # Group by InvoiceNo and Year, and collect all items in a set
        transactions = df.groupBy("InvoiceNo", "Year") \
                         .agg(collect_set("Description") \
                         .alias("items")) \
                         .orderBy("InvoiceNo")

        
        # Compute the prefix of each transaction
        prefix_length = int(t * transactions.count())

        transactions = transactions.groupBy("InvoiceNo", "Year") \
                                   .agg(collect_list("items").alias("prefix")) \
                                   .withColumn("prefix", slice("prefix", 1, prefix_length))
        
        # Self join on the transactions based on the prefix to get candidate pairs
        candidate_pairs = transactions.alias("t1") \
                                      .join(transactions.alias("t2"), array_intersect("t1.prefix", "t2.prefix").isNotNull() & (col("t1.InvoiceNo") < col("t2.InvoiceNo")) & (col("t1.Year") != col("t2.Year")))

        # Compute Jaccard similarity for each candidate pair and filter by the threshold
        result = candidate_pairs.withColumn("similarity", jaccard_similarity_udf("t1.prefix", "t2.prefix")) \
                                .filter(col("similarity") >= t) \
                                .select(concat(lit("("), least(col("t1.InvoiceNo"), col("t2.InvoiceNo")), lit(","), greatest(col("t1.InvoiceNo"), col("t2.InvoiceNo")), lit(")"), lit(","), "similarity")) \
                                .coalesce(1) \
                                .orderBy(col("t1.InvoiceNo"), col("t2.InvoiceNo"))
        # Write the result to the output path without quotes
        result.write.text(outputpath)

if __name__ == '__main__':
    project3().run(sys.argv[1], sys.argv[2], float(sys.argv[3]))
