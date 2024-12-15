from pyspark import SparkConf, SparkContext

# Define the Spark configuration
conf = SparkConf().setAppName("Graph Example").setMaster("local[*]")

# Initialize SparkContext
sc = SparkContext(conf=conf)

# Create an RDD
rdd_test = sc.parallelize([1, 2, 3, 4])

# Take the first few elements from the RDD
print(rdd_test.take(5))
sc.stop()