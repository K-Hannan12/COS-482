from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('sparkSQL').getOrCreate()

# A) Create Data frame from RDD
rdd = spark.sparkContext.textFile("HW3/pagerank_output/part-00000")

rdd_split = rdd.map(lambda line: line.split())

rank_df = spark.createDataFrame(rdd_split.map(lambda x: (int(x[0]), float(x[1]))), ['Node','Rank'])

rank_df.createOrReplaceTempView('rank_df')

# B) Get Rank of vertex 2
query = spark.sql("SELECT Rank FROM rank_df WHERE rank_df.Node = 2")
query.show()

# C) Get Largest rank
query = spark.sql("SELECT Node,Rank FROM rank_df ORDER BY Rank DESC LIMIT 1")
query.show()

# D) Create Data frame from person.txt
# Read File as RDD
rdd = spark.sparkContext.textFile("HW3/people.txt")

rdd_split = rdd.map(lambda line: line.split())

person_df = spark.createDataFrame(rdd_split.map(lambda x: (int(x[0]), x[1])), ['Node', 'Person'])

person_df.createOrReplaceTempView('person_df')

# E) Join person df and rank df and output to csv file
query = spark.sql("SELECT rank_df.Node AS ID, Person, Rank FROM rank_df, person_df WHERE person_df.Node = rank_df.Node")
query.write.csv("HW3/task3_joined_result", header=True)

spark.stop()