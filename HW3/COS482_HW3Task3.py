from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('sparkSQL').getOrCreate()

# A)
rdd = spark.sparkContext.textFile("HW3/pagerank_output/part-00000")

rdd_split = rdd.map(lambda line: line.split())


rank_df = spark.createDataFrame(rdd_split.map(lambda x: (int(x[0]), float(x[1]))), ['Node','Rank'])

rank_df.createOrReplaceTempView('rank_df')

# B) 
query = spark.sql("SELECT Rank FROM df WHERE df.Node = 2")
print(query.collect())

# C)
max_value = spark.sql("SELECT MAX(Rank) FROM df")
max_rank = max_value.collect()[0]["max_rank"]
query = spark.sql(f'SELECT Node,Rank FROM df WHERE Rank ={max_rank}')
#print(query.collect())

# D)
# Read File as RDD
rdd = spark.sparkContext.textFile("HW3/people.txt")

rdd_split = rdd.map(lambda line: line.split())

person_df = spark.createDataFrame(rdd_split.map(lambda x: (int(x[0]), x[1])), ['Node', 'Person'])

person_df.createOrReplaceTempView('person_df')

# E)
