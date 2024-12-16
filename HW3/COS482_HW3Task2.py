from pyspark import SparkContext, SparkConf

# Task 2 Page Rank Algorithem
# Kaleb Hannan
# COS 482

# Create Spark
conf = SparkConf().setAppName("PageRank").setMaster("local")
sc = SparkContext(conf=conf)

# Read text file and create graph 
#edges_rdd = sc.textFile("HW3/edges.txt")

# Define the edges as a list of tuples (each tuple is an edge between two vertices)
edges = [(0, 2), (2, 0), (1, 2), (1, 3), (3, 2)]

# Parallelize the edges to create an RDD
edges_rdd = sc.parallelize(edges)

print(edges_rdd.take(5))

# turn each edge into a tuple
edges = edges_rdd.map(lambda line: tuple(map(int,line.split())))

# Get each node
nodes = edges.flatMap(lambda x: x).distinct()

# Set initial rank for each node (set all ranks to 1.0 initially)
rank = nodes.map(lambda node: (node, 1.0))

# Print the contents of the rank RDD
#print("Rank RDD contents:")
#print(rank.collect()) 

# Find nodes with no outgoing nodes
outGoingedges = edges.flatMap(lambda x : x[1]).distinct()
noOutgoing = outGoingedges.subtract(nodes)
#final_ranks = rank.collect()

sc.stop()