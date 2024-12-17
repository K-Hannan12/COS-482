from pyspark import SparkContext, SparkConf

# Task 2 Page Rank Algorithem
# Kaleb Hannan
# COS 482


def pageRank(edges_rdd):
    # turn each edge into a tuple
    edges = edges_rdd.map(lambda line: tuple(map(int,line.split())))

    # Get each node
    nodes = edges.flatMap(lambda x: x).distinct()

    # Step 0:  Find nodes with outgoing edges
    outGoingedges = edges.flatMap(lambda x : [x[0]]).distinct()

    # subtract the nodes from all of the nodes to get node with no outgoing edges so we can add edges
    noOutgoing = nodes.subtract(outGoingedges)
    addedges(noOutgoing)

    # Step 1: Set initial rank for each node (set all ranks to 1.0 initially)
    rank = nodes.map(lambda node: (node, 1.0))
    #print("Rank: " + str(rank.collect()))

    # get all outgoing nodes and its outgoing neighbors
    neighbors = edges.groupByKey().mapValues(list)
    #print("neighbors: " + str(neighbors.collect()))

    # Step 4:
    k = 10
    for _ in range (k):

        # Step 2: get rank to add from each node
        contributions = neighbors.join(rank).flatMap(lambda x: [(neighbor, x[1][1] / len(x[1][0])) for neighbor in x[1][0]])

        #print("contributions: " + str(contributions.collect()))

        sumNeighbors  = contributions.reduceByKey(lambda x, y: x + y)
        sumNeighbors = sumNeighbors.union(nodes.subtract(sumNeighbors.keys()).map(lambda node: (node, 0.0)))

        #print("sumNeighbors: " + str(sumNeighbors.collect()))
        # Step 3: Set New Rank
        rank = sumNeighbors.mapValues(lambda sum : 0.15 + (0.85 * sum))
        print("rank: " + str(rank.collect()))
    
    numOfNodes = nodes.count()
    Final_rank  = rank.mapValues(lambda x: x/numOfNodes)
    print(Final_rank.collect())


def addedges(noOutgoing):
    return noOutgoing


# Create Spark
conf = SparkConf().setAppName("PageRank").setMaster("local")
sc = SparkContext(conf=conf)

# Read text file and create graph 
edges_rdd = sc.textFile("HW3/edges.txt")
pageRank(edges_rdd)

sc.stop()