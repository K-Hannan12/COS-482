from pyspark import SparkContext, SparkConf

# Task 2 Page Rank Algorithem
# Kaleb Hannan
# COS 482


def pageRank(file):
    # Create Spark
    conf = SparkConf().setAppName("PageRank").setMaster("local")
    sc = SparkContext(conf=conf)

    edges_rdd = sc.textFile(file)

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

    # get all outgoing nodes and its outgoing neighbors
    neighbors = edges.groupByKey().mapValues(list)

    # Step 4:
    k = 10
    for _ in range (k):

        # Step 2: get rank to add from each node
        contributions = neighbors.join(rank).flatMap(lambda x: [(neighbor, x[1][1] / len(x[1][0])) for neighbor in x[1][0]])

        sumNeighbors  = contributions.reduceByKey(lambda x, y: x + y)
        sumNeighbors = sumNeighbors.union(nodes.subtract(sumNeighbors.keys()).map(lambda node: (node, 0.0)))

        # Step 3: Set New Rank
        rank = sumNeighbors.mapValues(lambda sum : round(0.15 + (0.85 * sum), 3))
        print("rank: " + str(rank.collect()))
    
    numOfNodes = nodes.count()
    Final_rank  = rank.mapValues(lambda x: x/numOfNodes)
    print(Final_rank.collect())

    sc.stop()


def addedges(noOutgoing):
    return noOutgoing


pageRank("HW3/edges.txt")