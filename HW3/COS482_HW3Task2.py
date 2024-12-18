from pyspark import SparkContext, SparkConf

# Task 2 Page Rank Algorithem
# Kaleb Hannan
# COS 482


def pageRank(file):
    # Create Spark
    conf = SparkConf().setAppName("PageRankOptimization").setMaster("local[6]").set("spark.default.parallelism", "12").set("spark.sql.shuffle.partitions", "12").set("spark.executor.memory", "20g").set("spark.driver.memory", "8g")
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

    edges = addedges(noOutgoing, nodes, edges)

    # Step 1: Set initial rank for each node (set all ranks to 1.0 initially)
    rank = nodes.map(lambda node: (node, 1.0))

    # get all outgoing nodes neighbors
    neighbors = edges.groupByKey().mapValues(list)

    # Step 4:
    k = 10
    for _ in range (k):

        # Step 2: get rank to add from each node
        contributions = neighbors.join(rank).flatMap(lambda x: [(neighbor, x[1][1] / len(x[1][0])) for neighbor in x[1][0]])

        sumNeighbors  = contributions.reduceByKey(lambda x, y: x + y)
        sumNeighbors = sumNeighbors.union(nodes.subtract(sumNeighbors.keys()).map(lambda node: (node, 0.0)))

        # Step 3: Set New Rank
        rank = sumNeighbors.mapValues(lambda sum : 0.15 + (0.85 * sum))
        # For debuging
        #print("rank: " + str(rank.collect()))
    
    numOfNodes = nodes.count()
    Final_rank  = rank.mapValues(lambda x: round(x/numOfNodes,3))
    Final_rank = Final_rank.sortBy(lambda x: x[0])
    
    # Using saveAsTextFile to save in one text file in the pagerank_output dir
    Final_rank.map(lambda x: str(x[0]) + " " + str(x[1])).coalesce(1).saveAsTextFile("HW3/pagerank_output")

    sc.stop()


def addedges(noOutgoing, nodes, edges):
    nodes_list = nodes.collect()
    # Create new edges
    added_edges = noOutgoing.flatMap(lambda node : [(node, otherNode) for otherNode in nodes_list if node != otherNode] )

    # add new edges to edges rdd
    edges = edges.union(added_edges)
    return edges



pageRank("HW3/edges.txt")