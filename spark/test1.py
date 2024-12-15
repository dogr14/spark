from pyspark import SparkConf, SparkContext
from operator import add
# add 通常被用于 reduce 或者reduceByKey 来聚合数据



# SparkContext是Spark应用程序的入口点，用于连接到Spark集群并管理应用程序的资源
# 提供了创建RDD，加载数据以及与spark集群交互的接口
# 每个spark应用程序只能创建一个sparkcontext实例 instance
# 在spark-shell中，sparkcontext对象会自动初始化，并命名为sc
# 可以直接使用sc来操作RDD，例如加载文件并行化集合
auctionRDD = sc.textFile("input/auctiondata.csv") # type: ignore

auctionRDD.map(lambda x : (x[1],1)).reduceByKey(add).collect()
# 
