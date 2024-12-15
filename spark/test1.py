from pyspark import SparkConf, SparkContext
from operator import add
# add 通常被用于 reduce 或者reduceByKey 来聚合数据



# SparkContext是Spark应用程序的入口点，用于连接到Spark集群并管理应用程序的资源
# 提供了创建RDD，加载数据以及与spark集群交互的接口
# 每个spark应用程序只能创建一个sparkcontext实例 instance
# 在spark-shell中，sparkcontext对象会自动初始化，并命名为sc
# 可以直接使用sc来操作RDD，例如加载文件并行化集合
sc = SparkContext("local","test")

auctionRDD = sc.textFile("input/auctiondata.csv") # type: ignore

auctionRDD.map(lambda x : (x[1],1)).reduceByKey(add).collect()
# lambda是python中的一种用于创建匿名函数的关键字。它可以让你在一行内快速定义一个
# 简单的函数 lambda 参数1， 参数2 ，..:表达式
'''
这是一个多行注释
可以用于解释复杂逻辑或代码块的功能
Transformation 
将一个RDD转换成另一个RDD
特点： 惰性执行 Lazy 转换操作不会立即执行，而是记录下所需的操作步骤（在DAG有向无环图中增加步骤
每次Transformation都会返回一个新的RDD，原数据不变
常用于数据的过滤 映射 分组 合并 去重


map(func) 对RDD中每个元素应用func,返回一个新的RDD rdd.map(lambda x:x*2)
flatMap(func) 类似map,但每个元素可以映射为0或多个元素,返回扁平化的RDD rdd.flatMap(lambda x:x.split(" "))
filter(func) 返回满足func条件的元素组成的RDD rdd.filter(lambda x:x>10)
distinct() 返回去重后的RDD
union(other) 合并两个RDD,返回包含两个RDD所有元素的新RDD
intersection
subtract 返回只在第一个RDD中存在的元素
groupByKey() 按键值对分组,适用于键值对RDD  rdd.groupByKey()
reduceByKey(func) 对每个键的值使用func进行聚合，适用于键值对RDD。 rdd.reduceBykey(lambda a,b:a+b)
sortBy(fuc) 根据func对RDD排序，返回新的RDD 
coalesce(num) 减少分区数

Action
Action操作会触发Spark对整个DAG有向无环图的执行
转换操作会在Action被调用时依次执行

返回结果:返回给驱动程序 Driver ,或者将结果保存到存储中 HDFS
常用于 数据收集,统计,输出
collect() 收集RDD的所有元素到驱动程序中,适合小数据量
count() 返回RDD中的元素个数
take(n) 返回RDD中的前n个元素
first() 返回RDD的第一个元素
reduce(fuc) 使用func聚合RDD中的所有元素 rdd.reduce(lambda a,b:a+b)
foreach(fuc) 对RDD的每个元素应用func,通常用于输出或副作用操作。 rdd.foreach(lambda x:print(x))
countByKey() 对键值对RDD 统计每个键的出现次数,返回一个字典
'''

# auctionRDD.map(lambda x:(x[0],1)).reduceByKey(add).reduce(lambda x,y:x if x[1]>y[1] else y)


result = auctionRDD.map(lambda x:(x[0],1)).reduceByKey(add)

resultsort = result.sortBy(lambda x:x[1])

final = resultsort.take(5)