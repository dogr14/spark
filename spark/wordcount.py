from pyspark import SparkConf,SparkContext
from operator import add
sc = SparkContext("local","wordcount")

rdd = sc.textFile("input/pg100.txt")

wordcount = rdd.flatMap(lambda line : line.split(" ")).map(lambda word:(word,1)).reduceByKey(add)




'''
spark 实现wordcount

map的功能和特点

map将输入RDD的每个元素通过函数映射为另一个元素 结果是一个一对一的映射
每个输入元素生成的结果会保留原始的结构
input ["hello world", "spark is fun"]
output [['hello', 'world'], ['spark', 'is', 'fun']]

flatMap 也会将输入RDD的每个元素通过函数映射,但会将生成的结果扁平化
结果是一对多的映射 输入一个元素 输出零个 一个或多个元素 并且自动消除嵌套
input ["hello world", "spark is fun"]
output ['hello', 'world', 'spark', 'is', 'fun']


'''