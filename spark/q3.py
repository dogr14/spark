from pyspark import SparkContext, SparkConf
import sys
from typing import List, Tuple, Set
from pyspark.rdd import RDD
Id = 0
PostId = 1
VoteTypeID = 2
UserId = 3
CreationDate = 4

    
class Problem3:
    
    
    def Question1(self, fields):
       
        pairs = fields.map(lambda x: (x[VoteTypeID], set([x[PostId]]))) # type: RDD
        
        res = pairs.reduceByKey(lambda x, y: x | y)
        res = res.map(lambda x: (x[0], len(x[1]))).sortBy(lambda x: x[1], ascending =False)
        res = res.take(5)
        for r in res:
            print(f"{r[0]}\t{r[1]}") 

    
    def Question2(self, fields):
        
        pairs = fields.map(lambda x: (x[PostId], set([x[UserId]])))# type: RDD
        
        # pairs = pairs.map(lambda x: x if None not in x[1] else (x[0], x[1].remove(None)))
        pairs = pairs.map(lambda x: x if "" not in x[1] else (x[0], x[1].remove("")))
        # 去除空字段
        pairs = pairs.filter(lambda x: True if x[1] else False)
        # 删除所有没有用户关注的帖子
        res = pairs.reduceByKey(lambda x, y: x | y)

        res = res.filter(lambda x: x[1] and len(x[1]) > 10 )
        res = res.map(lambda x: (x[0], sorted([int(item) for item in list(x[1])])))
        # res = res.sortByKey(keyfunc=lambda x:int(x))
        res = res.sortBy(lambda x:int(x[0]))
        res = res.map(lambda x: f"{x[0]}#{','.join([str(item) for item in x[1]])}")
        
        res.foreach(lambda x: print(x))



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Wrong inputs")
        sys.exit(-1)
  
    conf = SparkConf().setAppName("problem3")
    sc = SparkContext(conf=conf)
    textFile = sc.textFile(sys.argv[1])
    fields = textFile.map(lambda line: line.split(","))
    p3 = Problem3()
    p3.Question1(fields)
    p3.Question2(fields)
    sc.stop()
