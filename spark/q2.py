from pyspark import SparkConf,SparkContext
from operator import add
import sys
import re


class Q2:
    def run(self,inputpath,outputpath):
        conf = SparkConf().setAppName("Q2")
        sc = SparkContext(conf=conf)

        textfile = sc.textFile(inputpath)

        wordflat = textfile.flatMap(lambda word:re.split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+", word))

        wordlowercase = wordflat.map(lambda word: word.lower())

        vaildword1 = wordlowercase.filter(lambda word:len(word)>=1)

        vaildword = vaildword1.filter(lambda word: word.isalpha())

        res = vaildword.map(lambda word : (word[0],(1,len(word))))
        # 以该字母为开头的单词 word[0]   以该字母为开头的单词出现了多少次word[1][0] 以该字母开头的单词的长度 word[1][1]
        res = res.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))
        # 流式数据 ，
        res = res.map(lambda x: (x[0],x[1][1]/x[1][0]))

        res = res.sortByKey().map(lambda x: str(x[0]) + "," + str(x[1]))

        res.saveAsTextFile(outputpath)

        sc.stop()









if __name__ == "__main__":
    if len(sys.argv)!= 3:
        sys.exit(-1)
    q2 = Q2() 
    # 在 Python 中，实例化是指通过类创建类的对象（实例）的过程。实例化后，可以通过实例访问类的方法和属性。
    q2.run(sys.argv[1], sys.argv[2])
