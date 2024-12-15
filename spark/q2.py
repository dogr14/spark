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

        res = res.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))

        res = res.map(lambda x: (x[0],x[1][1]/x[1][0]))

        res = res.sortByKey().map(lambda x: str(x[0]) + "," + str(x[1]))

        res.saveAsTextFile(outputpath)

        sc.stop()









if __name__ == "__main__":
    if len(sys.argv)!= 3:
        sys.exit(-1)
    q2 = Q2()
    q2.run(sys.argv[1], sys.argv[2])
