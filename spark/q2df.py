from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import re

import sys


class Problem2:
    def run(self,inputpath,outputpath):
        spark = SparkSession.builder.master("local").appName("problem2").getOrCreate()

        file = spark.sparkContext.textFile(inputpath)

        words = file.flatMap(lambda line: re.split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+", line))

        wordsDF = words.map(lambda x: (x, )).toDF("word string").withColumn("word", lower(col("word"))) 

        wordsDF = wordsDF.filter(length(col("word")) >= 1).filter((col("word").substr(0,1)<='z')) &(col("word").substr(0,1)>='a')

        pairDF = wordsDF.select(wordsDF.word.substr(0,1), length(wordsDF.word)).toDF("letter","length")

        countsDF = pairDF.groupBy("letter")\
            .agg(count("letter").alias("totalCount")
                 ,sum("length").alias("totalLength"))
        avgDF = countsDF.withColumn("ratio", countsDF.totalLength/countsDF.totalCount).select("letter","ratio").orderBy("letter")

        avgDF.write.format("csv").save(outputpath)

        spark.stop()



if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit()
    Problem2().run(sys.argv[1],sys.argv[2])

