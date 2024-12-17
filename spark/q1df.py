from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import sys

class Problem1:
    def run(self,inputpath,outputpath):
        spark = SparkSession.builder.master("local").appName("p1df").getOrCreate()

        fileDF = spark.read.text(inputpath)

        wordsDF = fileDF.selectExpr("explode(split(value,' ')) as word").withColumn("word", lower(col("word")))

        '''
        selectExpr 运行直接使用SQL表达式对列进行操作.
        explode(split(value,' '))
            split:将value 列 输入文件的每行文本 按空格' '分割成单词数组
            explode 将分割后的数组展开成多行 每个单词占一行
        结果:生成一列word,每行包含一个单词
        withColumn 用于创建一个新列或者替换现有列
        lower(col("word)) 将word列中的所有单词转换为小写
        col("word") 引用列 word
        '''
        wordsDF = wordsDF.filter(length(col("word")) >=1).filter((col("word").substr(0,1)<='z')&(col("word").substr(0,1)>='a'))
        # substr 用于截取字符串的子串
        letterDF = wordsDF.withColumn("word", col("word").substr(0,1)).toDF("letter")
        countsDF = letterDF.groupBy("letter").count().orderBy("letter")
        countsDF.write.format("csv").save(outputpath)
        spark.stop()



if __name__ == "__main__":
    if len(sys.argv)!=3:
        sys.exit(-1)
    Problem1().run(sys.argv[1],sys.argv[2])