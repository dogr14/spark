from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import DataFrame

import sys


class Q3:
    def q2(self,fields):
        puPairDF = fields.filter(col("VoteTypeId") == "5").select("PostId", "UserId").distinct().withColumn("PostId", col("PostId").cast("int")).withColumn("UserId", col("UserId").cast("int"))#type:DataFrame
        countsDF = puPairDF.groupBy("PostId").agg(collect_list("UserId").alias("userList"), count("PostId").alias("count")).filter(col("count") > 10)
        resDF = countsDF.select("PostId", "userList").withColumn("userList", sort_array(col("userList")))
        resDF.foreach(lambda x: print(str(x.PostId)+"#"+str(x.userList)))

    def q1(self,fields):
        pairDF = fields.select("VoteTypeId","PostId").distinct() #type:DataFrame
        countsDF = pairDF.groupBy("VoteTypeId").count().toDF("VoteTypeId","num").orderBy(-col("num"))
        res = countsDF.take(5)
        for x in res:
            print(str(x.VoteTypeId) + "\t" + str(x.num))
if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(-1)
    
    inputfile = sys.argv[1]
    spark = SparkSession.builder.master("local").appName("problem3").getOrCreate()
    fields = spark.read.csv(inputfile).toDF("Id","PostId","VoteTypeId","UserId","CreationDate")

    p3 = Q3()
    p3.q2(fields)
    p3.q1(fields)

    spark.stop()
