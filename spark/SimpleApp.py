from pyspark import SparkContext, SparkConf

# 配置 SparkContext
conf = SparkConf().setAppName("SimpleApplication")
sc = SparkContext(conf=conf)

# 输入文件路径
logFile = "input/pg100.txt"
logData = sc.textFile(logFile, 2).cache()

# 计算包含 'a' 和 'b' 的行数
numAs = logData.filter(lambda line: "a" in line).count()
numBs = logData.filter(lambda line: "b" in line).count()

# 将结果保存到 RDD
resultRDD = sc.parallelize([f"Lines with a: {numAs}", f"Lines with b: {numBs}"])

# 保存到 HDFS 输出目录
output_path = "hdfs://<namenode>:<port>/path/to/output"
resultRDD.saveAsTextFile(output_path)

# 停止 SparkContext
sc.stop()