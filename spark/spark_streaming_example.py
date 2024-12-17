from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# 初始化 SparkContext 和 StreamingContext
sc = SparkContext()
ssc = StreamingContext(sc, 5)  # 批次间隔设置为 5 秒

# 监听本地端口 9999
lines = ssc.socketTextStream("192.168.56.151", 9999)

# 进行词频统计
words = lines.flatMap(lambda line: line.split(" "))
word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# 打印输出
word_counts.pprint()

# 启动流式计算
ssc.start()
print("Spark Streaming job started. Waiting for data...")

# 持续运行，直到手动停止
ssc.awaitTermination()
