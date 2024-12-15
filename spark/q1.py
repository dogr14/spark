from pyspark import SparkConf,SparkContext
from operator import add
import sys




class Q1:
    def run(self,inputPath,outputPath):
        conf = SparkConf().setAppName("Q1")
        sc = SparkContext(conf=conf)
        textfile = sc.textFile(inputPath)
        wordflat = textfile.flatMap(lambda line:line.split(" "))
        lowerword = wordflat.map(lambda word:word.lower())
        wordfilteroneword  = lowerword.filter(lambda word:len(word)>=1)
        wordfilternon_alpha = wordfilteroneword.filter(lambda x: True if x[0] >= 'a' and x[0] <= 'z' else False)
        #判断是否是字母
        word= wordfilternon_alpha.map(lambda word:(word[0],1)).reduceByKey(add)
        # 计算以该字母开头的单词的数量
        result = word.sortByKey()

        result.saveAsTextFile(outputPath)

        sc.stop()



if __name__ == "__main__":
    # 检查命令行参数
    if len(sys.argv) != 3:
        print("Usage: spark-submit <script> <input_path> <output_path>")
        sys.exit(-1)

    # 运行 Q1 的 run 方法
    Q1().run(sys.argv[1], sys.argv[2])