from pyspark import SparkConf,SparkContext
import sys
import math
from operator import add
class TFIDF_computation:

    def TF_IDF(term,year):
        1
        


    def run(self,inputpath,outputpath,n,k):
        conf = SparkConf().setAppName("TFIDF_Computation").setMaster("local")
        sc = SparkContext(conf=conf)
        textfile = sc.textFile(inputpath)

        textfile_split = textfile.map(lambda line:line.split(","))

        pairRDD = textfile_split.map(lambda x: (x[0][:4],x[1].split(" "))) #x[0]: Date  x[1]:news
        
        terms  = pairRDD.flatMap(lambda x: x[1])

        term_counts = terms.map(lambda x: (x,1)).reduceByKey(add)
        # 计算stopwords 根据传入的n的数量选取排名前n个词作为stopwords
        sorted_terms = term_counts.sortBy(lambda x:(-x[1],x[0]))
        top_n_terms = sorted_terms.take(int(n)) # remove stopwords
        stopwordset = set([t[0] for t in top_n_terms])
        broadcast_stopwords = sc.broadcast(stopwordset)

        data_filter = pairRDD.map(lambda x:(x[0],[t for t in x[1] if t not in broadcast_stopwords.value]))
        headlines_per_year = data_filter.map(lambda x:(x[0],1)).reduceByKey(add)
        headlines_per_year_dict = headlines_per_year.collectAsMap()
        #计算TF 在该年单词出现的总次数
        term_year_counts = data_filter.flatMap(lambda x: [((x[0],t),1) for t in x[1]]).reduceByKey(add)
        #在标题中出现的次数 通过set使得不出现重复单词
        term_year_headlines = data_filter.flatMap(lambda x:[((x[0],t),1) for t in set(x[1])]).reduceByKey(add)

        joined = term_year_counts.join(term_year_headlines)
        # 计算TF-IDF
        term_weight = joined.map(lambda x: (x[0][1], math.log10(x[1][0])*math.log10(headlines_per_year_dict[x[0][0]]/x[1][1])))

        term_weight_sum_count = term_weight.map(lambda x:(x[0],(x[1],1))).reduceByKey(lambda a,b :(a[0] + b[0],a[1]+b[1]))

        term_avg_weights = term_weight_sum_count.mapValues(lambda x:x[0]/x[1])

        sorted_terms = term_avg_weights.sortBy(lambda x:(-x[1],x[0]))

        top_k_terms = sorted_terms.take(int(k))

        output = ["{}\t{}".format(t[0],t[1]) for t in top_k_terms]

        sc.parallelize(output).coalesce(1).saveAsTextFile(outputpath)

        
        sc.stop()


if __name__ == "__main__":
    if len(sys.argv)!=5:
        sys.exit(-1)

    TFIDF_computation().run(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4])

