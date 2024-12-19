from pyspark import SparkContext, SparkConf
import sys
import math

class Project2:           
    def run(self, inputPath, outputPath, stopwords, k):
        conf = SparkConf().setAppName("project2_rdd").setMaster("local")
        sc = SparkContext(conf=conf)
        lines = sc.textFile(inputPath)
        data = lines.map(lambda line: line.strip().split(',', 1)).filter(lambda x: len(x) == 2).map(lambda x: (x[0][:4], x[1].strip().split()))
        terms = data.flatMap(lambda x: x[1])
        term_counts = terms.map(lambda t: (t, 1)).reduceByKey(lambda a, b: a + b)
        sorted_terms = term_counts.sortBy(lambda x: (-x[1], x[0]))
        top_n_terms = sorted_terms.take(int(stopwords))
        stopwords_set = set([t[0] for t in top_n_terms])
        broadcast_stopwords = sc.broadcast(stopwords_set)
        data_filtered = data.map(lambda x: (x[0], [t for t in x[1] if t not in broadcast_stopwords.value]))
        headlines_per_year = data_filtered.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)
        headlines_per_year_dict = headlines_per_year.collectAsMap()
        # {'2003': 3, '2004': 3, '2020': 4}
        # TF
        term_year_counts = data_filtered.flatMap(lambda x: [((x[0], t), 1) for t in x[1]]).reduceByKey(lambda a, b: a + b)
        # DF
        term_year_headlines = data_filtered.flatMap(lambda x: [((x[0], t), 1) for t in set(x[1])]).reduceByKey(lambda a, b: a + b)
        # {(year, term):(TF, DF)}
        joined = term_year_counts.join(term_year_headlines)
        term_weights = joined.map(lambda x: (x[0][1], math.log10(x[1][0]) * math.log10(headlines_per_year_dict[x[0][0]] / x[1][1])))
        term_weight_sum_count = term_weights.map(lambda x: (x[0], (x[1], 1))).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        term_avg_weights = term_weight_sum_count.mapValues(lambda x: x[0] / x[1])
        sorted_terms = term_avg_weights.sortBy(lambda x: (-x[1], x[0]))
        top_k_terms = sorted_terms.take(int(k))
        output = ["{}\t{}".format(t[0], t[1]) for t in top_k_terms]
        sc.parallelize(output).coalesce(1).saveAsTextFile(outputPath)
        sc.stop()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])

