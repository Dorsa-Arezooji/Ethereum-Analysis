import pyspark
import time

sc = pyspark.SparkContext()

def is_good_line(line):
    try:
        fields = line.split(',')
        if len(fields)!=9:
            return False
        float(fields[7])
        float(fields[8])
        return True
    except:
        return False

lines = sc.textFile("/data/ethereum/blocks")
clean_lines = lines.filter(is_good_line)
time_n = clean_lines.map(lambda l: (l.split(',')[7], l.split(',')[8]))
date_n = time_n.map(lambda (t, n): (time.strftime("%y.%m", time.gmtime(float(t))), float(n)))
result = date_n.reduceByKey(lambda a, b: a+b)
result_ordered = result.sortByKey(ascending=True)
final = result_ordered.persist()
final.saveAsTextFile('Part1')
