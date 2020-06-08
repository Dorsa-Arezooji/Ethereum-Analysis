import pyspark
import json
import time

sc = pyspark.SparkContext()
sqlContext = pyspark.SQLContext(sc)

def is_good_line_tra(line):
    try:
        fields = line.split(',')
        if len(fields) != 7:
            return False
        float(fields[3])
        return True
    except:
        return False

def address_split(x):
    for i in range(len(x[0])):
        return (x[0][i], (x[1], x[2]))

def is_scamming(x):
    if x[1][1][0] == 'Scamming':
        return True

def is_phishing(x):
    if x[1][1][0] == 'Phishing':
        return True

def is_fICO(x):
    if x[1][1][0] == 'Fake ICO':
        return True

path = "/user/dma30/Project"
scams_df = sqlContext.read.option('multiLine', True).json(sc.wholeTextFiles(path).values()).drop('id', 'url', 'name','coin', 'description', 'reporter', 'ip', 'nameservers', 'subcategory')
# ([addresses], category, status)
scams_RDD = scams_df.rdd.map(address_split)
# (address i, (category, status))
lines_tra = sc.textFile('/data/ethereum/transactions')
clean_lines_tra = lines_tra.filter(is_good_line_tra)
address_val_pair = clean_lines_tra.map(lambda l: (l.split(',')[2], (float(l.split(',')[3]), time.strftime("%y.%m", time.gmtime(float(l.split(',')[6]))))))
joined_RDD = address_val_pair.join(scams_RDD)
# (to add, ((val, time), (cat, status)))

# most lucrative form of scam
key_cat = joined_RDD.map(lambda x: (x[1][1][0], x[1][0][0]))
most_lucrative_cat = key_cat.reduceByKey(lambda a,b: a+b).sortBy(lambda x: -x[1]).collect()

for rec in most_lucrative_cat:
    print(rec)

# most lucrative forms of scam vs time
scmamming = joined_RDD.filter(is_scamming).map(lambda x: (x[1][0][1], x[1][0][0])).reduceByKey(lambda a,b: a+b).sortByKey(ascending=True).saveAsTextFile('scamming') # (time, total_val)
phishing = joined_RDD.filter(is_phishing).map(lambda x: (x[1][0][1], x[1][0][0])).reduceByKey(lambda a,b: a+b).sortByKey(ascending=True).saveAsTextFile('phishing')
fICO = joined_RDD.filter(is_fICO).map(lambda x: (x[1][0][1], x[1][0][0])).reduceByKey(lambda a,b: a+b).sortByKey(ascending=True).saveAsTextFile('fICO')
