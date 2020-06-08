import pyspark
from graphframes import *
import json
from pyspark.sql.types import *
from pyspark.sql.types import Row

sc = pyspark.SparkContext()
sqlContext = pyspark.SQLContext(sc)

def v_f(x):
	d = {}
	d['id'] = x
	d['name'] = x
	d['attr'] = x
	return d

def e_f(x):
	d = {}
	d['src'] = x[0]
	d['dst'] = x[1]
	d['relationship'] = x[2]
	return d

path_tra = '/data/ethereum/transactions/part-00729-5ef0a9bd-d53d-4fd8-9a96-e33e04c37eed-c000.csv'
tra = sc.textFile(path_tra).map(lambda l: (l.split(',')[1], l.split(',')[2], (l.split(',')[3], l.split(',')[6])))
# (from, to, (val, time))

# creating the graph (RDDs to DFs)
v_RDD = tra.map(lambda x: x[0]).union(tra.map(lambda x: x[1])).distinct()
v_df = sqlContext.createDataFrame(v_RDD.map(lambda x: Row(**v_f(x))))
e_df = sqlContext.createDataFrame(tra.map(lambda x: Row(**e_f(x))))
G = GraphFrame(v_df, e_df)

# finding the triangles
triangle = G.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[ca]->(a)").rdd.saveAsTextFile('triangles')
