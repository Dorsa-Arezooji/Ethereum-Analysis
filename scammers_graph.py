import pyspark
from graphframes import *
import json
from pyspark.sql.types import *
from pyspark.sql.types import Row

sc = pyspark.SparkContext()
sqlContext = pyspark.SQLContext(sc)

def address_split(x):
	for i in range(len(x[0])):
		return (x[0][i], 1)

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

def is_scammer(x):
	if x[1][0] > 50:
		if x[1][1]/x[1][0] < 0.05:
				return True
	return False

path_scam = "/user/dma30/Project"
scams_df = sqlContext.read.option('multiLine', True).json(sc.wholeTextFiles(path_scam).values()).drop('id', 'url', 'name','coin', 'description', 'reporter', 'ip', 'nameservers', 'subcategory', 'category', 'status')
scammers = scams_df.rdd.map(address_split)
# (address, 1)

path_tra = '/data/ethereum/transactions'
tra = sc.textFile(path_tra).map(lambda l: (l.split(',')[1], l.split(',')[2], (l.split(',')[3], l.split(',')[6])))
# (from, to, (val, time))

# creating the graph (RDDs to DFs)
v_RDD = tra.map(lambda x: x[0]).union(tra.map(lambda x: x[1])).distinct()
v_df = sqlContext.createDataFrame(v_RDD.map(lambda x: Row(**v_f(x))))
e_df = sqlContext.createDataFrame(tra.map(lambda x: Row(**e_f(x))))
G = GraphFrame(v_df, e_df)

# finding the triangles
print('-----------------------------------------------------------------')
triangle = G.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[ca]->(a)").show()

# addresses of wallets used to accumulate scammed ether
degrees_RDD = G.inDegrees.rdd.join(G.outDegrees.rdd).filter(is_scammer) #(v, (in, out))
scammer_wallets = degrees_RDD.join(scammers).saveAsTextFie('sca_acc')
