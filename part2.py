import pyspark

sc = pyspark.SparkContext()

def is_good_line_tra(line):
    try:
        fields = line.split(',')
        if len(fields) != 7:
            return False
        float(fields[3])
        return True
    except:
        return False

def is_good_line_con(line):
    try:
        fields = line.split(',')
        if len(fields) != 5:
            return False
        return True
    except:
        return False

lines_con = sc.textFile('/data/ethereum/contracts')
clean_lines_con = lines_con.filter(is_good_line_con)
address = clean_lines_con.map(lambda l: (l.split(',')[0], 1))

lines_tra = sc.textFile('/data/ethereum/transactions')
clean_lines_tra = lines_tra.filter(is_good_line_tra)
address_val_pair = clean_lines_tra.map(lambda l: (l.split(',')[2], float(l.split(',')[3])))
results = address_val_pair.join(address)
address_val_pair_agg = results.reduceByKey(lambda (a,b), (c,d): (float(a) + float(c), b+d))
top10 = address_val_pair_agg.takeOrdered(10, key = lambda x: -x[1][0])

for rec in top10:
    print('{},{}'.format(rec[0], rec[1][0]))
