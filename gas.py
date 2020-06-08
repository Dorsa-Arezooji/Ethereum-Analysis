import pyspark
import time

sc = pyspark.SparkContext()

def is_good_line_tra(line):
    try:
        fields = line.split(',')
        if len(fields) != 7:
            return False
        float(fields[5])
        float(fields[6])
        return True
    except:
        return False

def is_good_line_con(line):
    try:
        fields = line.split(',')
        if len(fields) != 5:
            return False
        float(fields[3])
        return True
    except:
        return False

def is_good_line_bl(line):
    try:
        fields = line.split(',')
        if len(fields)!=9:
            return False
        float(fields[0])
        float(fields[3])
        float(fields[7])
        return True
    except:
        return False

lines_tra = sc.textFile('/data/ethereum/transactions')
clean_lines_tra = lines_tra.filter(is_good_line_tra)
time_pr = clean_lines_tra.map(lambda l: (float(l.split(',')[6]), float(l.split(',')[5])))
date_pr = time_pr.map(lambda (t, pr): (time.strftime("%y.%m", time.gmtime(t)), (pr, 1)))
pr_time = date_pr.reduceByKey(lambda (pr1, n1), (pr2, n2): (pr1 + pr2, n1 + n2)).map(lambda x: (x[0], (x[1][0] / x[1][1])))
final = pr_time.sortByKey(ascending=True)
final.saveAsTextFile('AveGas_T')


lines_con = sc.textFile('/data/ethereum/contracts')
clean_lines_con = lines_con.filter(is_good_line_con)
block_id = clean_lines_con.map(lambda l: (l.split(',')[3], 1))

lines_bl = sc.textFile('/data/ethereum/blocks')
clean_lines_bl = lines_bl.filter(is_good_line_bl)
block_diff_time = clean_lines_bl.map(lambda l: (l.split(',')[0], (int(l.split(',')[3]), int(l.split(',')[6]), time.strftime("%y.%m", time.gmtime(float(l.split(',')[7]))))))
results = block_diff_time.join(block_id).map(lambda (id, ((d, g, t), n)): (t, ((d, g), n)))
final = results.reduceByKey(lambda ((d1, g1), n1), ((d2, g2), n2): ((d1 + d2, g1 + g2), n1 + n2)).map(lambda x: (x[0], (float(x[1][0][0] / x[1][1]), x[1][0][1] / x[1][1]))).sortByKey(ascending=True)
final.saveAsTextFile('Diff_Time')
