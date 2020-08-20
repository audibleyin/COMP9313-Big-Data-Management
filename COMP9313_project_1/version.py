from pyspark import SparkContext, SparkConf
from time import time
import pickle
import submission1

def createSC():
    conf = SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName("C2LSH")
    sc = SparkContext(conf = conf)
    return sc

with open("F:/9313/COMP9313_project_1/toy/hashed_data", "rb") as file:
    data = pickle.load(file)

with open("F:/9313/COMP9313_project_1/toy/hashed_query", "rb") as file:
    query_hashes = pickle.load(file)

alpha_m = 20
beta_n = 10

length = len(data) - 1
sc = createSC()
data_hashes = sc.parallelize([(length - index, x) for index, x in enumerate(data)])
res = submission1.c2lsh(data_hashes, query_hashes, alpha_m, beta_n).collect()
sc.stop()

print('Number of candidate: ', len(res))
print('set of candidate: ', set(res))

# Number of candidate:  14
# set of candidate:  {161, 66, 34, 68, 139, 492, 461, 399, 303, 401, 112, 307, 248, 447}