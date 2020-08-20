from pyspark import SparkConf, SparkContext
# 创建 spark 上下文（context）， 并配置
conf = SparkConf().setMaster("local").setAppName("practice_RDD")  # 当前阶段 setMaster 就填 local 本地模式， appName 随便写
sc = SparkContext(conf = conf)  # sc 这个变量是使用 Spark 的入口， 如果使用 shell， shell 会自动帮你把这两部做了
record = [('z3212321',66),('z3212321',77),('z3212321',77),
          ('z5672322',74),('z4212331',98),('z4212331',87),
          ('z4212331',57),('z4212331',62),('z3212431',78),('z3212431',70)]
student_rdd = sc.parallelize(record)


tup = []
def createCombiner(value):
    return (value)

def mergeValue(acc,value):

    tup = (max(acc,value))
    return tup

def mergeCombiners(acc1,acc2):
    return (acc1[0]+acc2[0],acc1[1]+acc2[1])

result = student_rdd.combineByKey(createCombiner,mergeValue,mergeCombiners)

print(result.collect())
