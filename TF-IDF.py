from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

# def mapFunction(line):
    # result = []
    # for i in range(len(line)-1):
    #     result.append((line[i],line[-1]))
    # print(result)
    # return result
def delimitter_split(line):
    arr = line[0].split(',')
    result = []
    for value in arr:
        words = value.split(" ")
        for i in range(len(words)):
            result.append([words[i],line[1]])    
    return result
    
def key_aggregation(val_1,val_2):
    if type(val_1) == list:
        val_1.append(val_2)
        return val_1
    arr = [val_1,val_2]
    return arr 

input = sc.textFile("file:////home/abdulla/Documents/Homework3/HW3/userdata.txt")
lines_rdd = input.flatMap(lambda x: x.split("\n"))
indexed_rdd = lines_rdd.zipWithIndex() 
lines_rdd = indexed_rdd.flatMap(delimitter_split)

# lines_rdd = lines_rdd.reduceByKey(key_aggregation) 
lines_rdd = lines_rdd.groupByKey().map(lambda x : (x[0], list(x[1])))
# print(lines_rdd.collect())
lines_rdd = lines_rdd.filter(lambda x : str(x[0]).isalpha())
lines_rdd = lines_rdd.sortBy(lambda x: len(x[1]),ascending=False)
max_repeated_length = len(lines_rdd.take(1)[0][1])
# print("max_repeated_length -->>>>",max_repeated_length)

lines_rdd = lines_rdd.filter(lambda x : len(x[1]) >= max_repeated_length) 

final_rdd = lines_rdd.map(lambda x : f'{x[0]} {len(x[1])}')
final_rdd.map(lambda x:"{0}\t{1}".format(x.split(' ')[0],x.split(' ')[1])).saveAsTextFile("/home/abdulla/Documents/Homework3/HW3/Q2_output")

print(final_rdd.collect())