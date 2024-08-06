from pyspark import SparkContext, SparkConf
import csv
import datetime

conf = SparkConf().setAppName("Example1").setMaster("local")

sc = SparkContext(conf=conf)

rdd1 = sc.textFile("data_team_1.csv")

def check_numbers(x):
    return x[1].isdigit() and x[3].isdigit() and x[7].isdigit() and x[8].replace('.', '', 1).isdigit() and x[13].isdigit()

# Separate by comma, ignoring double quotes
rdd2 = rdd1.map(lambda x: next(csv.reader([x], delimiter=',', quotechar='"')))

# Remove header
rdd3 = rdd2.filter(lambda x: x[0] != "order_id")

# Filter out results that don't have ints and floats where there should be
rdd4 = rdd3.filter(check_numbers)

# Remap list to have numeric types in certain places
rdd5 = rdd4.map(lambda x: [x[0], int(x[1]), x[2], int(x[3])] + x[4:7] + [int(x[7]), float(x[8])] + x[9:13] + [int(x[13])] + x[14:])

# Filter out results that have "/" in website name
rdd6 = rdd5.filter(lambda x: "/" not in x[12])

# Question 1
rdd_q1_map = rdd6.map(lambda x: (x[5], 1))
rdd_q1_dict = rdd_q1_map.reduceByKey(lambda a, b : (a+b))
# Number of items sold by category in descending order
print(rdd_q1_dict.sortBy(lambda x: -x[1]).collect())
# Question 1 Per Country?
rdd_q1_map_country = rdd6.map(lambda x: (x[10] + ", " + x[5], 1))
rdd_q1_dict_country = rdd_q1_map_country.reduceByKey(lambda a, b : (a+b)).map(lambda x: x[0].split(',') + [x[1]])
# Number of items sold by category per country (unsure how to sort atm)
print(rdd_q1_dict_country.collect())
