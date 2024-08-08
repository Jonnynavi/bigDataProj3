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

# Filter out results that don't have ints and floats where there should be (actually does nothing, but I think it's a good check)
rdd4 = rdd3.filter(check_numbers)

# Remap list to have numeric types in certain places
rdd5 = rdd4.map(lambda x: [x[0], int(x[1]), x[2], int(x[3])] + x[4:7] + [int(x[7]), float(x[8])] + x[9:13] + [int(x[13])] + x[14:])

# Filter out results that have "/" in website name
rdd6 = rdd5.filter(lambda x: "/" not in x[12])

# Question 1 - Part a
rdd_q1_part_a = rdd6.map(lambda x: (x[5], x[7])).reduceByKey(lambda a, b:(a+b)) # Number of sales by category
print(rdd_q1_part_a.sortBy(lambda x: -x[1]).collect())
# Question 1 - Part b Per Country
rdd_q1_part_b = rdd6.map(lambda x: ((x[10], x[5]), x[7])).reduceByKey(lambda a, b: a+b).map(lambda x: (x[0][0], [x[0][1]] + [x[1]])).reduceByKey(lambda a, b: max(a, b, key=lambda x: x[-1]))
print(rdd_q1_part_b.sortBy(lambda x: x[0]).take(5))

#extract month from datetime
def extract_month(datetime_str):
    dt = datetime.datetime.strptime(datetime_str, "%Y-%m-%d %H:%M")
    return dt.month

#Question 2 - Part a
rdd_q2_part_a = rdd6.map(lambda x: ((x[4], extract_month(x[9])), x[7])).reduceByKey(lambda a, b: (a+b)).map(lambda x: (x[0][0], x[0][1], x[1])) # Gives rdd of form (Product name, month, sales)
print(rdd_q2_part_a.sortBy(lambda x: -x[2]).collect())
#Question 2 - Part b Per Country
rdd_q2_part_b = rdd6.map(lambda x: ((x[4], extract_month(x[9]), x[10]), x[7])).reduceByKey(lambda a, b: (a+b)).map(lambda x: (x[0][0], x[0][1], x[0][2], x[1])) # Gives rdd of form (Product name, month, country, sales)
print(rdd_q2_part_b.sortBy(lambda x: -x[3]).collect())

# Question 3 
rdd_q3_city = rdd6.map(lambda x: (x[11], x[7])).reduceByKey(lambda a, b:(a+b)) # Number of sales by city
rdd_q3_country = rdd6.map(lambda x: (x[10], x[7])).reduceByKey(lambda a, b:(a+b)) # Number of sales by country
rdd_q3_website = rdd6.map(lambda x: (x[12], x[7])).reduceByKey(lambda a, b:(a+b)) # Number of sales by website
print(rdd_q3_city.sortBy(lambda x: -x[1]).take(5))
print(rdd_q3_country.sortBy(lambda x: -x[1]).take(5))
print(rdd_q3_website.sortBy(lambda x: -x[1]).take(5))

# Question 4
#extract hour from datetime
def extract_hour(datetime_str):
    dt = datetime.datetime.strptime(datetime_str, "%Y-%m-%d %H:%M")
    return dt.hour

# Count sales per hour per country
rdd_q4 = rdd6.map(lambda x: ((x[10], extract_hour(x[9])), x[7])) \
              .reduceByKey(lambda a, b: a + b)

# Count sales per hour for all countries
rdd_q4_overall = rdd6.map(lambda x: (extract_hour(x[9]), x[7])) \
                     .reduceByKey(lambda a, b: a + b)

# Sort and organize sales per hour per country
rdd_q4_sorted = rdd_q4.map(lambda x: (x[0][0], (x[0][1], x[1]))) \
                      .groupByKey() \
                      .mapValues(lambda hours: sorted(hours, key=lambda x: -x[1]))

# Sort and organize sales per hour overall
rdd_q4_overall_sorted = rdd_q4_overall.map(lambda x: (x[0], x[1])) \
                                      .sortBy(lambda x: -x[1])

# Collect and display top 3 hours with highest traffic for each country
# results = rdd_q4_sorted.collect()
# for country, hours in results:
#     print(f"Country: {country}")
#     for hour, count in hours[:3]:
#         print(f"Hour: {hour}, Sales: {count}")
#     print()

# Collect and display top 3 hours with highest traffic over all countries
# overall_results = rdd_q4_overall_sorted.collect()
# print("Overall Sales Traffic:")
# for hour, count in overall_results[:3]:
#     print(f"Hour: {hour}, Sales: {count}")

# Collect and prepare data for CSV
country_hourly_sales = [(country, hour, sales) for country, hours in rdd_q4_sorted.collect() for hour, sales in hours]
all_country_hourly_sales = rdd_q4_overall_sorted.collect()

# Save to 4.1 to country_hourly_sales CSV
with open("country_hourly_sales.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerow(["Country", "Hour", "Sales"])
    writer.writerows(country_hourly_sales)

# Save 4.2 to all_country_hourly_sales.csv
with open("all_country_hourly_sales.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerow(["Hour", "Sales"])
    writer.writerows(all_country_hourly_sales)