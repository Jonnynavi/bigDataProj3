import names
import csv
import random
import datetime

with open("worldcities.csv", encoding="utf8") as csvfile:
    reader = csv.reader(csvfile)
    next(reader, None)
    city_country = [[row[0], row[4]] for row in reader]

customer_table = [[i, names.get_full_name(), *random.choice(city_country)] for i in range(1, 1001)]
""" print("##Customers Table")
for c in customer_table:
    print(c) """

with open("home-and-garden.csv", encoding="utf8") as csvfile:
    reader = csv.reader(csvfile)
    next(reader, None)
    product_category_price = [[row[1], row[5], float(row[19])] for row in reader]

product_table = [[i, *product_category_price[i]] for i in range(len(product_category_price))]
print(product_table[2][2])
""" print("##Products Table")
for p in product_table:
    print(p) """

website_names = ["Amazon", "eBay", "Home Depot", "Lowes", "Walmart"]

payment_types_failure = {
    "Card":["Invalid CVV", "Invalid Number", "Invalid Expiration Date"], 
    "Internet Banking":["Invalid Routing Number", "Transfer Timeout"], 
    "PayPal":["Wrong Credentials", "insufficient Funds"], 
    "Google Pay":["Wrong Credentials", "insufficient Funds"]
}


def random_date():
    year = random.randrange(2014, 2024)
    month = random.randrange(1, 13)
    day = -1
    match month:
        case 1 | 3 | 5 | 7 | 8 | 10 | 12:
            #31 day months
            day = random.randrange(1, 32)
        case 4 | 6 | 9 | 11:
            #30 day months
            day = random.randrange(1, 31)
        case 2:
            #february
            if year % 4 == 0:
                #leap year
                day = random.randrange(1, 30)
            else:
                day = random.randrange(1, 29)
    
    hour = random.randrange(0,24)
    minute = random.randrange(0,60)
    second = random.randrange(0,60)
    
    generated_datetime = datetime.datetime(year, month, day, hour, minute, second)
    return generated_datetime

if x % 20 == 0:
    bad_data = random.randrange(1,3)
    match bad_data:
        case 1:
            order_id = None
            payment_date = None
        case 2:
            customer_id = None
            customer_name = None
        case 3:
            qty = None

with open("e_commerce.csv", "w", newline='', encoding='utf8') as csvfile:
    writer = csv.writer(csvfile)
    for i in range(1, 10001):
        customer = random.choice(customer_table)
        product = random.choice(product_table)
        payment_type = random.choice(list(payment_types_failure.keys()))
        payment_success = "Y" if random.uniform(0, 1) > 0.005 else "N"
        columns = [
            i,
            customer[0],
            customer[1],
            product[0],
            product[1],
            product[2],
            payment_type,
            random.randrange(0, 10),
            product[3],
            random_date(),
            customer[3],
            random.choice(website_names),
            64537 + i,
            payment_success,
            '' if payment_success == "Y" else random.choice(payment_types_failure[payment_type])
        ]
        if i % 20 == 0:
            columns[0] 
        else:
            writer.writerow(columns)
