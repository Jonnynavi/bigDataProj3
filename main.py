#e_commerce table generator

import names
import csv
import random
import datetime

with open("worldcities.csv", encoding="utf8") as csvfile:
    reader = csv.reader(csvfile)
    next(reader, None)
    city_country = [[row[0], row[4]] for row in reader]

#customer_row = [id, name, city, country]
customer_table = [(i, names.get_full_name(), *random.choice(city_country)) for i in range(1, 1001)]

with open("home-and-garden.csv", encoding="utf8") as csvfile:
    reader = csv.reader(csvfile)
    next(reader, None)
    product_category_price = [[row[1], row[5], float(row[19])] for row in reader]

#product_row = [i, name, category, price]
product_table = [(i, *product_category_price[i]) for i in range(len(product_category_price))]

website_names = ("Amazon", "eBay", "Home Depot", "Lowes", "Walmart")

#dictionary of payment types and reasons for failed transactions with payment type
# {payment_type:[reason_for_failures]}
payment_types_failure = {
    "Card":["Invalid CVV", "Invalid Number", "Invalid Expiration Date"], 
    "Internet Banking":["Invalid Routing Number", "Transfer Timeout"], 
    "PayPal":["Wrong Credentials", "insufficient Funds"], 
    "Google Pay":["Wrong Credentials", "insufficient Funds"]
}

def random_date():
    year = random.randrange(2014, 2025)
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

bad_data_indices = []
for i in range(1, 501):
    bad_data_indices.append(random.randint(20*(i-1)+1, 20*i))

with open("e_commerce.csv", "w", newline='', encoding='utf8') as csvfile:
    writer = csv.writer(csvfile)
    for i in range(1, 10001):
        customer = random.choice(customer_table)
        product = random.choice(product_table)
        payment_type = random.choice(list(payment_types_failure.keys()))
        payment_success = "Y" if random.uniform(0, 1) > 0.005 else "N"
        payment_date = random_date()
        if i in bad_data_indices:
            bad_data = random.randrange(1, 5)
            match bad_data:
                case 1:
                    customer = [None, None, None, None]
                case 2:
                    product = [None, None, None, None]
                case 3:
                    payment_type  = None
                case 4:
                    payment_date = '00000000000'

        reason_for_failure = '' if payment_success == "Y" or payment_type == None else random.choice(payment_types_failure[payment_type])
        writer.writerow([
            i,                            #order_id
            customer[0],                  #customer_id
            customer[1],                  #customer_name
            product[0],                   #product_id
            product[1],                   #product_name
            product[2],                   #product_category
            payment_type,                 #payment_type
            random.randrange(1, 10),      #qty
            product[3],                   #price
            payment_date,                 #datetime
            customer[3],                  #country
            customer[2],                  #city
            random.choice(website_names), #e_commerce_website_name
            64537 + i,                    #payment_txn_id
            payment_success,              #payment_txn_id
            reason_for_failure            #failure_reason
        ])
  