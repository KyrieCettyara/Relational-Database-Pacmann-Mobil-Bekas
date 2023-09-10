import random
import csv
import uuid
import psycopg2
from psycopg2 import sql
from faker import Faker


connection = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="root"
)

fake = Faker('id_ID')


def generate_user():
    # Generate users dummy data
    records = 100  # Number of records to generate
    header = ['user_id', 'name', 'contact', 'city_id']
    usersData = []

    #read the city data from csv
    with open('../Pacmann/Relational Database/dummy_data/city.csv', 'r') as city_file:
        city_reader = csv.DictReader(city_file)
        cities = list(city_reader)

        #append user data using the location as city_id
        for i in range(records):
            city = random.choice(cities)
            city_id = city['kota_id']
            usersData.append([i+1, fake.name(), fake.msisdn(), city_id])

    #save the users data as csv
    filename = '../Pacmann/Relational Database/dummy_data/users.csv'
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(usersData)


def generate_adv():
    #read the cars data from csv
    cars= []
    with open('../Pacmann/Relational Database/dummy_data/car_product.csv', 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            cars.append(row)

    #read the users data from csv
    users = []
    with open('../Pacmann/Relational Database/dummy_data/users.csv', 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            users.append(row)

    #Create the advertisement data and put in list
    header = ['adv_id', 'car_id', 'user_id', 'color', 'mileage_in_km', 'bid_flag', 'post_date']
    advs = []
    for i in range(1, 101):  
        ad_id = i
        car_id = random.randint(1, 50)
        user_id = random.choice(users)['user_id']
        color = fake.color_name()
        mileage_in_km = random.randint(1, 1000)
        bid_flag = random.choice([True, False])
        post_date = fake.date_between(start_date='-1y', end_date='today')
        advs.append([ad_id, car_id, user_id, color, mileage_in_km,bid_flag,post_date])

    #Insert the list to the csv file
    filename = '../Pacmann/Relational Database/dummy_data/advertisement.csv'
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(advs)


def generate_bid():
    #read the advertisments data from csv 
    advs = []
    with open('../Pacmann/Relational Database/dummy_data/advertisement.csv', 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            advs.append(row)

    #read the users data from csv
    users = []
    with open('../Pacmann/Relational Database/users.csv', 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            users.append(row)

    #Create the bids data
    header = ['adv_id', 'bidder_id',
              'bid_date', 'bid_price']
    data = []
    for i in range(1, 151):  # Generate 150 records
        bid_id = i
        adv_id = random.choice(advs)['adv_id']
        bidder_id = random.choice(users)['user_id']
        bid_date = fake.date_between(start_date='-1y', end_date='today')
        bid_price = random.randint(30_000_000, 900_000_000)
        data.append([adv_id, bidder_id, bid_date, bid_price])

    #Write the list to csv
    filename = '../Pacmann/Relational Database/dummy_data/bids.csv'
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(data)



def import_dummy_dataset(connection, filename, table_name, column_names):
    path = f'../Pacmann/Relational Database/dummy_data/{filename}'

    with open(path, 'r') as file:
        cursor = connection.cursor()

        # Create the COPY SQL statement
        copy_sql = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV, HEADER)").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, column_names))
        )

        
        cursor.copy_expert(copy_sql, file)
        connection.commit()
        cursor.close()

    print(f"{table_name} data berhasil dimasukkan")

generate_user()
generate_adv()
generate_bid()

dummy_dataset_dict = [
    {
        "filename": "users.csv",
        "table_name": "users",
        "column_names": ["user_id", "name", "contact", "city_id"]
    },
    {
        "filename": "advertisement.csv",
        "table_name": "advertisement",
        "column_names": ["adv_id", "car_id", "user_id", "color", "mileage_in_km", "bid_flag", "post_date"]
    },
    {
        "filename": "bids.csv",
        "table_name": "bids",
        "column_names": ["adv_id", "bidder_id", "bid_date", "bid_price"]
    }
]

for i in range(len(dummy_dataset_dict)):
    import_dummy_dataset(connection, dummy_dataset_dict[i]["filename"],
                         dummy_dataset_dict[i]["table_name"], dummy_dataset_dict[i]["column_names"])


# Close the database connection
connection.close()


