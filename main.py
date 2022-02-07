import json
import string
import sys
from faker import Faker
import random
import datetime
import string
import psycopg2

'''
DATA SOURCE : SIMULATION
'''

'''
fake = Faker()

# random key generator
def rand_key():
    num = str(random.randint(0, 10))
    characters = ''.join(random.choice(string.ascii_uppercase) for i in range(3))
    result = num + characters
    return result


# random json generator
def input_data():
    rand_data = {'key': rand_key(),
                    'value': round(random.uniform(0, 50), 1),
                    'ts': str(fake.date_time())}
    print(rand_data)

    with open('rand_data.json', 'w') as fp:
        json.dump(rand_data, fp)
    return rand_data


file = input_data()
'''


'''
DATA SOURCE: JSON FILE
'''


# def json_file():
#     with open('json_template', encoding='utf-8', errors='ignore') as json_data:
#         data = json.load(json_data)
#         return data
#
#
# # def print_to_stdout(*a):
# #     print(*a, file=sys.stdout)
#
#
# x = json_file()
# print(x)
# # print_to_stdout(x)

'''
SINK: POSTGRESQL
'''

connection = psycopg2.connect("dbname=db1 user=postgres password=postgres")
cursor = connection.cursor()
cursor.execute("set search_path to public")

print("Opening file...\n")
with open('json_template') as file:
    data = file.read()
print("File successfully loaded... \n")
query_sql = """
insert into table1 select * from
json_populate_recordset(NULL::table1, %s);
"""

cursor.execute(query_sql, (data,))
connection.commit()
print("File successfully loaded in database.")
