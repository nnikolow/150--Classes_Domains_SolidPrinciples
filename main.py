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


def data_source_simulation():
    fake = Faker()

# random "key" generator
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

        with open('rand_data.json', 'w') as fp:
            json.dump(rand_data, fp)
        return rand_data

    file = input_data()
    return file


'''
DATA SOURCE: JSON FILE
'''


def data_source_json_file():
    def json_file():
        with open('json_template', encoding='utf-8', errors='ignore') as json_data:
            data = json.load(json_data)
            return data

    def print_to_stdout(*a):
        print(*a, file=sys.stdout)

    x = json_file()
    return x


'''
SINK: POSTGRESQL
'''


class DataSinkPostgres:

    def __init__(self):
        self.connection = psycopg2.connect("dbname=db1 user=postgres password=postgres")
        self.cursor = self.connection.cursor()
        self.cursor.execute("set search_path to public")

    def file(self):
        print("Opening file...\n")
        with open('json_template2') as file:
            data = file.read()
        print("File successfully loaded! \n")
        query_sql = """
        insert into table1 select * from
        json_populate_recordset(NULL::table1, %s);
        """

        self.cursor.execute(query_sql, (data,))
        self.connection.commit()

    def simulation(self):
        print("Opening file...\n")
        with open('rand_data.json') as file:
            data = file.read()
        print("File successfully loaded! \n")
        query_sql = """
        insert into table1 select * from
        json_populate_recordset(NULL::table1, %s);
        """

        self.cursor.execute(query_sql, (data,))
        self.connection.commit()


# p = DataSinkPostgres()
# p.simulation()


cycle = True
data_source = input("Hello, which data source do you want to use? \n Simulation (s) or File (f)?\n")
while cycle:
    if data_source == "s":
        data_sink = input("You've chosen Simulation! Where do you want to put the data? Console (c) or Postgres (p)?\n")
        while cycle:
            if data_sink == "c":
                result = data_source_simulation()
                print(result)
                cycle = False
            elif data_sink == "p":
                p = DataSinkPostgres()
                p.simulation()
                cycle = False
            else:
                print("Please, choose an option between Console (c) or Postgres (p)")

    elif data_source == "f":
        data_sink = input("You've chosen File! Where do you want to put the data? Console (c) or Postgres (p)?\n")
        while cycle:
            if data_sink == "c":
                result = data_source_json_file()
                print(result)
                cycle = False
            elif data_sink == "p":
                p = DataSinkPostgres()
                p.file()
                cycle = False
            else:
                print("Please, choose an option between Console (c) or Postgres (p)")


