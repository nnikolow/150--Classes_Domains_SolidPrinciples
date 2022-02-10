import json
import sys
from faker import Faker
import random
import string
import psycopg2

'''
Data Source: Simulation and File
'''


# simulated random data

class DataSource:
    def data_source_simulation(self):
        fake = Faker()

        # random json generator
        def input_data(self):
            def rand_key():
                num = str(random.randint(0, 10))
                characters = ''.join(random.choice(string.ascii_uppercase) for i in range(3))
                result = num + characters
                return result
            rand_data = {'key': rand_key(),
                        'value': round(random.uniform(0, 50), 1),
                        'ts': str(fake.date_time())}

            with open('rand_data.json', 'w') as fp:
                json.dump(rand_data, fp)
            return rand_data

        file = input_data("file")
        return file

    # existing json file
    def data_source_json_file(self):
        def json_file():
            with open('json_template', encoding='utf-8', errors='ignore') as json_data:
                data = json.load(json_data)
                return data

        def print_to_stdout(*a):
            print(*a, file=sys.stdout)

        x = json_file()
        return x


'''
Data Sink: Postgres
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
        try:
            print("Opening file...\n")
            with open('rand_data.json') as file:
                data = file.read()

            query_sql = """
            insert into table1 select * from
            json_populate_recordset(NULL::table1, %s);
            """

            self.cursor.execute(query_sql, (data,))
            self.connection.commit()
            print("File successfully loaded! \n")
        except psycopg2.errors.InvalidParameterValue:
            print("Error in the JSON file -> reformat the file and try again.")


'''
Execution Logic
'''

while True:
    data_source = input("Hello, which data source do you want to use? \n Simulation (s) or File (f)?\n")

    if data_source == "s":
        while True:
            data_sink = input(
                "You've chosen Simulation! Where do you want to send the data? Console (c) or Postgres (p)?\n")
            if data_sink == "c":
                r = DataSource()
                print(r.data_source_simulation())
                break
            elif data_sink == "p":
                p = DataSinkPostgres()
                p.simulation()
                break
            else:
                print("Please, enter 'c' for Console or 'p' for Postgres.\n")
        break

    elif data_source == "f":
        while True:
            data_sink = input("You've chosen File! Where do you want to send the data? Console (c) or Postgres (p)?\n")
            if data_sink == "c":
                r = DataSource()
                print(r.data_source_json_file())
                break
            elif data_sink == "p":
                p = DataSinkPostgres()
                p.file()
                break
            else:
                print("Choose an option for the data sink between 'c' for Console and 'p' for Postgres")
        break
    else:
        print("Please, enter 's' for Simulation of 'f' for File!\n")

