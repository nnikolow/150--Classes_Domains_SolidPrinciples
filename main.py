import json
import sys
from faker import Faker
import random
import string
import psycopg2


class ETL:
    def __init__(self, ftype, location, sink):
        self.ftype = ftype
        self.location = location
        self.sink = sink

    def source(self, ftype, location):
        if ftype == "file":
            return DataSource.input_data(ftype)
        elif ftype == "json":
            return DataSource.data_source_json_file(ftype)
        else:
            print("source:txt or json")

    def dsink(self, sink):
        if sink == "console":
            return DataSink.console(sink)
        elif sink == "file":
            return DataSink.postgres(sink)
        else:
            print("sink: console or postgres")


class DataSource(ETL):
    def input_data(self):
        fake = Faker()
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

    def data_source_json_file(self):
        def json_file():
            with open(location, encoding='utf-8', errors='ignore') as json_data:
                data = json.load(json_data)
                return data

        def print_to_stdout(*a):
            print(*a, file=sys.stdout)

        x = json_file()
        return x


class DataSink(ETL):
    def console(self):
        print(ftype)

    def postgres(self):
        try:
            print("Opening file...\n")
            with open(location) as file:
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

x = ETL(ftype="file", location="C://Users/Nikolay.Nikolov2//PycharmProjects//pythonProject9", sink='console')
print(x.source(ftype="file", location="C://Users/Nikolay.Nikolov2//PycharmProjects//pythonProject9"))

