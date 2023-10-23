import time
import sys
import psycopg2
import pandas as pd
import csv


username = 'postgres'
password = 'PetroIsFucker55'
database = 'DB_lab_1'
#database = 'DB_lab_2'
#host = 'db'
host = 'localhost'
#port = "5432"
port = "5433"


#print("Container starts")

# визначаємо тип кодування сsv файлу
def detect_encoding(filename):
    encodings = ['utf-8', 'iso-8859-1', 'windows-1252', 'cp1251', 'koi8-r', 'ascii', 'cp437', 'cp850', 'cp866', 'iso-8859-2', 'iso-8859-5', 'iso-8859-15', 'mac_cyrillic', 'windows-1250', 'windows-1252']  # list of encodings to try
    for encoding in encodings:
        try:
            with open(filename, encoding=encoding) as f:
                csv_reader = csv.reader(f)
                next(csv_reader)  # skip header row
                #print(f'Encoding : {encoding}')
                return encoding
        except UnicodeDecodeError:
            continue
    # if none of the encodings work, raise an exception
    raise ValueError(f"Could not detect encoding of file '{filename}'")
# res = detect_encoding('Odata2020File.csv')



def get_columns_type(df):
    column_types = []
    for column in df.columns:
        column_type = df[column].dtype
        if column_type == 'int64':
            column_types.append(f"INTEGER")
        elif column_type == 'float64':
            column_types.append(f"FLOAT")
        else:
            column_types.append(f"TEXT")
    return column_types



def bd_conect():
    for i in range(10):
        try:
            conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)
            print("BD conected")
            return conn

        except:
            print("OperationalError: unable to connect to database")
            time.sleep(5)

    print("End of Program")
    sys.exit()





conn = None
cursor = None



#drop table
while True:
            try:
                conn = bd_conect()
                print(type(conn))
                cursor = conn.cursor()


                # визначаємо тип кодування сsv файлу
                res = detect_encoding('Odata2020File.csv')

                # зчитуємо дані в таблицю
                df = pd.read_csv('Odata2020File.csv', encoding=res, delimiter=';')

                # робимо список з типв клонок
                column_types = get_columns_type(df)

                # create table
                column_names_str = ', '.join(df.columns.map(lambda x: f'"{x}"').tolist())
                fields = df.columns
                result = ", ".join([f"{field} {type}" for field, type in zip(fields, column_types)])
                query_2 = f"""
                Drop table if exists Opendata2020;
                CREATE TABLE IF NOT EXISTS Opendata2020 ({result});
                """

                # print(query_2)
                cursor.execute(query_2)
                print("Table dropped")
                print("Table created")
                conn.commit()
                break


            except:
                print(conn)
                print("OperationalError: not able to connect to database")
                time.sleep(1)
                continue











with open('Odata2020File.csv', 'r', encoding= "windows-1251" ) as file:
    print("Starting importing data")
    reader = csv.reader(file, delimiter=';')
    next(reader) # Пропустити перший рядок (заголовок)
    rows = []
    batch_size = 100

    counter = 0
    #counter_lim = df.shape[0] +1
    counter_lim = 150 + 1
    print(counter_lim - 1)
    # Вставка даних у таблицю
    for row in reader:
        # cursor.execute(query_2)


        # заміна null to None
        for i in range(len(row)):
            if row[i] == "null":
                row[i] = None

        rows.append(row)
        counter +=1



        # if len(rows) >= batch_size:
        #     cursor.executemany("INSERT INTO testtt2 ({}) VALUES ({}) ON CONFLICT DO NOTHING".format(
        #         ', '.join(df.columns), ', '.join(['%s'] * len(df.columns))),
        #         rows)
        #     rows = []


        if len(rows) >= batch_size:
            while True:
                try:

                    if conn is None or conn.closed != 0:
                        conn = bd_conect()
                        cursor = conn.cursor()

                    cursor.executemany("INSERT INTO Opendata2020 ({}) VALUES ({}) ON CONFLICT DO NOTHING".format(
                        ', '.join(df.columns), ', '.join(['%s'] * len(df.columns))),
                        rows)
                    rows = []

                    conn.commit()
                    break
                except:
                    print(conn)
                    print("OperationalError: not able to connect to database 1")
                    time.sleep(1)
                    continue

        elif counter == counter_lim:
            break
        elif counter <= counter_lim - batch_size:
            continue

        if rows:
            while True:
                try:

                    if conn is None or conn.closed != 0:
                        conn = bd_conect()
                        cursor = conn.cursor()

                    cursor.executemany("INSERT INTO Opendata2020 ({}) VALUES ({}) ON CONFLICT DO NOTHING".format(
                        ', '.join(df.columns), ', '.join(['%s'] * len(df.columns))),
                        rows)
                    rows = []

                    conn.commit()
                    break
                except:
                    print(conn)
                    print("OperationalError: not able to connect to database")
                    time.sleep(1)
                    continue





for i in fields:
    if 'Ball100' in str(i):
        query_ball = f"""
                    UPDATE opendata2020
                    SET {i.lower()} = REPLACE({i.lower()}, ',', '.');

                    ALTER TABLE opendata2020
                    ALTER COLUMN {i.lower()} TYPE double precision USING {i.lower()}::double precision;
                    """
        cursor.execute(query_ball)
    else:
        continue




print('Program good worked')
# Збереження змін до бази даних
conn.commit()

# Закриття з'єднання з базою даних
cursor.close()
conn.close()