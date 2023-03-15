import csv
import mysql.connector

def read_data_from_file(filename):
    data = []
    with open(filename, 'r') as f:
        reader = csv.reader(f, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        next(reader)  # skip header row
        for row in reader:
            data.append(row)
    return data

data = read_data_from_file('monthly_data.txt')

db = mysql.connector.connect(
    host="localhost",
    user="ffl",
    password="ffladdresslookup",
    database="FFL_Address"
)
cursor = db.cursor()

for row in data:
    # Assuming columns in the table are: col1, col2, col3, ..., col16
    # Adjust the number of columns and their names as needed.
    query = f"""INSERT INTO table_name (col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    cursor.execute(query, row)

db.commit()
cursor.close()
db.close()
