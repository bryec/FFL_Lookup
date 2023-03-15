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
    query = f"""INSERT INTO store_data (LIC_REGN, LIC_DIST, LIC_CNTY, LIC_TYPE, LIC_XPRDTE, LIC_SEQN, LICENSE_NAME, BUSINESS_NAME, PREMISE_STREET, PREMISE_CITY, PREMISE_STATE, PREMISE_ZIP_CODE, MAIL_STREET, MAIL_CITY, MAIL_STATE, MAIL_ZIP_CODE, VOICE_PHONE)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
    cursor.execute(query, row)

db.commit()
cursor.close()
db.close()

