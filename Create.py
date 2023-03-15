import mysql.connector
import csv

# Connect to the database (or create it if it doesn't exist)
db = mysql.connector.connect(
    host="localhost",
    user="ffl",
    password="ffladdresslookup",
    database="FFL_Address"
)
cursor = db.cursor()

def update_database(data):
    for row in data:
        # Make sure the row has exactly 17 columns
        if len(row) != 17:
            print(f"Skipping entry due to incorrect number of columns: {row}")
            continue

        try:
            cursor.execute("""
                INSERT INTO store_data (LIC_NUM, LIC_NAME, LIC_ISSU_DATE, LIC_EXP_DATE, MAIL_LINE1, MAIL_LINE2, MAIL_CITY, MAIL_STATE, MAIL_ZIP, PREMISE_NAME, PREMISE_STREET, PREMISE_CITY, PREMISE_STATE, PREMISE_ZIP, PREMISE_PHONE)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                LIC_NAME=%s,
                LIC_ISSU_DATE=%s,
                LIC_EXP_DATE=%s,
                MAIL_LINE1=%s,
                MAIL_LINE2=%s,
                MAIL_CITY=%s,
                MAIL_STATE=%s,
                MAIL_ZIP=%s,
                PREMISE_NAME=%s,
                PREMISE_STREET=%s,
                PREMISE_CITY=%s,
                PREMISE_STATE=%s,
                PREMISE_ZIP=%s,
                PREMISE_PHONE=%s
                """, row + row[:15])
            print(f"Updated entry for {row[1]}")
        except mysql.connector.Error as e:
            print(f"Error updating entry for {row[1]}: {e}")

        db.commit()



def read_data_from_file(filename):
    data = []
    with open(filename, 'r') as f:
        reader = csv.reader(f, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        next(reader)  # skip header row
        for row in reader:
            while len(row) < 17:  # Fill missing columns with empty strings
                row.append('')
            data.append(row)
    return data

# Replace 'data.txt' with the actual file name containing the data
data = read_data_from_file('monthly_data.txt')

def update_monthly():
    update_database(data)

if __name__ == "__main__":
    update_monthly()
