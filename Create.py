import mysql.connector
import csv

# Connect to the database (or create it if it doesn't exist)
conn = mysql.connector.connect(
    host="localhost",
    user="ffl",
    password="ffladdresslookup",
    database="FFL_Address"
)
cursor = db.cursor()

def update_database(data):
    for row in data:
        num_columns = len(row)

        # Add a None value for latitude and longitude columns
        row.extend([None, None])

        try:
            cursor.execute("""
                INSERT INTO store_data VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                PREMISE_PHONE=%s,
                LATITUDE=%s,
                LONGITUDE=%s
                """, row * 2)
            print(f"Updated entry for {row[1]}")
        except mysql.connector.Error as e:
            print(f"Error updating entry for {row[1]}: {e}")

        db.commit()

def update_monthly():
    with open('file.txt', 'r') as file:
        reader = csv.reader(file, delimiter=',')
        data = [row for row in reader]
        update_database(data)

if __name__ == "__main__":
    update_monthly()