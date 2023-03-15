import mysql.connector

def retrieve_sample_entries():
    db = mysql.connector.connect(
        host="localhost",
        user="ffl",
        password="ffladdresslookup",
        database="FFL_Address"
    )
    cursor = db.cursor()

    cursor.execute("SELECT * FROM store_data LIMIT 20")
    rows = cursor.fetchall()

    for row in rows:
        print(row)

if __name__ == "__main__":
    retrieve_sample_entries()
