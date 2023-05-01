import pandas as pd
import mysql.connector

# Read the input file into a pandas dataframe
df = pd.read_csv(r"C:\Users\pavan\OneDrive\Documents\GitHub\DB_Project\App_Data.csv")
df2= pd.read_csv(r"C:\Users\pavan\OneDrive\Documents\GitHub\DB_Project\Price_Data.csv")
df3= pd.read_csv(r"C:\Users\pavan\OneDrive\Documents\GitHub\DB_Project\Developer_Data.csv")
df4= pd.read_csv(r"C:\Users\pavan\OneDrive\Documents\GitHub\DB_Project\Install_Data.csv")
df5 = pd.read_csv(r"C:\Users\pavan\OneDrive\Documents\GitHub\DB_Project\Ad_Supported_Data.csv");

# Connect to the MySQL database

mydb = mysql.connector.connect(
    host = 'playstore-rds.cblwo5izabz6.us-east-1.rds.amazonaws.com',
    port = '3306',
    database = 'playstoredb',
    user = 'admin',
    password = 'Welcome1'
);

# Connect to the MySQL database
mydb = mysql.connector.connect(
    host='playstore-rds.cblwo5izabz6.us-east-1.rds.amazonaws.com',
    port='3306',
    database='playstoredb',
    user='admin',
    password='Welcome1'
)

# Batch insert the rows into the MySQL table
batch_size = 3000
for i in range(0, len(df), batch_size):
    batch_df = df.iloc[i:i+batch_size]
    rows = [tuple(x) for x in batch_df.to_numpy()]
    cursor = mydb.cursor()
    sql = "INSERT INTO App (AppId, AppName, Category, Rating, RatingCount, Size, MinimumAndroid, Released, LastUpdated, ContentRating) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    try:
        cursor.executemany(sql, rows)
        mydb.commit()
        print(cursor.rowcount, "record(s) inserted.")
    except mysql.connector.Error as error:
        print("Failed to insert records into App table: {}".format(error))
        mydb.rollback()
    cursor.close()


for i in range(0, len(df2), batch_size):
    batch_df = df2.iloc[i:i+batch_size]
    rows = [tuple(x) for x in batch_df.to_numpy()]
    cursor = mydb.cursor()
    sql = "INSERT INTO Price_Data (AppId, Price, Currency) VALUES (%s, %s, %s)"
    try:
        cursor.executemany(sql, rows)
        mydb.commit()
        print(cursor.rowcount, "record(s) inserted.")
    except mysql.connector.Error as error:
        print("Failed to insert records into App table: {}".format(error))
        mydb.rollback()
    cursor.close()


for i in range(0, len(df3), batch_size):
    batch_df = df3.iloc[i:i+batch_size]
    rows = [tuple(x) for x in batch_df.to_numpy()]
    cursor = mydb.cursor()
    sql = "INSERT INTO Developer (DeveloperId, AppId, DeveloperEmail) VALUES (%s, %s, %s)"
    try:
        cursor.executemany(sql, rows)
        mydb.commit()
        print(cursor.rowcount, "record(s) inserted.")
    except mysql.connector.Error as error:
        print("Failed to insert records into App table: {}".format(error))
        mydb.rollback()
    cursor.close()

for i in range(0, len(df4), batch_size):
    batch_df = df4.iloc[i:i+batch_size]
    rows = [tuple(x) for x in batch_df.to_numpy()]
    cursor = mydb.cursor()
    sql = "INSERT INTO Install_Data (AppId, MinimumInstalls, MaximumInstalls, Installs) VALUES (%s, %s, %s, %s)"
    try:
        cursor.executemany(sql, rows)
        mydb.commit()
        print(cursor.rowcount, "record(s) inserted.")
    except mysql.connector.Error as error:
        print("Failed to insert records into App table: {}".format(error))
        mydb.rollback()
    cursor.close()

for i in range(0, len(df5), batch_size):
    batch_df = df5.iloc[i:i+batch_size]
    rows = [tuple(x) for x in batch_df.to_numpy()]
    cursor = mydb.cursor()
    sql = "INSERT INTO Ad_Supported_Data (AppId, Ad_Supported, In_App_Purchases, Editors_Choice) VALUES (%s, %s, %s, %s)"
    try:
        cursor.executemany(sql, rows)
        mydb.commit()
        print(cursor.rowcount, "record(s) inserted.")
    except mysql.connector.Error as error:
        print("Failed to insert records into App table: {}".format(error))
        mydb.rollback()
    cursor.close()

mydb.close()