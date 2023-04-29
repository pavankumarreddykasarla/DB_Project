import numpy as np # linear algebra
import pandas as pd
import csv
import sys

data = (r"C:\Users\pavan\OneDrive\Desktop\sem1\Project-DB\Google-Playstore.csv\Google-Playstore.csv");

df = pd.read_csv(data, index_col=False)

# Remove null values from App Name
App_Name_null_values = df['App Name'].isnull()
df = df[~App_Name_null_values]
# Replace Currency null values with NNN
df['Currency'] = df['Currency'].fillna('NNN')
# delete null values from Size column as size has only few data which is null.
Size_null_values = df['Size'].isnull()
df = df[~Size_null_values]
# delete null values from developer Id
Developer_Id_null_values = df['Developer Id'].isnull()
df = df[~Developer_Id_null_values]
# delete null values from devloper Email.
Developer_Email_null_values = df['Developer Email'].isnull()
df = df[~Developer_Email_null_values]
# delete Rating and rating count if it has null values.
Rating_null_values = df['Rating'].isnull()
Rating_count_null_values = df['Rating Count'].isnull()
df = df[~Rating_null_values]
df = df[~Rating_count_null_values]
#delete Minimum_Android null values as we cannot insert the random data
Minimum_Android_null_values = df['Minimum Android'].isnull()
df = df[~Minimum_Android_null_values]
#insert random value into the developer as we have 33% null values in this column
df['Developer Website'].fillna('N/A', inplace=True)
#print(df.count());
#delete release date becuase we can replace with any random date and only 3% of data is null.
Released_null_values = df['Released'].isnull()
df = df[~Released_null_values]
#insert random value into the privacy policy as we have 18% missing values in this column
df['Privacy Policy'].fillna('N/A', inplace=True)

#Data Preprocessing
# Filter the rows where CONTENT WRITING is Everyone or Teen
df = df.loc[df['Content Rating'].isin(['Everyone', 'Teen'])];
#print(df.count());
#dropping these columns as we dont have any use and there are lot of null values. 
df = df.drop(['Privacy Policy', 'Scraped Time', 'Free', 'Developer Website'], axis=1)

print(df.isnull().sum());
duplicates = df[df.duplicated()]

if duplicates.empty:
    print('No duplicates found in the dataset')
else:
    print('Duplicates found in the dataset:')
    print(duplicates)

# convert the 'App' column to string data type
df['App Name'] = df['App Name'].astype(str)
#df['Category'] = df['Category'].astype(str)
df['Category'] = df['Category'].astype(str)
#print(df['Category'].unique())
boolean = df['App Name'].duplicated().any()
#print(boolean);
# Print the datatypes as per the csv downloaded.
#print(df.dtypes)

#print(df.describe());

# REMOVE CONTENT WRITING 