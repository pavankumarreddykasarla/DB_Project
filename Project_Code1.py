import numpy as np 
import pandas as pd
import csv
import sys
import re
import string
import datetime

#Reading data from the csv file.
data = (r"C:\Users\pavan\Downloads\Google-Playstore_last.csv\Google-Playstore.csv");
#data = (r"C:\Users\pavan\OneDrive\Desktop\sem1\Project-DB\play-store1\Google-Playstore.csv\Google-Playstore.csv");
df = pd.read_csv(data, index_col=False)
# Data cleaning and data transformation for all the columns in the dataset.
App_Name_null_values = df['App Name'].isnull()
df = df[~App_Name_null_values]
# delete null values from Size column as size has only few data which is null.
Size_null_values = df['Size'].isnull()
df = df[~Size_null_values]
# delete null values from developer Id
Developer_Id_null_values = df['Developer Id'].isnull()
df = df[~Developer_Id_null_values]
# delete Rating and rating count if it has null values.
Rating_null_values = df['Rating'].isnull()
Rating_count_null_values = df['Rating Count'].isnull()
df = df[~Rating_null_values]
df = df[~Rating_count_null_values]
# delete null values from devloper Email.
Developer_Email_null_values = df['Developer Email'].isnull()
df = df[~Developer_Email_null_values]
#delete Minimum_Android null values as we cannot insert the random data
Minimum_Android_null_values = df['Minimum Android'].isnull()
df = df[~Minimum_Android_null_values]
#delete release date becuase we can replace with any random date and only 3% of data is null.
Released_null_values = df['Released'].isnull()
df = df[~Released_null_values]

#insert random value into the developer as we have 33% null values in this column
df['Developer Website'].fillna('N/A', inplace=True)

#insert random value into the privacy policy as we have 18% missing values in this column
df['Privacy Policy'].fillna('N/A', inplace=True)

#Data Preprocessing
# Filter the rows where CONTENT WRITING is Everyone or Teen
#df = df.loc[df['Content Rating'].isin(['Everyone', 'Teen'])];

#dropping these columns as we dont have any use and there are lot of null values. 
df = df.drop(['Privacy Policy', 'Scraped Time', 'Free', 'Developer Website'], axis=1)

duplicates = df[df.duplicated()]

# Remove non-ASCII values from AppId column
#df = df.apply(lambda x: ''.join(filter(lambda y: ord(y) < 128, str(x))))

if duplicates.empty:
    print('No duplicates found in the dataset')
else:
    print('Duplicates found in the dataset:')
    print(duplicates)

# converting the column data types and altering the values based on the requirements.
df['App Name'] = df['App Name'].astype(str)
df['Category'] = df['Category'].astype(str)
df['App Name'].duplicated().any();
android_data = df['Minimum Android']
df['Minimum Android'] = android_data.str.split(' ').str[0];
pattern = re.compile(r'^\d+\.\d+\.\d+$');
df.loc[df['Minimum Android'].str.match(pattern), 'Minimum Android'] = df.loc[df['Minimum Android'].str.match(pattern), 'Minimum Android'].str[:3]
df['Minimum Android'].replace('Varies with device', '10.0');
df['Minimum Android'] = df['Minimum Android'].str.strip().replace('Varies with device', '10.0');
df['Minimum Android'] = df['Minimum Android'].str.strip().replace('Varies', '10.0');
df['Minimum Android'] = df['Minimum Android'].str.replace('4.4W', '4.4')
df['Minimum Android'].astype(float);
df['Size'] = df['Size'].str.replace(',', '').str.replace('M', '').str.replace('k', '000').str.replace('G', '').str.replace('Varies with device', '10').astype(float);
df['Installs'] = df['Installs'].str.replace(',', '')
df['Installs'] = df['Installs'].str.replace('+', '').astype(np.int64);
df.to_csv("Cleaned_Data.csv", index=False);

# Renaming the Column names.
#Based on the normalization we are moving the columns to different tables as in csv files.
new_column_names = {
    'App Id': 'AppId',
    'App Name': 'AppName',
    'Rating Count': 'RatingCount',
    'Minimum Android': 'MinimumAndroid',
    'Last Updated': 'LastUpdated',
    'Content Rating': 'ContentRating',
    'Minimum Installs': 'MinimumInstalls',
    'Maximum Installs': 'MaximumInstalls',
    'Developer Id': 'DeveloperId',
    'Developer Email':'DeveloperEmail',
    'Ad Supported': 'AdSupported',
    'In App Purchases': 'InAppPurchases',
    'Editors Choice': 'EditorsChoice'
}
df.rename(columns=new_column_names, inplace=True)
data = df;
print(df.count(), "Done");
# Select columns for App table
app_cols = ['AppId', 'AppName', 'Category', 'Rating', 'RatingCount', 'Size', 'MinimumAndroid', 'Released', 'LastUpdated', 'ContentRating']
app_data = data[app_cols]

# Select columns for Install Data table
install_cols = ['AppId', 'MinimumInstalls', 'MaximumInstalls', 'Installs']
install_data = data[install_cols]

# Select columns for Developer table
developer_cols = ['DeveloperId', 'AppId', 'DeveloperEmail']
developer_data = data[developer_cols]

# Select columns for Price Data table
price_cols = ['AppId', 'Price', 'Currency']
price_data = data[price_cols]

# Select columns for Ad Supported Data table
ad_supported_cols = ['AppId', 'AdSupported', 'InAppPurchases', 'EditorsChoice']
ad_supported_data = data[ad_supported_cols]

# Write data to new CSV files
app_data.to_csv("App_Data.csv", index=False)
install_data.to_csv("Install_Data.csv", index=False)
developer_data.to_csv("Developer_Data.csv", index=False)
price_data.to_csv("Price_Data.csv", index=False)
ad_supported_data.to_csv("Ad_Supported_Data.csv", index=False)

#Reading csv files and 
App_df=pd.read_csv(r"App_Data.csv")
Dev_df=pd.read_csv(r'Developer_Data.csv')
Install_df=pd.read_csv(r'Install_Data.csv')
Ad_df=pd.read_csv(r'Ad_Supported_Data.csv')
Price_df=pd.read_csv(r'Price_Data.csv')

# Remove non-ASCII values from AppId column
App_df['AppId'] = App_df['AppId'].apply(lambda x: ''.join(filter(lambda y: ord(y) < 128, str(x))))
Dev_df['AppId'] = Dev_df['AppId'].apply(lambda x: ''.join(filter(lambda y: ord(y) < 128, str(x))))
Install_df['AppId'] = Install_df['AppId'].apply(lambda x: ''.join(filter(lambda y: ord(y) < 128, str(x))))
Price_df['AppId'] = Price_df['AppId'].apply(lambda x: ''.join(filter(lambda y: ord(y) < 128, str(x))))
Ad_df['AppId'] = Ad_df['AppId'].apply(lambda x: ''.join(filter(lambda y: ord(y) < 128, str(x))))

App_df.dropna(inplace=True);

#df['Released'] = df['Released'].apply(lambda x: datetime.strptime(x, '%d-%b-%y'))

App_df.to_csv('App_Data.csv', index=False);
Dev_df.to_csv('Developer_Data.csv', index=False)
Install_df.to_csv('Install_Data.csv', index=False)
Ad_df.to_csv('Ad_Supported_Data.csv', index=False)
Price_df.to_csv('Price_Data.csv', index=False)
print(App_df.count(), "Done");
