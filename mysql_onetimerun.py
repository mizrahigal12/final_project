import os
# drop database if exists
os.system("mysql -e 'drop database if exists crypto_coins;' ")
print('crypto_coins has been delted')
# create database audiostore
os.system("mysql -e 'create database crypto_coins;' ")
print('crypto_coins has been created')
# show databases
os.system("mysql -e 'show databases;'")
# create table crypto_coins_raw_data if not exists
os.system("mysql -e 'USE crypto_coins;'")
os.system("mysql -e 'CREATE TABLE if not exists crypto_coins.crypto_coins_raw_data (\
    id INT NOT NULL AUTO_INCREMENT,\
    currency NVARCHAR(10) NOT NULL,\
    rate FLOAT NOT NULL,\
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\
    PRIMARY KEY (id)\
\
);'")
os.system("mysql -e 'CREATE TABLE if not exists crypto_coins_test.crypto_coins_agg (\
    id INT NOT NULL AUTO_INCREMENT,\
    currency NVARCHAR(10) NOT NULL,\
    rate FLOAT NOT NULL,\
    ts TIMESTAMP,\
    average_rate FLOAT NOT NULL,\
    min_rate FLOAT NOT NULL,\
    max_rate FLOAT NOT NULL,\
    insert_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\
    PRIMARY KEY (id)\
\
);'")


os.system("mysql -e 'CREATE TABLE if not exists crypto_coins_test.test (\
    message NVARCHAR(10) NOT NULL\
);'")

print('crypto_coins_raw_data table has been created')


print('just test')
print('just test22')