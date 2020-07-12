import os
import psycopg2


from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, \
        StringType, LongType, TimestampType, \
        ShortType, DateType
from pyspark.sql.functions import col



DATASET = os.path.join(os.environ['HOME'], 'Code/spark/autos.csv')
CONN = psycopg2.connect(
    host='localhost',
    database='cars',
    user='imamdigmi',
    password='123')
CUR = CONN.cursor()


def init_spark():
    '''
    Initialize Spark Session
    '''
    spark = SparkSession.builder \
            .master('local[*]') \
            .appName('Simple ETL Job') \
            .getOrCreate()

    print('Spark Initialized', '\n')

    return spark


def loadDFWithoutSchema(spark):
    df = spark \
            .read \
            .format('csv') \
            .option('header', 'true') \
            .load(DATASET)

    return df


def loadDFWithSchema(spark):
    schema = StructType([
        StructField('dateCrawled', TimestampType(), True),
        StructField('name', StringType(), True),
        StructField('seller', StringType(), True),
        StructField('offerType', StringType(), True),
        StructField('price', LongType(), True),
        StructField('abtest', StringType(), True),
        StructField('vehicleType', StringType(), True),
        StructField('yearOfRegistration', StringType(), True),
        StructField('gearbox', StringType(), True),
        StructField('powerPS', ShortType(), True),
        StructField('model', StringType(), True),
        StructField('kilometer', LongType(), True),
        StructField('monthOfRegistration', StringType(), True),
        StructField('fuelType', StringType(), True),
        StructField('brand', StringType(), True),
        StructField('notRepairedDamage', StringType(), True),
        StructField('dateCreated', DateType(), True),
        StructField('nrOfPictures', ShortType(), True),
        StructField('postalCode', StringType(), True),
        StructField('lastSeen', TimestampType(), True)
        ])

    df = spark \
            .read \
            .format('csv') \
            .schema(schema) \
            .option('header', 'true') \
            .load(DATASET)

    print('Data loaded into PySpark', '\n')

    # Returning the DataFrame
    return df


def clean_drop_data(df):
    df_dropped = df.drop('dateCrawled', 'nrOfPictures', 'lastSeen')
    df_filtered = df_dropped.where(col('seller') != 'gewerblich')
    df_dropped_seller = df_filtered.drop('seller')
    df_filtered2 = df_dropped_seller.where(col('offerType') != 'Gesuch')
    df_final = df_filtered2.drop('offerType')

    print('Data transformed', '\n')

    return df_final


def create_table(cursor):
    try:
        sql = '''
        CREATE TABLE IF NOT EXISTS cars (
            name VARCHAR(255) NOT NULL,
            price INTEGER NOT NULL,
            abtest VARCHAR(255) NOT NULL,
            vehicleType VARCHAR(255),
            yearOfRegistration VARCHAR(4) NOT NULL,
            gearbox VARCHAR(255),
            powerPS INTEGER NOT NULL,
            model VARCHAR(255),
            kilometer INTEGER,
            monthOfRegistration VARCHAR(255) NOT NULL,
            fuelType VARCHAR(255),
            brand VARCHAR(255) NOT NULL,
            notRepairedDamage VARCHAR(255),
            dateCreated DATE NOT NULL,
            postalCode VARCHAR(255) NOT NULL);
        '''
        cursor.execute(sql)
        CONN.commit()
        print('Table successfully created...', '\n')
    except:
        print('Something went wrong when creating the table', '\n')


def write_postgresql(df):
    cars = [tuple(x) for x in df.collect()]
    records = ','.join(['%s'] * len(cars))
    sql = '''
        INSERT INTO cars (
            name,
            price,
            abtest,
            vehicleType,
            yearOfRegistration,
            gearbox,
            powerPS,
            model,
            kilometer,
            monthOfRegistration,
            fuelType,
            brand,
            notRepairedDamage,
            dateCreated,
            postalCode)
        VALUES {}
    '''.format(records)
    CUR.execute(sql, cars)
    CONN.commit()
    print('Data inserted into PostgreSQL', '\n')


def get_inserted_data(cursor):
    sql = 'SELECT brand, model, price FROM cars'
    cursor.execute(sql)
    cars = cursor.fetchmany(2)

    print('Printing 2 rows')

    for row in cars:
        print('Brand = ', row[0], )
        print('Model = ', row[1])
        print('Price = ', row[2], '\n')

    cursor.close()


def main():
    spark = init_spark()
    df = loadDFWithSchema(spark)
    df_cleaned = clean_drop_data(df)

    create_table(CUR)
    write_postgresql(df_cleaned)
    get_inserted_data(CUR)

    print('Closing connection', '\n')
    CONN.close()

    print('Done!', '\n')

if __name__ == '__main__':
    main()
