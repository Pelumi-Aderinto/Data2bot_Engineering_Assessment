from fileinput import filename
import boto3
from botocore import UNSIGNED
from botocore.exceptions import ClientError
from botocore.client import Config
import configparser
import psycopg2
import os
import logging
from sql_queries import insert_table_queries
import pandas as pd

# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")
data_download_path = "./dataset"

#table name
tbl_name1 = 'peluader5437_staging.reviews'
tbl_name2 = 'peluader5437_staging.shipments_deliveries'
tbl_name3 = 'peluader5437_staging.orders'
tbl_name4 = 'peluader5437_analytics.agg_public_holiday'
tbl_name5 = 'peluader5437_analytics.agg_shipments'
tbl_name6 = 'peluader5437_analytics.best_performing_product'
prefix = 'analytics_export/peluader5437'
filenames = [f"{data_download_path}/agg_public_holiday.csv", f"{data_download_path}/agg_shipments.csv", f"{data_download_path}/best_performing_product.csv"]

if not os.path.exists(data_download_path):
    os.makedirs(data_download_path)
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
bucket_name = "d2b-internal-assessment-bucket"
response = s3.list_objects(Bucket=bucket_name, Prefix="orders_data")
# for example to download the orders.csv
s3.download_file(bucket_name, "orders_data/orders.csv", f"{data_download_path}/orders.csv")
s3.download_file(bucket_name, "orders_data/reviews.csv", f"{data_download_path}/reviews.csv")
s3.download_file(bucket_name, "orders_data/shipment_deliveries.csv", f"{data_download_path}/shipment_deliveries.csv")


def copy_to_db(cur, conn, tbl_name, file):

    #open the csv file, save it as an object
    my_file = open(file)
    print('file opened in memory')
    
    #upload to db
    SQL_STATEMENT = """
    COPY %s FROM STDIN WITH
        CSV
        HEADER
        DELIMITER AS ','
    """

    cur.copy_expert(sql=SQL_STATEMENT % tbl_name, file=my_file)
    print('file copied to db')
    
    conn.commit()
    print('table {0} imported to db completed'.format(tbl_name))

    return

def insert_tables(cur, conn):
    """
    Parameters:
        cur: This holds the data retrieved from database
        conn: This holds the connection made to the database
    Function:
        To extract info from the staging table, transform and load it in the
        dimensions and fact table that makes up the DWH
    """
    try:
        i = 0
        for query in insert_table_queries:
            print(f"\nStarting insert {i+1}")
            cur.execute(query)
            conn.commit()
            i += 1
            print(f"\tinsert {i} done! ")

    except Exception as e:
        print(e)

def export_to_csv(cur, conn, tbl_name, filename):

    SQL_STATEMENT = """
        SELECT * FROM %s;
    """
    cur.execute(SQL_STATEMENT % tbl_name)
    data = cur.fetchall()
    cols = []
    for elt in cur.description:
        cols.append(elt[0])
    df = pd.DataFrame(data=data, columns=cols)
    df.to_csv(filename, index=False)
    print('file exported to csv')
    
    conn.commit()

    print('table {0} exported to csv completed'.format(tbl_name))
    return

def upload_to_s3(bucket, file_name, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    for filename in file_name:
        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = os.path.basename(filename)
            object_name = f"{prefix}/{object_name}"

        # Upload the file
        s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        try:
            response = s3_client.upload_file(filename, bucket, object_name)
            print('file {0} uploaded to s3 completed'.format(object_name))
        except ClientError as e:
            logging.error(e)
            return False
    return True

def main():
    """
        This makes connection to redshift cluster using already defined parameters,
        - calls the function to load data from AWS S3 bucket into the staging table and
        - calls the function that extract data from staging table and load it into Fact
          and dimensions table 
    """

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                .format(*config['CLUSTER'].values()))
    print('CONNECTED!')

    cur = conn.cursor()

    #upload data to db  
    copy_to_db(cur, conn, tbl_name1,  file=f"{data_download_path}/reviews.csv")
    copy_to_db(cur, conn, tbl_name2,  file=f"{data_download_path}/shipment_deliveries.csv")
    copy_to_db(cur, conn, tbl_name3,  file=f"{data_download_path}/orders.csv")
    insert_tables(cur, conn)
    export_to_csv(cur, conn, tbl_name4, filename=f"{data_download_path}/agg_public_holiday.csv")
    export_to_csv(cur, conn, tbl_name5, filename=f"{data_download_path}/agg_shipments.csv")
    export_to_csv(cur, conn, tbl_name6, filename=f"{data_download_path}/best_performing_product.csv")
    upload_to_s3(bucket_name, file_name=filenames)


    conn.close()


if __name__ == "__main__":
    main()


