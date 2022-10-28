# Implementing Data Warehouse on AWS
# DATA2BOTS DATA ENGINEERING ASSESSMENT
# Project Overview
This project is about a certain company, `ABC Inc.` that wants to extract certain information from the existing data they have. These information they are interested in is to be extracted as tables to an analytics schema in a relational DB first and then exported as a csv file to a certain cloud storage(AWS S3).

# The Schemas Available
if_common Schema: This schema contains all the dimensions tables which include the dim_customers table, dim_addresses table, dim_product table and dim_dates table.

peluader_staging5437 Schema: This contains all the facts tables namely orders table, reviews table and shipment_deliveries table.

peluader_analytics Schema: From the name, this schema contains all the derived and transformed analytics tables from facts and dimensions tables by writing complex sql commands. This essentially holds the information the client is interested in.

# Project Structure
The project contains five files:
1. `test.ipynb` As the name implies, it is a test notebook where all my rough work and unit testing were done.
1. `create_tables.py` drops and creates all tables in postgres DB (i.e, the Staging table and the Analytics table). I run this file to reset my tables each time before I run my ETL scripts.
1. `etl.py` defines the ETL pipeline that downloads/extracts data from S3 bucket, loads it into the respective tables in the staging schema on postgres, transfroms into the analytics table and uploads the analytics table to s3 bucket as a csv file.
1. `sql_queries.py` defines SQL queries that creates the tables and ETL pipeline
1. `dwh.cfg` This essentially holds my credentials

# Project Parameters
You will need to create a configuration file with the file name `dwh.cfg` and the following structure:

```
[CLUSTER]
HOST=<your_host>
DB_NAME=<your_db_name>
DB_USER=<your_db_user>
DB_PASSWORD=<your_db_password>
DB_PORT=<your_db_port>
```

# Project Procedure
1. Define your parameter in `dwh.cfg file`
1. Write the neccessary SQL to get the ETL processes done in `sql_queries.py`
    * start with the staging tabele creation
    * Download the files from s3 bucket to your local directory
    * Be sure the downloaded data in step 2 above is loaded/copied to the staging table from local dir without error
    * Create the analytics table and write sql commands to populate these analytics tables with necessary information.
    * Export the analytics tables as csv file to the data lake on aws
    * `Best practice, use .ipynb file to do this step by step. It helps in error troubleshooting`
1. Run `create_tables.py` every time before running etl.py to clean and create database
1. Run `etl.py` to start ETL pipeline process

# Create Table and SQL queries
The major focus of this project is on the `sql_queries.py`, this is where all tables are created and where the ETL pipeline processes are written. To start, I created and connected to Postgres DB. After connection, I downloaded data from the S3 bucket and copied them to the staging tables on Postgres. 

Once the tables were created (staging and analytics table), I wrote the ETL to load the data into the stage tables and transform into analytics tables.

## Query Examples
Here is the code that `creates one of the staging_tables`
``` sql
     CREATE TABLE peluader5437_analytics.agg_public_holiday (
            ingestion_date date NOT NULL PRIMARY KEY,
            tt_order_hol_jan INT,
            tt_order_hol_feb INT,
            tt_order_hol_mar INT,
            tt_order_hol_apr INT,
            tt_order_hol_may INT,
            tt_order_hol_jun INT,
            tt_order_hol_jul INT,
            tt_order_hol_aug INT,
            tt_order_hol_sep INT,
            tt_order_hol_oct INT,
            tt_order_hol_nov INT,
            tt_order_hol_dec INT 
        );
```

Here is the code that `copies data into the staging_tables in the database`
``` python
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
```

Here is the code that `inserts into one of the tables in the peluader5437.analytics schema(agg_shipments table)`
```sql
INSERT INTO peluader5437_analytics.agg_shipments (
            ingestion_date, tt_late_shipments, tt_undelivered_items)
            SELECT
            now() AS ingestion_date,
            (SELECT
            COUNT(*) FROM (SELECT *  FROM peluader5437_staging.orders so 
            JOIN peluader5437_staging.shipments_deliveries sd ON so.order_id = sd.order_id) t1 WHERE DATE_PART('day', t1.shipment_date::timestamp - t1.order_date::timestamp) >= 6 AND t1.delivery_date IS NULL )  AS tt_late_shipments,
            
            (SELECT
            COUNT(*) FROM (SELECT *  FROM peluader5437_staging.orders so 
            JOIN peluader5437_staging.shipments_deliveries sd ON so.order_id = sd.order_id) t1 WHERE t1.shipment_date IS NULL AND t1.delivery_date IS NULL AND DATE_PART('day', '2022-09-05'::timestamp - t1.order_date::timestamp) >= 15 )  AS tt_undelivered_items
```

NOTE

1. The columns in agg_public_holiday were all pre-defined in the assessment to be NOT NULL. However, this is not true as some of the columns CAN and WILL be NULL due to the fact that the months contains no public holidays in the last years and hence no orders were made ON A PUBLIC HOLIDAY in those months. More specifically, Months of May, June, Sept. and Dec had no orders placed on a public holiday in the last years and hence are NULL. I included these changes by removing the NOT NULL constraint while creating the table.
2. The name of the csv is 'shipment_deliveries.csv' in contrast to what was wriiten in the assessment as 'shipments_deliveries.csv'. I had problem downloading the right file in time.

