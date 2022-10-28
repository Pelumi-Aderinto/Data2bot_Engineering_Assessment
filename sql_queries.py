
# DROP TABLES

reviews_table_drop = "drop table if exists peluader5437_staging.reviews"
shipments_deliveries_table_drop = "drop table if exists peluader5437_staging.shipments_deliveries"
orders_table_drop = "drop table if exists peluader5437_staging.orders"
agg_public_holiday_table_drop = "drop table if exists peluader5437_analytics.agg_public_holiday"
agg_shipments_table_drop = "drop table if exists peluader5437_analytics.agg_shipments"
best_performing_product_table_drop = "drop table if exists peluader5437_analytics.best_performing_product"


# CREATE TABLES

reviews_table_create= ("""
    CREATE TABLE peluader5437_staging.reviews (
            review INT NOT NULL,
            product_id INT NOT NULL
        );
""")

orders_table_create ="""CREATE TABLE peluader5437_staging.orders(
                        order_id INT NOT NULL PRIMARY KEY,
                        customer_id INT NOT NULL,
                        order_date date NOT NULL,
                        product_id varchar NOT NULL,
                        unit_price INT NOT NULL,
                        quantity INT NOT NULL,
                        amount INT NOT NULL
                    );"""




shipments_deliveries_table_create = (""" 
     CREATE TABLE peluader5437_staging.shipments_deliveries (
            shipment_id INT NOT NULL PRIMARY KEY,
            order_id INT NOT NULL,
            shipment_date date NULL,
            delivery_date date NULL
        );
""")



agg_public_holiday_table_create = (""" 
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
""")

agg_shipments_table_create = (""" 
     CREATE TABLE peluader5437_analytics.agg_shipments (
            ingestion_date date NOT NULL PRIMARY KEY,
            tt_late_shipments INT NOT NULL,
            tt_undelivered_items INT NOT NULL
        );
""")

best_performing_product_table_create = (""" 
     CREATE TABLE peluader5437_analytics.best_performing_product (
            ingestion_date date NOT NULL PRIMARY KEY,
            product_name varchar NOT NULL,
            most_ordered_day date NOT NULL,
            is_public_holiday bool NOT NULL,
            tt_review_points INT NOT NULL,
            pct_one_star_review float NOT NULL,
            pct_two_star_review float NOT NULL,
            pct_three_star_review float NOT NULL,
            pct_four_star_review float NOT NULL,
            pct_five_star_review float NOT NULL,
            pct_early_shipments float NOT NULL,
            pct_late_shipments float NOT NULL
        );
""")

agg_public_holiday_table_insert = '''INSERT INTO peluader5437_analytics.agg_public_holiday (
            ingestion_date, tt_order_hol_jan, tt_order_hol_feb, tt_order_hol_mar,
            tt_order_hol_apr, tt_order_hol_may, tt_order_hol_jun, tt_order_hol_jul,
            tt_order_hol_aug, tt_order_hol_sep, tt_order_hol_oct, tt_order_hol_nov,
            tt_order_hol_dec)
            SELECT
            now() AS ingestion_date,
            (SELECT
            SUM(t2.quantity) FROM (select order_date, quantity FROM peluader5437_staging.orders so JOIN if_common.dim_dates idd ON so.order_date::timestamp = idd.calendar_dt::timestamp
            WHERE idd.day_of_the_week_num<6 AND idd.working_day=FALSE) t2  
            WHERE EXTRACT(MONTH FROM t2.order_date) = 1) AS tt_order_hol_jan,
            (SELECT
            SUM(t2.quantity) FROM (select order_date, quantity FROM peluader5437_staging.orders so JOIN if_common.dim_dates idd ON so.order_date::timestamp = idd.calendar_dt::timestamp
            WHERE idd.day_of_the_week_num<6 AND idd.working_day=FALSE) t2 
            WHERE EXTRACT(MONTH FROM t2.order_date) = 2) AS tt_order_hol_feb,
            (SELECT
            SUM(t2.quantity) FROM (select order_date, quantity FROM peluader5437_staging.orders so JOIN if_common.dim_dates idd ON so.order_date::timestamp = idd.calendar_dt::timestamp
            WHERE idd.day_of_the_week_num<6 AND idd.working_day=FALSE) t2 
            WHERE EXTRACT(MONTH FROM t2.order_date) = 3) AS tt_order_hol_mar,
            (SELECT
            SUM(t2.quantity) FROM (select order_date, quantity FROM peluader5437_staging.orders so JOIN if_common.dim_dates idd ON so.order_date::timestamp = idd.calendar_dt::timestamp
            WHERE idd.day_of_the_week_num<6 AND idd.working_day=FALSE) t2 
            WHERE EXTRACT(MONTH FROM t2.order_date) = 4) AS tt_order_hol_apr,
            (SELECT
            SUM(t2.quantity) FROM (select order_date, quantity FROM peluader5437_staging.orders so JOIN if_common.dim_dates idd ON so.order_date::timestamp = idd.calendar_dt::timestamp
            WHERE idd.day_of_the_week_num<6 AND idd.working_day=FALSE) t2 
            WHERE EXTRACT(MONTH FROM t2.order_date) = 5) AS tt_order_hol_may,
            (SELECT
            SUM(t2.quantity) FROM (select order_date, quantity FROM peluader5437_staging.orders so JOIN if_common.dim_dates idd ON so.order_date::timestamp = idd.calendar_dt::timestamp
            WHERE idd.day_of_the_week_num<6 AND idd.working_day=FALSE) t2 
            WHERE EXTRACT(MONTH FROM t2.order_date) = 6) AS tt_order_hol_jun,
            (SELECT
            SUM(t2.quantity) FROM (select order_date, quantity FROM peluader5437_staging.orders so JOIN if_common.dim_dates idd ON so.order_date::timestamp = idd.calendar_dt::timestamp
            WHERE idd.day_of_the_week_num<6 AND idd.working_day=FALSE) t2 
            WHERE EXTRACT(MONTH FROM t2.order_date) = 7) AS tt_order_hol_jul,
            (SELECT
            SUM(t2.quantity) FROM (select order_date, quantity FROM peluader5437_staging.orders so JOIN if_common.dim_dates idd ON so.order_date::timestamp = idd.calendar_dt::timestamp
            WHERE idd.day_of_the_week_num<6 AND idd.working_day=FALSE) t2 
            WHERE EXTRACT(MONTH FROM t2.order_date) = 8) AS tt_order_hol_aug,
            (SELECT
            SUM(t2.quantity) FROM (select order_date, quantity FROM peluader5437_staging.orders so JOIN if_common.dim_dates idd ON so.order_date::timestamp = idd.calendar_dt::timestamp
            WHERE idd.day_of_the_week_num<6 AND idd.working_day=FALSE) t2 
            WHERE EXTRACT(MONTH FROM t2.order_date) = 9) AS tt_order_hol_sep,
            (SELECT
            SUM(t2.quantity) FROM (select order_date, quantity FROM peluader5437_staging.orders so JOIN if_common.dim_dates idd ON so.order_date::timestamp = idd.calendar_dt::timestamp
            WHERE idd.day_of_the_week_num<6 AND idd.working_day=FALSE) t2 
            WHERE EXTRACT(MONTH FROM t2.order_date) = 10) AS tt_order_hol_oct,
            (SELECT
            SUM(t2.quantity) FROM (select order_date, quantity FROM peluader5437_staging.orders so JOIN if_common.dim_dates idd ON so.order_date::timestamp = idd.calendar_dt::timestamp
            WHERE idd.day_of_the_week_num<6 AND idd.working_day=FALSE) t2 
            WHERE EXTRACT(MONTH FROM t2.order_date) = 11) AS tt_order_hol_nov,
            (SELECT
            SUM(t2.quantity) FROM (select order_date, quantity FROM peluader5437_staging.orders so JOIN if_common.dim_dates idd ON so.order_date::timestamp = idd.calendar_dt::timestamp
            WHERE idd.day_of_the_week_num<6 AND idd.working_day=FALSE) t2 
            WHERE EXTRACT(MONTH FROM t2.order_date) = 12) AS tt_order_hol_dec
            
            '''


agg_shipments_table_insert = '''INSERT INTO peluader5437_analytics.agg_shipments (
            ingestion_date, tt_late_shipments, tt_undelivered_items)
            SELECT
            now() AS ingestion_date,
            (SELECT
            COUNT(*) FROM (SELECT *  FROM peluader5437_staging.orders so 
            JOIN peluader5437_staging.shipments_deliveries sd ON so.order_id = sd.order_id) t1 WHERE DATE_PART('day', t1.shipment_date::timestamp - t1.order_date::timestamp) >= 6 AND t1.delivery_date IS NULL )  AS tt_late_shipments,
            
            (SELECT
            COUNT(*) FROM (SELECT *  FROM peluader5437_staging.orders so 
            JOIN peluader5437_staging.shipments_deliveries sd ON so.order_id = sd.order_id) t1 WHERE t1.shipment_date IS NULL AND t1.delivery_date IS NULL AND DATE_PART('day', '2022-09-05'::timestamp - t1.order_date::timestamp) >= 15 )  AS tt_undelivered_items
            
            '''



best_performing_product_table_insert = '''INSERT INTO peluader5437_analytics.best_performing_product (
            ingestion_date, product_name, most_ordered_day, is_public_holiday, tt_review_points,
            pct_one_star_review, pct_two_star_review, pct_three_star_review, pct_four_star_review,
            pct_five_star_review, pct_early_shipments, pct_late_shipments)
            SELECT
            now() AS ingestion_date,

            (SELECT product_name FROM peluader5437_staging.reviews sr JOIN if_common.dim_products dp 
            ON sr.product_id = dp.product_id GROUP BY product_name ORDER BY SUM(review) DESC fetch first 1 rows with ties) AS product_name,

            (SELECT t2.order_date FROM (SELECT * FROM if_common.dim_products dp JOIN peluader5437_staging.orders so ON dp.product_id = so.product_id::INT WHERE product_name = (SELECT product_name FROM peluader5437_staging.reviews sr 
            JOIN if_common.dim_products dp ON sr.product_id = dp.product_id GROUP BY product_name ORDER BY SUM(review) DESC fetch first 1 rows with ties)) t2 
            GROUP BY order_date ORDER By SUM(t2.quantity) DESC fetch first 1 rows with ties) AS most_ordered_day,

            (SELECT
                CASE
                    WHEN (day_of_the_week_num < 6) AND (working_day = FALSE) THEN CAST('TRUE' AS BOOLEAN)
                    ELSE CAST('FALSE' AS BOOLEAN)
                END AS is_public_holiday
                FROM (SELECT t2.order_date FROM (SELECT * FROM if_common.dim_products dp JOIN peluader5437_staging.orders so ON dp.product_id = so.product_id::INT 
                WHERE product_name = (SELECT product_name FROM peluader5437_staging.reviews sr JOIN if_common.dim_products dp ON sr.product_id = dp.product_id 
                GROUP BY product_name ORDER BY SUM(review) DESC fetch first 1 rows with ties)) t2 GROUP BY order_date ORDER By SUM(t2.quantity) DESC fetch first 1 rows with ties) t3
                JOIN if_common.dim_dates idd ON (t3.order_date::timestamp = idd.calendar_dt::timestamp)), 

            (SELECT MAX(tt_review) FROM (SELECT product_name, SUM(review) as tt_review FROM if_common.dim_products dp 
            JOIN peluader5437_staging.reviews sr ON dp.product_id = sr.product_id GROUP BY product_name) tt) AS tt_review_points,

            (SELECT  (SELECT COUNT(*) FROM peluader5437_staging.reviews sr JOIN if_common.dim_products idp
            ON sr.product_id=idp.product_id WHERE product_name = (SELECT product_name FROM peluader5437_staging.reviews sr JOIN if_common.dim_products dp 
            ON sr.product_id = dp.product_id GROUP BY product_name ORDER BY SUM(review) DESC fetch first 1 rows with ties) AND review = 1)/(SELECT COUNT(*) FROM peluader5437_staging.reviews sr 
            JOIN if_common.dim_products idp ON sr.product_id=idp.product_id WHERE product_name = (SELECT product_name FROM peluader5437_staging.reviews sr JOIN if_common.dim_products dp 
            ON sr.product_id = dp.product_id GROUP BY product_name ORDER BY SUM(review) DESC fetch first 1 rows with ties))::float * 100) AS pct_one_star_review,

            (SELECT  (SELECT COUNT(*) FROM peluader5437_staging.reviews sr JOIN if_common.dim_products idp
            ON sr.product_id=idp.product_id WHERE product_name = (SELECT product_name FROM peluader5437_staging.reviews sr JOIN if_common.dim_products dp 
            ON sr.product_id = dp.product_id GROUP BY product_name ORDER BY SUM(review) DESC fetch first 1 rows with ties) AND review = 2)/(SELECT COUNT(*) FROM peluader5437_staging.reviews sr 
            JOIN if_common.dim_products idp ON sr.product_id=idp.product_id WHERE product_name = (SELECT product_name FROM peluader5437_staging.reviews sr JOIN if_common.dim_products dp 
            ON sr.product_id = dp.product_id GROUP BY product_name ORDER BY SUM(review) DESC fetch first 1 rows with ties))::float * 100) AS pct_two_star_review,

            (SELECT  (SELECT COUNT(*) FROM peluader5437_staging.reviews sr JOIN if_common.dim_products idp
            ON sr.product_id=idp.product_id WHERE product_name = (SELECT product_name FROM peluader5437_staging.reviews sr JOIN if_common.dim_products dp 
            ON sr.product_id = dp.product_id GROUP BY product_name ORDER BY SUM(review) DESC fetch first 1 rows with ties) AND review = 3)/(SELECT COUNT(*) FROM peluader5437_staging.reviews sr 
            JOIN if_common.dim_products idp ON sr.product_id=idp.product_id WHERE product_name = (SELECT product_name FROM peluader5437_staging.reviews sr JOIN if_common.dim_products dp 
            ON sr.product_id = dp.product_id GROUP BY product_name ORDER BY SUM(review) DESC fetch first 1 rows with ties))::float * 100) AS pct_three_star_review,

            (SELECT  (SELECT COUNT(*) FROM peluader5437_staging.reviews sr JOIN if_common.dim_products idp
            ON sr.product_id=idp.product_id WHERE product_name = (SELECT product_name FROM peluader5437_staging.reviews sr JOIN if_common.dim_products dp 
            ON sr.product_id = dp.product_id GROUP BY product_name ORDER BY SUM(review) DESC fetch first 1 rows with ties) AND review = 4)/(SELECT COUNT(*) FROM peluader5437_staging.reviews sr 
            JOIN if_common.dim_products idp ON sr.product_id=idp.product_id WHERE product_name = (SELECT product_name FROM peluader5437_staging.reviews sr JOIN if_common.dim_products dp 
            ON sr.product_id = dp.product_id GROUP BY product_name ORDER BY SUM(review) DESC fetch first 1 rows with ties))::float * 100) AS pct_four_star_review,

            (SELECT  (SELECT COUNT(*) FROM peluader5437_staging.reviews sr JOIN if_common.dim_products idp
            ON sr.product_id=idp.product_id WHERE product_name = (SELECT product_name FROM peluader5437_staging.reviews sr JOIN if_common.dim_products dp 
            ON sr.product_id = dp.product_id GROUP BY product_name ORDER BY SUM(review) DESC fetch first 1 rows with ties) AND review = 5)/(SELECT COUNT(*) FROM peluader5437_staging.reviews sr 
            JOIN if_common.dim_products idp ON sr.product_id=idp.product_id WHERE product_name = (SELECT product_name FROM peluader5437_staging.reviews sr JOIN if_common.dim_products dp 
            ON sr.product_id = dp.product_id GROUP BY product_name ORDER BY SUM(review) DESC fetch first 1 rows with ties))::float * 100) AS pct_five_star_review,

            (SELECT (SELECT
            COUNT(*) FROM (SELECT *  FROM peluader5437_staging.orders so 
            JOIN peluader5437_staging.shipments_deliveries sd ON so.order_id = sd.order_id) t1 JOIN if_common.dim_products idp
            ON t1.product_id::INT = idp.product_id WHERE DATE_PART('day', t1.shipment_date::timestamp - t1.order_date::timestamp) < 6 
            AND t1.delivery_date IS NOT NULL AND product_name = (SELECT product_name FROM peluader5437_staging.reviews sr JOIN if_common.dim_products dp 
            ON sr.product_id = dp.product_id GROUP BY product_name ORDER BY SUM(review) DESC fetch first 1 rows with ties))/(SELECT
            COUNT(*) FROM (SELECT *  FROM peluader5437_staging.orders so 
            JOIN peluader5437_staging.shipments_deliveries sd ON so.order_id = sd.order_id) t1 JOIN if_common.dim_products idp
            ON t1.product_id::INT = idp.product_id WHERE product_name = (SELECT product_name FROM peluader5437_staging.reviews sr JOIN if_common.dim_products dp 
            ON sr.product_id = dp.product_id GROUP BY product_name ORDER BY SUM(review) DESC fetch first 1 rows with ties))::float * 100) AS pct_early_shipments,


            (SELECT (SELECT
            COUNT(*) FROM (SELECT *  FROM peluader5437_staging.orders so 
            JOIN peluader5437_staging.shipments_deliveries sd ON so.order_id = sd.order_id) t1 JOIN if_common.dim_products idp
            ON t1.product_id::INT = idp.product_id WHERE DATE_PART('day', t1.shipment_date::timestamp - t1.order_date::timestamp) >= 6 
            AND t1.delivery_date IS NULL AND product_name = (SELECT product_name FROM peluader5437_staging.reviews sr JOIN if_common.dim_products dp 
            ON sr.product_id = dp.product_id GROUP BY product_name ORDER BY SUM(review) DESC fetch first 1 rows with ties))/(SELECT
            COUNT(*) FROM (SELECT *  FROM peluader5437_staging.orders so 
            JOIN peluader5437_staging.shipments_deliveries sd ON so.order_id = sd.order_id) t1 JOIN if_common.dim_products idp
            ON t1.product_id::INT = idp.product_id WHERE product_name = (SELECT product_name FROM peluader5437_staging.reviews sr JOIN if_common.dim_products dp 
            ON sr.product_id = dp.product_id GROUP BY product_name ORDER BY SUM(review) DESC fetch first 1 rows with ties))::float * 100) AS pct_late_shipments
            
            '''

create_table_queries = [reviews_table_create, shipments_deliveries_table_create, 
                        orders_table_create, agg_public_holiday_table_create, agg_shipments_table_create, 
                        best_performing_product_table_create]

drop_table_queries = [reviews_table_drop, shipments_deliveries_table_drop, 
                    orders_table_drop, agg_public_holiday_table_drop, 
                    agg_shipments_table_drop, best_performing_product_table_drop]

insert_table_queries = [agg_public_holiday_table_insert, agg_shipments_table_insert, best_performing_product_table_insert]