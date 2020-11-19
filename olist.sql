/*
Author: Mariam Bazzi
Date: 2020-11-19
Project:  Business analysis for latin-american e-commerce player
Project specifics: 
   Data: Brazilian E-Commerce Public Dataset by Olist
   Data Source: Kaggle (2018 Data is taken)
   Description: The dataset contains orders made at Olist Store. Olist connects small businesses from all over Brazil
				to channels without hassle and with a single contract. Those merchants are able to sell their products
				through the Olist Store and ship them directly to the customers using Olist logistics partners.
   Data Specifics & Scope: Analyze current sales, profitability, inventory, customer satisfaction & growth compared to last time period
   Layers: Below is the order of layers that are implemented 
     A: Data Warehousing Layer:  Data Import from various files
     B: Operational Layer:       Normalized operational layer is developed by doing various ETL operations over the Data warehousing layer
     C: Data Marts:              Departmental Views such as sales, inventory & customer satisfaction are created using operational pipeline 
     D: Analytics Layer:         Summarized table is created from the operational layer using various types of aggregation & casting on different tables present in operational layer
     E: Analytics:               Datamarts & Analytics layer go hand in hand to solve our analytics cases and there is faster implementation as tables are already summarized.
     F: Analytics use cases:
          1.	Evaluate critical monthly business KPIs e.g. monthly revenue, average order value,  number of customers, order volume
	      2.    What is company annual profitablity? 
          3.    What is our top selling product categories?
          4.    What are our top selling markets by revenue?
          5.    What is current customer satisfaction score and change over time?
     
 Query Management Tool: MySQL Workbench 8.0
 Project Testing: Referential integrity was tested & successfully implemented in the operational layer itself
 Project Reproducibility:  Given the path of csv files is in place, the entire code can be rerun to produce same output
 Things To Note: 
     1.  Mysql server should be running on the machine and must allow local file import to database
     2.  Please make sure, the query management tool we're using should allow 2-3 minutes to run to avoid server shut down
     3.  Please make sure to change the csv file paths are correct to ensure smooth run
*/

/* Create a database */
DROP DATABASE IF EXISTS olist;
CREATE DATABASE olist;

USE olist;

/* Create tables to import raw data */
-- Create table customer_dataset
DROP TABLE IF EXISTS olist.customer_dataset;
CREATE TABLE olist.customer_dataset(
  customer_id varchar(50),
  customer_unique_id varchar(50),
  customer_zip_code_prefix varchar(10),
  customer_city varchar(50),
  customer_state varchar(5)
);
LOAD DATA LOCAL INFILE "~/Desktop/Term-DE1/data/olist_customers_dataset.csv" 
INTO TABLE olist.customer_dataset 
COLUMNS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"' 
ESCAPED BY '"' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES;


-- Create table order_items_dataset
DROP TABLE IF EXISTS olist.order_items_dataset;
CREATE TABLE olist.order_items_dataset(
  order_id varchar(50),
  order_item_id int,
  product_id varchar(50),
  seller_id varchar(50),
  shipping_limit_date datetime,
  price float,
  freight_value float,
  sales int
);
LOAD DATA LOCAL INFILE "~/Desktop/Term-DE1/data/olist_order_items_dataset_new.csv" 
INTO TABLE olist.order_items_dataset 
COLUMNS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"' 
ESCAPED BY '"' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES;


-- Create table order_payments_dataset
DROP TABLE IF EXISTS olist.order_payments_dataset;
CREATE TABLE olist.order_payments_dataset(
  order_id varchar(50),
  payment_sequential int,
  payment_type varchar(30),
  payment_installments int,
  payment_value float
);
LOAD DATA LOCAL INFILE "~/Desktop/Term-DE1/data/olist_order_payments_dataset.csv" 
INTO TABLE olist.order_payments_dataset 
COLUMNS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"' 
ESCAPED BY '"' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES;


-- Create table orders_dataset
DROP TABLE IF EXISTS olist.orders_dataset;
CREATE TABLE olist.orders_dataset(
  order_id varchar(50),
  customer_id varchar(50),
  order_status varchar(30),
  order_purchase_timestamp datetime,
  order_approved_at datetime,
  order_delivered_carrier_date datetime,
  order_delivered_customer_date datetime,
  order_estimated_delivery_date datetime,
  fix_id int
);
LOAD DATA LOCAL INFILE "~/Desktop/Term-DE1/data/olist_orders_dataset_new.csv" 
INTO TABLE olist.orders_dataset 
COLUMNS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"' 
ESCAPED BY '"' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES;


-- Create table order_reviews_dataset
DROP TABLE IF EXISTS olist.order_reviews_dataset;
CREATE TABLE olist.order_reviews_dataset(
  review_id varchar(50),
  order_id varchar(50),
  review_answer_timestamp datetime,
  review_score int
    
);
LOAD DATA LOCAL INFILE "~/Desktop/Term-DE1/data/olist_order_reviews_dataset_new.csv" 
INTO TABLE olist.order_reviews_dataset 
FIELDS TERMINATED BY ','  
OPTIONALLY ENCLOSED BY '"'   
ESCAPED BY '"' 
LINES TERMINATED BY '\n' IGNORE 1 LINES;


-- Create table products_dataset
DROP TABLE IF EXISTS olist.products_dataset;
CREATE TABLE olist.products_dataset(
  product_id varchar(50),
  product_category_name text,
  product_name_lenght int NULL,
  product_description_lenght INT NULL,
  product_photos_qty int NULL,
  product_weight_g int NULL,
  product_length_cm int NULL,
  product_height_cm int NULL,
  product_width_cm int NULL,
  cost int
);
LOAD DATA LOCAL INFILE "~/Desktop/Term-DE1/data/olist_products_dataset.csv" 
INTO TABLE olist.products_dataset 
COLUMNS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"' 
ESCAPED BY '"' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES;


-- Create table sellers_dataset
DROP TABLE IF EXISTS olist.sellers_dataset;
CREATE TABLE olist.sellers_dataset(
  seller_id varchar(50),
  seller_zip_code_prefix varchar(20),
  seller_city varchar(50),
  seller_state varchar(10)
);
LOAD DATA LOCAL INFILE "~/Desktop/Term-DE1/data/olist_sellers_dataset.csv" 
INTO TABLE olist.sellers_dataset 
COLUMNS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"' 
ESCAPED BY '"' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES;


-- Create table product_category_name_translation
DROP TABLE IF EXISTS olist.product_category_name_translation;
CREATE TABLE olist.product_category_name_translation(
  product_category_name varchar(100),
  product_category_name_english varchar(100)
);
LOAD DATA LOCAL INFILE "~/Desktop/Term-DE1/data/product_category_name_translation.csv" 
INTO TABLE olist.product_category_name_translation 
COLUMNS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"' 
ESCAPED BY '"' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES;


-- Create table geolocation_dataset for geographical data
DROP TABLE IF EXISTS olist.geolocation_dataset;
CREATE TABLE olist.geolocation_dataset(
  zip_code varchar(10),
  city varchar(50),
  state varchar(5)
);
LOAD DATA LOCAL INFILE "~/Desktop/Term-DE1/data/olist_geolocation_dataset_new.csv" 
INTO TABLE olist.geolocation_dataset 
COLUMNS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"' 
ESCAPED BY '"' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES;


/*  Operational Layer  */
-- ETL Pipeline for data load from warehouse layer to operational layer
DROP DATABASE IF EXISTS prod_ops;
CREATE DATABASE prod_ops;

USE prod_ops;

-- Create normalized datasets for various tables

-- Create table geographical data
DROP TABLE IF EXISTS prod_ops.geolocation_data;
CREATE TABLE prod_ops.geolocation_data(
  zip_code varchar(10),
  city varchar(50),
  state varchar(5),
  primary key(zip_code)
);

-- Create table customers data
DROP TABLE IF EXISTS prod_ops.customers_data;
CREATE TABLE prod_ops.customers_data(
  customer_id varchar(50),
  customer_zip_code_prefix varchar(10),
  primary key(customer_id),
  foreign key(customer_zip_code_prefix) references prod_ops.geolocation_data(zip_code)
);

-- Create table products dataset
DROP TABLE IF EXISTS prod_ops.products_data;
CREATE TABLE prod_ops.products_data(
  product_id varchar(50),
  product_category_name varchar(100),
  cost int,
  primary key(product_id)
);

-- Create table sellers dataset
DROP TABLE IF EXISTS prod_ops.sellers_data;
CREATE TABLE prod_ops.sellers_data(
  seller_id varchar(50),
  seller_zip_code_prefix varchar(20),
  primary key(seller_id),
  foreign key(seller_zip_code_prefix) references prod_ops.geolocation_data(zip_code)
);

-- Create table orders dataset
DROP TABLE IF EXISTS prod_ops.orders_data;
CREATE TABLE prod_ops.orders_data(
  order_id varchar(50),
  customer_id varchar(50),
  order_status varchar(30),
  order_purchase_timestamp datetime,
  order_approved_at datetime,
  order_delivered_carrier_date datetime,
  order_delivered_customer_date datetime,
  order_estimated_delivery_date datetime,
  primary key(order_id),
  foreign key(customer_id) references prod_ops.customers_data(customer_id)
);

-- Create table order items data
DROP TABLE IF EXISTS prod_ops.order_items_data;
CREATE TABLE prod_ops.order_items_data(
  order_id varchar(50),
  product_id varchar(50),
  seller_id varchar(50),
  shipping_limit_date datetime,
  quantity int,
  price float,
  freight_value float,
  sales float,
  primary key(order_id, product_id),
  foreign key(seller_id) references prod_ops.sellers_data(seller_id)
);
-- Create table order payments
DROP TABLE IF EXISTS prod_ops.order_payments_data;
CREATE TABLE prod_ops.order_payments_data(
  payment_id int not null auto_increment,
  order_id varchar(50),
  payment_sequential int,
  payment_type varchar(30),
  payment_installments int,
  payment_value float,
  primary key(payment_id),
  foreign key(order_id) references prod_ops.orders_data(order_id)
);
-- Create table order reviews dataset
DROP TABLE IF EXISTS prod_ops.order_reviews_data;
CREATE TABLE prod_ops.order_reviews_data(
  review_id varchar(50),
  order_id varchar(50),
  review_score int,
  review_date date,
  primary key(review_id),
  foreign key(order_id) references prod_ops.orders_data(order_id)
);


/* ETL : EXTRACT information from imported tables, TRANSFORM i.e. Normalize, aggregate and LOAD into operational layer */

-- ETL : Geolocation Data 
INSERT INTO
  prod_ops.geolocation_data
SELECT
  zip_code,
  max(city) as city,
  max(state) as state
FROM
  olist.geolocation_dataset
GROUP BY
  zip_code;
  
  
-- ETL : Customers  Data 
INSERT INTO
  prod_ops.customers_data
SELECT DISTINCT 
	customer_id,
	customer_zip_code_prefix
FROM
  olist.customer_dataset;
  
  -- ETL : Products Data 
INSERT INTO
  prod_ops.products_data
SELECT
  a.product_id,
  b.product_category_name_english
  AS product_category_name,
  a.cost
FROM
  olist.products_dataset a
  LEFT JOIN olist.product_category_name_translation b 
  ON a.product_category_name = b.product_category_name;
  
  -- ETL : Sellers Data 
INSERT INTO
  prod_ops.sellers_data
SELECT DISTINCT
	seller_id,
	seller_zip_code_prefix
FROM
  olist.sellers_dataset;

-- ETL : Order Level Data 
INSERT INTO
  prod_ops.orders_data
SELECT
  order_id,
  a.customer_id,
  order_status,
  order_purchase_timestamp,
  order_approved_at,
  order_delivered_carrier_date,
  order_delivered_customer_date,
  order_estimated_delivery_date
FROM
  olist.orders_dataset a;
  
-- ETL : Order-Item level Data 
INSERT INTO
  prod_ops.order_items_data
SELECT
  order_id,
  product_id,
  a.seller_id AS seller_id,
  shipping_limit_date,
  count(order_id) AS quantity,
  sum(price) AS price,
  sum(freight_value) AS freight_value,
  sum(sales) AS sales
FROM
  olist.order_items_dataset a
GROUP BY
  order_id,
  product_id,
  seller_id,
  shipping_limit_date;
  
-- ETL : Customer Reviews Data 
INSERT INTO
  prod_ops.order_reviews_data
SELECT
  review_id,
  max(a.order_id) AS order_id,
  max(review_score) AS review_score,
  max(cast(review_answer_timestamp as date)) AS review_date
FROM
  olist.order_reviews_dataset a
  INNER JOIN (
    SELECT DISTINCT 
		order_id
    FROM
      prod_ops.orders_data
  ) b ON a.order_id = b.order_id
GROUP BY
  review_id;
  
-- ETL : Order-Payments Data 
INSERT INTO
  prod_ops.order_payments_data(
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
  )
SELECT
  a.order_id,
  payment_sequential,
  payment_type,
  payment_installments,
  payment_value
FROM
  olist.order_payments_dataset a
  INNER JOIN prod_ops.orders_data b 
  ON a.order_id = b.order_id;
  
/* End of Operational Layer */ 

/* Data Marts */

-- Create stored procedure to create datamarts
DROP PROCEDURE IF EXISTS prod_ops.usp_Create_Datamarts;
DELIMITER $$ 
CREATE PROCEDURE prod_ops.usp_Create_Datamarts() 
BEGIN 

-- create sales datamart
DROP VIEW IF EXISTS prod_ops.sales;
CREATE VIEW prod_ops.sales AS
SELECT
  a.order_id,
  a.customer_id,
  cast(a.order_purchase_timestamp as date) AS order_date,
  sum(c.sales) AS sales
FROM
  prod_ops.orders_data a
  JOIN (
    SELECT
      order_id,
      sum(payment_value) AS sales
    FROM
      prod_ops.order_payments_data
    GROUP BY
      order_id) c 
   ON a.order_id = c.order_id
WHERE
  order_status = 'delivered'
GROUP BY
  a.order_id,
  a.customer_id,
  cast(a.order_purchase_timestamp as date);
  
-- inventory data mart
DROP VIEW IF EXISTS prod_ops.inventory;
CREATE VIEW prod_ops.inventory AS
SELECT
  a.product_id,
  a.product_category_name AS product_category_name,
  a.cost,
  (
    CASE
      WHEN a.cost <= 50 THEN 'low-cost'
      WHEN a.cost <= 120 THEN 'mid-cost'
      ELSE 'high-cost'
    END
  ) AS cost_category
FROM
  prod_ops.products_data a;
  
-- customer reviews/satisfaction data mart
DROP VIEW IF EXISTS prod_ops.customer_satisfaction;
CREATE VIEW prod_ops.customer_satisfaction AS
SELECT
  review_id,
  max(a.order_id) AS order_id,
  max(review_score) AS review_score,
  max(cast(review_date as date)) AS review_date
FROM
  prod_ops.order_reviews_data a
  INNER JOIN (
    SELECT DISTINCT 
		order_id
    FROM
      prod_ops.orders_data) b 
   ON a.order_id = b.order_id
GROUP BY
  review_id;
END $$
DELIMITER ;

CALL prod_ops.usp_Create_Datamarts();

-- Analytics Layer

-- Create denormalized table for analytics
DROP DATABASE IF EXISTS prod_analytics;
CREATE DATABASE prod_analytics;

-- Create summarized table in analytics database
DROP TABLE IF EXISTS prod_analytics.order_summary;
CREATE TABLE prod_analytics.order_summary(id int not null  auto_increment,order_id varchar(50),
product_id varchar(50), product_category varchar(50), customer_id varchar(50), customer_state varchar(10),
order_status varchar(30),sales int, quantity int,order_purchase_date date,estimated_delivery_date date,
actual_delivery_date date, primary key(id));

-- Create table to track trigger records in analytics table
DROP TABLE IF EXISTS prod_analytics.audit_logs;
CREATE TABLE prod_analytics.audit_logs (
    log_id varchar(50) NOT NULL,
    old_row_data JSON,
    new_row_data JSON,
    dml_type ENUM('INSERT', 'UPDATE', 'DELETE') NOT NULL,
    dml_timestamp TIMESTAMP NOT NULL,
    dml_created_by VARCHAR(255) ,
    PRIMARY KEY (log_id, dml_type, dml_timestamp)
);

-- create trigger to track insertions into analytics DB
DROP TRIGGER IF EXISTS prod_analytics.record_insert_audit_trigger;
DELIMITER $$
CREATE TRIGGER prod_analytics.record_insert_audit_trigger
AFTER INSERT ON prod_analytics.order_summary FOR EACH ROW
BEGIN
    INSERT INTO prod_analytics.audit_logs (
        log_id,
        old_row_data,
        new_row_data,
        dml_type,
        dml_timestamp,
        dml_created_by
    )
    VALUES(
        NEW.id,
        null,
        JSON_OBJECT(
            "order_id", NEW.order_id
        ),
        'INSERT',
        CURRENT_TIMESTAMP,
        @logged_user
);
END  $$
DELIMITER ;


INSERT INTO prod_analytics.order_summary(order_id,
product_id, product_category, customer_id, customer_state,
order_status, sales, quantity,order_purchase_date,estimated_delivery_date ,
actual_delivery_date)
SELECT
  a.order_id,
  d.product_id,
  e.product_category_name AS product_category,
  max(a.customer_id) AS customer_id,
  max(f.state) AS customer_state,
  max(a.order_status) AS order_status,
  max(d.sales) AS sales,
  max(d.quantity) AS quantity,
  max(cast(order_purchase_timestamp as date)) AS order_purchase_date,
  max(cast(order_estimated_delivery_date as date)) AS estimated_delivery_date,
  max(cast(order_delivered_customer_date as date)) AS actual_delivery_date
FROM
  prod_ops.orders_data a
  JOIN prod_ops.customers_data c 
	ON a.customer_id = c.customer_id
  JOIN (
    SELECT
      order_id,
      product_id,
      seller_id,
      count(*) AS quantity,
      sum(sales) AS sales
    FROM
      prod_ops.order_items_data
    GROUP BY
      order_id,
      product_id,
      seller_id) d 
  ON a.order_id = d.order_id
  JOIN prod_ops.products_data e 
  ON d.product_id = e.product_id
  JOIN prod_ops.geolocation_data f
  ON c.customer_zip_code_prefix = f.zip_code
GROUP BY
  a.order_id,
  d.product_id,
  e.product_category_name;
  
 -- Check trigger records
 SELECT * FROM prod_analytics.audit_logs;
  
-- Analytics performed with the help of data marts and analytics layer

-- 1. Evaluate critical business monthly KPIs e.g. monthly revenue, average order value,  number of customers, order volume
  
DROP PROCEDURE IF EXISTS prod_analytics.usp_Monthly_Kpis;
CREATE PROCEDURE prod_analytics.usp_Monthly_Kpis() 
SELECT
  date_format(order_purchase_date, '%Y-%m') AS order_month,
  sum(sales) AS sales,
  count(DISTINCT order_id) AS orders,
  count(DISTINCT customer_id) AS customers,
  sum(sales) / count(DISTINCT order_id) AS average_oder_value
FROM
  prod_analytics.order_summary a
WHERE
  order_purchase_date < '2018-08-01'
GROUP BY
  date_format(order_purchase_date, '%Y-%m')
ORDER BY
  date_format(order_purchase_date, '%Y-%m') DESC;

-- 2.	What is the company's annual profitablity?
DROP PROCEDURE IF EXISTS prod_analytics.usp_Annual_Profitability;
CREATE PROCEDURE prod_analytics.usp_Annual_Profitability() 
SELECT
  year(order_purchase_date) as year,
  sum(sales) AS sales,
  sum(b.cost) AS costs,
  (1 - sum(cost) / sum(sales)) AS margin_percent
FROM
  prod_analytics.order_summary a
  JOIN prod_ops.inventory b 
	ON a.product_id = b.product_id
WHERE
  order_purchase_date < '2018-08-01'
GROUP BY
  year(order_purchase_date)
ORDER BY
  year(order_purchase_date) DESC;


-- 3. Current Best selling product categories
DROP PROCEDURE IF EXISTS prod_analytics.usp_Bestselling_Categories;
CREATE PROCEDURE prod_analytics.usp_Bestselling_Categories() 
SELECT
  product_category,
  sum(Sales) AS sales
FROM
  prod_analytics.order_summary
WHERE
  order_purchase_date < '2018-08-01'
GROUP BY
  product_category
ORDER BY
  sum(Sales) DESC;


-- 4. Identify top selling markets by revenue
DROP PROCEDURE IF EXISTS prod_analytics.usp_Bestselling_Markets;
CREATE PROCEDURE prod_analytics.usp_Bestselling_Markets() 
SELECT
  customer_state AS market,
  sum(sales) AS sales
FROM
  prod_analytics.order_summary
WHERE
  order_purchase_date < '2018-08-01'
GROUP BY
  customer_state
ORDER BY
  sum(Sales) DESC;

-- 5. What is customer satisfaction score and change with time?
DROP PROCEDURE IF EXISTS prod_analytics.usp_Customer_Satisfaction;
CREATE PROCEDURE prod_analytics.usp_Customer_Satisfaction() 
SELECT
  date_format(review_date, '%Y-%m') AS review_month,
  count(DISTINCT review_id) AS feedbacks,
  AVG(review_score) AS review_score
FROM
  prod_ops.customer_satisfaction
WHERE
  YEAR(review_date) = 2018
  AND review_date < '2018-08-01'
GROUP BY
  date_format(review_date, '%Y-%m')
ORDER BY
  date_format(review_date, '%Y-%m') DESC;

-- Call the below stored procedure to get final insights:
CALL prod_analytics.usp_Monthly_Kpis();
-- Average order value for current month is around 168 and has grown by 10% compared to beginning of the year

CALL prod_analytics.usp_Annual_Profitability();
-- Company is operating on around 54% healthy margin compared to e-commerce industry average of 48%

CALL prod_analytics.usp_Bestselling_Categories();
-- Health & Beauty is best selling category for the e-commerce company

CALL prod_analytics.usp_Bestselling_Markets();
-- SP (SAO PAULO) is best selling market having 2.9M sales i.e 3 times more than second best selling market

CALL prod_analytics.usp_Customer_Satisfaction();
-- Satisfaction score is 4.23 out of 5 and has increased compared to last month

