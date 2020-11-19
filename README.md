# Data Engineering 1 - Term Project


## Business Analysis for Latin American E-commerce Player

### Project Specifics
* **Data:** Brazilian E-Commerce Public Dataset by Olist 
* **Data Source:** Kaggle (2018 Data is taken)
* **Description:** The dataset contains orders made at Olist Store. Olist connects small businesses from all over Brazil to channels without hassle and with a                        single contract. Those merchants are able to sell their products through the Olist Store and ship them directly to the customers using Olist                        logistics partners.

### Data Specifics & Scope
 Analyze current sales, profitability, inventory, customer satisfaction & growth compared to last time period

### Layers 
 Below is the order of layers that are implemented: 

 1. **Data Warehousing Layer:**  Data Import from various files
 2. **Operational Layer:**       Normalized operational layer is developed by performing various ETL operations over the Data warehousing layer
 3. **Data Marts:**              Departmental Views such as sales, inventory & customer satisfaction are created using the operational pipeline 
 4. **Analytics Layer:**         Summarized table is created from the operational layer using various types of aggregation & casting on different tables present in                                  the operational layer
 5. **Analytics:**               Data marts & Analytics layer go hand-in-hand to solve our analytics cases and there is faster implementation as tables are already                                  summarized.
 6. **Analytics use cases:**
    1. Evaluate critical monthly business KPIs e.g. monthly revenue, average order value,  number of customers, order volume
    2. What is the company's annual profitablity? 
    3. What is our top selling product categories?
    4. What are our top selling markets by revenue?
    5. What is current customer satisfaction score and change over time?

## ER Diagram
![ER Diagram][EER]

[EER]: EER.png "ER Diagram"


### Project Testing
Referential integrity was tested & successfully implemented in the operational layer itself

### Project Reproducibility
Given the path of csv files is in place, the entire code can be rerun to produce same output


### Things To Note:
 1. MySQL server should be running on the machine, and must allow local file import to database
 2. To avoid server shut down, please allow the query management tool we're using 2-3 minutes to run
 3. Please make sure to change the csv file paths to ensure smooth run

<br/><br/><br/>
