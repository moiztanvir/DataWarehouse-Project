# DataWarehouse-Project

## Project Overview
This project implements a near-real-time Data Warehouse (DW) for the METRO Shopping Store. It uses a star schema, the MESHJOIN algorithm, and SQL-based OLAP queries to analyze customer shopping behavior and generate actionable insights.

## Project Structure
The project consists of three main files:
 
- metro.sql: Creates the star schema in the database, including fact and dimension tables.

- MeshJoin.java: Implements the ETL pipeline using the MESHJOIN algorithm to load and enrich data.

- olap_queries.sql: Contains OLAP queries to analyze the loaded data.

## Prerequisites
### Software Requirements:

- Java Development Kit (JDK) 8 or higher.

- Visual Studio Code, Eclipse IDE (or any Java IDE).

- MySQL Server and Workbench.

- MySQL JDBC Connector (e.g mysql-connector-xxx.jar)

### CSV Files Required:

- customers_data.csv (Customer information).

- products_data.csv (Product and supplier information).

- transactions.csv (Transaction records).

### Environment Configuration:

A MySQL database named metro_dw should be created. Ensure all required CSV files are placed in the project's root directory.

## Step-by-Step Guide

### Step 1: Setting up the Database

- Open MySQL Workbench or your preferred SQL client.

- Execute the metro.sql script to create the star schema.

- This will create the FACT_TRANSACTIONS table and the dimension tables (CUSTOMERS, PRODUCTS, TRANSACTIONS).

- Ensure no tables with the same name exist, as the script will drop pre-existing tables.

### Step 2: Importing the Project

- Open Visual Studio Code or Eclipse IDE.

- Create a new Java project and name it MeshJoinProject.

- Add the provided MeshJoin.java file to the main folder of your project.

- Add the MySQL JDBC connector to your project

- Select the downloaded JDBC connector JAR file.

### Step 3: Running the MESHJOIN Algorithm

- Open the MeshJoin.java file in VS Code or Eclipse.

- Modify the database connection parameters

- Replace DB_URL with your MySQL server address (e.g., jdbc:mysql://localhost:3306/metro_dw).

- Leave the USER and PASS variables blank; these will be entered at runtime.

- Run the program using the following commands:

javac -cp .;mysql-connector-xxx.jar MeshJoin.java

java -cp .;mysql-connector-xxx.jar MeshJoin

- Enter the MySQL username and password when prompted.

- The program will load the CUSTOMERS, PRODUCTS, and TRANSACTIONS data from the respective CSV files into their tables.

- Enrich transactional data using the MESHJOIN algorithm.

- Populate the FACT_TRANSACTIONS table in the DW.

### Step 4: Running OLAP Queries

Open the olap_queries.sql file in your SQL client. Execute each query to generate insights, such as:

- Top revenue-generating products.

- Quarterly revenue growth by store.

- Seasonal sales trends and more, etc. 

Save the query results if needed for reporting purposes.

### Input Files

- customers_data.csv:

Columns: CUSTOMER_ID, CUSTOMER_NAME, GENDER.

- products_data.csv:

Columns: PRODUCT_ID, PRODUCT_NAME, PRODUCT_PRICE, SUPPLIER_ID, SUPPLIER_NAME, STORE_ID, 
STORE_NAME.

- transactions.csv:

Columns: ORDER_ID, ORDER_DATE, PRODUCT_ID, CUSTOMER_ID, QUANTITY.

Ensure these files are formatted correctly and placed in the root directory.

## Expected Outputs

### Data Warehouse Content:

A fully populated FACT_TRANSACTIONS table with enriched transactional data.
OLAP Query Results:

- Revenue trends, growth rates, product affinities, and supplier contributions, etc.

### Error Handling

- Missing or malformed data in CSV files:

- Skipped with a warning in the console.

- Incorrect database credentials:

- The program will terminate with an error message.

- Pre-existing records:

- The program uses ON DUPLICATE KEY UPDATE to update existing records.

### Additional Notes

Modify partitionSize and chunkSize variables in the MeshJoin.java file to optimize performance based on system capabilities. Review console output for logs and warnings during the ETL process.
