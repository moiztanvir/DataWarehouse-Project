-- Step 1: Create the DataWarehouse database
CREATE DATABASE IF NOT EXISTS DataWarehouse;
USE DataWarehouse;

-- Step 2: Create Dimension Tables

-- Customer Dimension
CREATE TABLE Customer_Dimension (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100),
    gender VARCHAR(10)
);

-- Product Dimension
CREATE TABLE Product_Dimension (
    ProductID INT PRIMARY KEY,
    productName VARCHAR(150),
    productPrice DECIMAL(10, 2),
    supplierID INT,
    storeID INT
);

-- Supplier Dimension
CREATE TABLE Supplier_Dimension (
    supplierID INT PRIMARY KEY,
    supplierName VARCHAR(100)
);

-- Store Dimension
CREATE TABLE Store_Dimension (
    storeID INT PRIMARY KEY,
    storeName VARCHAR(100)
);

-- Step 3: Create the Fact Table

CREATE TABLE Sales_Fact (
    OrderID INT PRIMARY KEY,
    OrderDate DATETIME,
    ProductID INT,
    QuantityOrdered INT,
    customer_id INT,
    FOREIGN KEY (ProductID) REFERENCES Product_Dimension(ProductID),
    FOREIGN KEY (customer_id) REFERENCES Customer_Dimension(customer_id)
);

-- Step 4: Add Foreign Key Constraints in Product Dimension
ALTER TABLE Product_Dimension
ADD FOREIGN KEY (supplierID) REFERENCES Supplier_Dimension(supplierID),
ADD FOREIGN KEY (storeID) REFERENCES Store_Dimension(storeID);

-- The schema is now created. Next, populate it with data.
