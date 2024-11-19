import java.sql.Connection;
import java.sql.Date;  // Importing java.sql.Date explicitly
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class MeshJoin {
    private static final String DB_URL = "jdbc:mysql://localhost:3306/metro_dw";
    private static final String USER = "root"; // replace with your DB username
    private static final String PASS = "W7301@jqir#"; // replace with your DB password

    private static final int BUFFER_SIZE = 100; // Define an appropriate buffer size

    public static void main(String[] args) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection connection = DriverManager.getConnection(DB_URL, USER, PASS);
            loadDataIntoDW(connection);
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void loadDataIntoDW(Connection connection) {
        try {
            System.out.println("Loading data into Data Warehouse...");
            List<Transaction> transactions = readTransactions(connection);
            System.out.println("Transactions read: " + transactions.size());
    
            Map<String, Customer> customerMap = loadCustomers(connection);
            System.out.println("Customers loaded: " + customerMap.size());
    
            Map<String, Product> productMap = loadProducts(connection);
            System.out.println("Products loaded: " + productMap.size());
    
            for (Transaction transaction : transactions) {
                if (customerMap.containsKey(transaction.getCustomerId()) &&
                    productMap.containsKey(transaction.getProductId())) {
                    // Enrich transaction
                    double sale = transaction.getQuantity() * productMap.get(transaction.getProductId()).getProductPrice();
                    insertIntoDW(connection, transaction, customerMap.get(transaction.getCustomerId()), productMap.get(transaction.getProductId()), sale);
                    System.out.println("Inserted transaction: " + transaction.getOrderId());
                }
            }
            System.out.println("Data loading complete.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static List<Transaction> readTransactions(Connection connection) throws SQLException {
        List<Transaction> transactions = new ArrayList<>();
        String query = "SELECT ORDER_ID, ORDER_DATE, PRODUCT_ID, CUSTOMER_ID, QUANTITY FROM TRANSACTIONS";
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);
    
        while (resultSet.next()) {
            String orderId = resultSet.getString("ORDER_ID");
            java.sql.Date orderDate = resultSet.getDate("ORDER_DATE");
    
            // Check if orderDate is null (which means it was invalid)
            if (orderDate == null) {
                System.out.println("Invalid date for ORDER_ID: " + orderId);
                continue; // Skip this transaction
            }
    
            String productId = resultSet.getString("PRODUCT_ID");
            String customerId = resultSet.getString("CUSTOMER_ID");
            int quantity = resultSet.getInt("QUANTITY");
    
            Transaction transaction = new Transaction(orderId, orderDate, productId, customerId, quantity);
            transactions.add(transaction);
        }
        resultSet.close();
        statement.close();
        return transactions;
    }

    private static Map<String, Customer> loadCustomers(Connection connection) throws SQLException {
        Map<String, Customer> customerMap = new HashMap<>();
        String query = "SELECT CUSTOMER_ID, CUSTOMER_NAME, GENDER FROM CUSTOMERS";
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);

        while (resultSet.next()) {
            Customer customer = new Customer(
                resultSet.getString("CUSTOMER_ID"),
                resultSet.getString("CUSTOMER_NAME"),
                resultSet.getString("GENDER")
            );
            customerMap.put(customer.getCustomerId(), customer);
        }
        resultSet.close();
        statement.close();
        return customerMap;
    }

    private static Map<String, Product> loadProducts(Connection connection) throws SQLException {
        Map<String, Product> productMap = new HashMap<>();
        String query = "SELECT PRODUCT_ID, PRODUCT_NAME, PRODUCT_PRICE, SUPPLIER_ID, SUPPLIER_NAME, STORE_ID, STORE_NAME FROM PRODUCTS";
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);

        while (resultSet.next()) {
            Product product = new Product(
                resultSet.getString("PRODUCT_ID"),
                resultSet.getString("PRODUCT_NAME"),
                resultSet.getDouble("PRODUCT_PRICE"),
                resultSet.getString("SUPPLIER_ID"),
                resultSet.getString("SUPPLIER_NAME"),
                resultSet.getString("STORE_ID"),
                resultSet.getString("STORE_NAME")
            );
            productMap.put(product.getProductId(), product);
        }
        resultSet.close();
        statement.close();
        return productMap;
    }

    private static void insertIntoDW(Connection connection, Transaction transaction, Customer customer, Product product, double sale) throws SQLException {
        String insertQuery = "INSERT INTO FACT_TRANSACTIONS (ORDER_ID, ORDER_DATE, PRODUCT_ID, CUSTOMER_ID, CUSTOMER_NAME, GENDER, PRODUCT_NAME, PRODUCT_PRICE, SUPPLIER_ID, SUPPLIER_NAME, STORE_ID, STORE_NAME, QUANTITY, SALE) " +
                             "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                             "ON DUPLICATE KEY UPDATE QUANTITY = VALUES(QUANTITY), SALE = VALUES(SALE)";

        PreparedStatement preparedStatement = connection.prepareStatement(insertQuery);
        preparedStatement.setString(1, transaction.getOrderId());
        preparedStatement.setDate(2, transaction.getOrderDate());
        preparedStatement.setString(3, transaction.getProductId());
        preparedStatement.setString(4, transaction.getCustomerId());
        preparedStatement.setString(5, customer.getCustomerName());
        preparedStatement.setString(6, customer.getGender());
        preparedStatement.setString(7, product.getProductName());
        preparedStatement.setDouble(8, product.getProductPrice());
        preparedStatement.setString(9, product.getSupplierId());
        preparedStatement.setString(10, product.getSupplierName());
        preparedStatement.setString(11, product.getStoreId());
        preparedStatement.setString(12, product.getStoreName());
        preparedStatement.setInt(13, transaction.getQuantity());
        preparedStatement.setDouble(14, sale);

        preparedStatement.executeUpdate();
        preparedStatement.close();
    }

    // Transaction, Customer, and Product classes
    static class Transaction {
        private String orderId;
        private java.sql.Date orderDate;  // Specify java.sql.Date explicitly
        private String productId;
        private String customerId;
        private int quantity;

        public Transaction(String orderId, java.sql.Date orderDate, String productId, String customerId, int quantity) {
            this.orderId = orderId;
            this.orderDate = orderDate;
            this.productId = productId;
            this.customerId = customerId;
            this.quantity = quantity;
        }

        public String getOrderId() { return orderId; }
        public java.sql.Date getOrderDate() { return orderDate; }  // Specify java.sql.Date explicitly
        public String getProductId() { return productId; }
        public String getCustomerId() { return customerId; }
        public int getQuantity() { return quantity; }
    }

    static class Customer {
        private String customerId;
        private String customerName;
        private String gender;

        public Customer(String customerId, String customerName, String gender) {
            this.customerId = customerId;
            this.customerName = customerName;
            this.gender = gender;
        }

        public String getCustomerId() { return customerId; }
        public String getCustomerName() { return customerName; }
        public String getGender() { return gender; }
    }

    static class Product {
        private String productId;
        private String productName;
        private double productPrice;
        private String supplierId;
        private String supplierName;
        private String storeId;
        private String storeName;

        public Product(String productId, String productName, double productPrice, String supplierId, String supplierName, String storeId, String storeName) {
            this.productId = productId;
            this.productName = productName;
            this.productPrice = productPrice;
            this.supplierId = supplierId;
            this.supplierName = supplierName;
            this.storeId = storeId;
            this.storeName = storeName;
        }

        public String getProductId() { return productId; }
        public String getProductName() { return productName; }
        public double getProductPrice() { return productPrice; }
        public String getSupplierId() { return supplierId; }
        public String getSupplierName() { return supplierName; }
        public String getStoreId() { return storeId; }
        public String getStoreName() { return storeName; }
    }
}