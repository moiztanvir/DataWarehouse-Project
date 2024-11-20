import java.sql.Connection;
import java.sql.Date;  // Importing java.sql.Date explicitly
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class MeshJoin2 {

    private static final String DB_URL = "jdbc:mysql://localhost:3306/metro_dw";
    private static final String USER = "root"; // Replace with your DB username
    private static final String PASS = "W7301@jqir#"; // Replace with your DB password

    public static void main(String[] args) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection connection = DriverManager.getConnection(DB_URL, USER, PASS);
            importDataFromCSV(connection);
            loadDataIntoDW(connection);
            olapQueries(connection);
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void importDataFromCSV(Connection connection) {
        importCustomers(connection);
        importProducts(connection);
        importTransactions(connection);
    }

    private static void importCustomers(Connection connection) {
        String csvFile = "customers_data.csv"; // Path to your CSV
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                String customerId = values[0];
                String customerName = values[1];
                String gender = values[2];

                String insertQuery = "INSERT INTO CUSTOMERS (CUSTOMER_ID, CUSTOMER_NAME, GENDER) VALUES (?, ?, ?) " +
                                     "ON DUPLICATE KEY UPDATE CUSTOMER_NAME = VALUES(CUSTOMER_NAME), GENDER = VALUES(GENDER)";
                try (PreparedStatement ps = connection.prepareStatement(insertQuery)) {
                    ps.setString(1, customerId);
                    ps.setString(2, customerName);
                    ps.setString(3, gender);
                    ps.executeUpdate();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void importProducts(Connection connection) {
        String csvFile = "products_data.csv"; // Path to your CSV
        List<String> validProducts = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            String line;
            boolean isFirstLine = true;
            while ((line = br.readLine()) != null) {
                if (isFirstLine) {
                    isFirstLine = false; // Skip the header
                    continue;
                }
                String[] values = line.split(",");
                if (values.length < 7) continue; // Ensure there's enough data
    
                String productId = values[0];
                String productName = values[1];
                double productPrice = 0.0;
                try {
                    productPrice = Double.parseDouble(values[2]);
                    if (productPrice < 0) throw new NumberFormatException(); // Check for negative prices
                } catch (NumberFormatException e) {
                    continue; // Skip this record
                }
                String supplierId = values[3];
                String supplierName = values[4];
                String storeId = values[5];
                String storeName = values[6];
    
                String insertQuery = "INSERT INTO PRODUCTS (PRODUCT_ID, PRODUCT_NAME, PRODUCT_PRICE, SUPPLIER_ID, SUPPLIER_NAME, STORE_ID, STORE_NAME) " +
                                     "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                                     "ON DUPLICATE KEY UPDATE PRODUCT_NAME = VALUES(PRODUCT_NAME), PRODUCT_PRICE = VALUES(PRODUCT_PRICE)";
                try (PreparedStatement ps = connection.prepareStatement(insertQuery)) {
                    ps.setString(1, productId);
                    ps.setString(2, productName);
                    ps.setDouble(3, productPrice);
                    ps.setString(4, supplierId);
                    ps.setString(5, supplierName);
                    ps.setString(6, storeId);
                    ps.setString(7, storeName);
                    ps.executeUpdate();
                }
            }
        } catch (Exception e) {
            // Handle exceptions silently if needed
        }
    }
    
    private static void importTransactions(Connection connection) {
        String csvFile = "transactions.csv"; // Path to your CSV
        List<String> dateFormats = Arrays.asList("yyyy-MM-dd", "MM/dd/yyyy", "dd-MM-yyyy", "yyyy/MM/dd");
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            String line;
            boolean isFirstLine = true;
            while ((line = br.readLine()) != null) {
                if (isFirstLine) {
                    isFirstLine = false; // Skip the header
                    continue;
                }
                String[] values = line.split(",");
                if (values.length < 5) continue; // Ensure there's enough data
    
                String orderId = values[0];
                Date orderDate = null;
                for (String format : dateFormats) {
                    try {
                        SimpleDateFormat sdf = new SimpleDateFormat(format);
                        sdf.setLenient(false); // Set lenient to false to strictly parse dates
                        orderDate = new Date(sdf.parse(values[1]).getTime());
                        break; // Exit the loop if parsing is successful
                    } catch (ParseException e) {
                        // Continue to the next format
                    }
                }
    
                if (orderDate == null) {
                    continue; // Skip this record if all formats fail
                }
    
                String productId = values[2];
                String customerId = values[3];
                int quantity;
                try {
                    quantity = Integer.parseInt(values[4]);
                    if (quantity < 0) throw new NumberFormatException(); // Check for negative quantity
                } catch (NumberFormatException e) {
                    continue; // Skip this record
                }
    
                String insertQuery = "INSERT INTO TRANSACTIONS (ORDER_ID, ORDER_DATE, PRODUCT_ID, CUSTOMER_ID, QUANTITY) " +
                                     "VALUES (?, ?, ?, ?, ?) " +
                                     "ON DUPLICATE KEY UPDATE ORDER_DATE = VALUES(ORDER_DATE), QUANTITY = VALUES(QUANTITY)";
                try (PreparedStatement ps = connection.prepareStatement(insertQuery)) {
                    ps.setString(1, orderId);
                    ps.setDate(2, orderDate);
                    ps.setString(3, productId);
                    ps.setString(4, customerId);
                    ps.setInt(5, quantity);
                    ps.executeUpdate();
                }
            }
        } catch (Exception e) {
            // Handle exceptions silently if needed
        }
    }
    

    private static void loadDataIntoDW(Connection connection) {
        try {
            System.out.println("Loading data into Data Warehouse...");
            List<Transaction> transactions = readTransactions(connection);
            Map<String, Customer> customerMap = loadCustomers(connection);
            Map<String, Product> productMap = loadProducts(connection);

            for (Transaction transaction : transactions) {
                if (customerMap.containsKey(transaction.getCustomerId()) &&
                    productMap.containsKey(transaction.getProductId())) {
                    double sale = transaction.getQuantity() * productMap.get(transaction.getProductId()).getProductPrice();
                    insertIntoDW(connection, transaction, customerMap.get(transaction.getCustomerId()), productMap.get(transaction.getProductId()), sale);
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
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(query)) {
            while (resultSet.next()) {
                String orderId = resultSet.getString("ORDER_ID");
                Date orderDate = resultSet.getDate("ORDER_DATE");
                String productId = resultSet.getString("PRODUCT_ID");
                String customerId = resultSet.getString("CUSTOMER_ID");
                int quantity = resultSet.getInt("QUANTITY");
                transactions.add(new Transaction(orderId, orderDate, productId, customerId, quantity));
            }
        }
        return transactions;
    }

    private static Map<String, Customer> loadCustomers(Connection connection) throws SQLException {
        Map<String, Customer> customerMap = new HashMap<>();
        String query = "SELECT CUSTOMER_ID, CUSTOMER_NAME, GENDER FROM CUSTOMERS";
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(query)) {
            while (resultSet.next()) {
                Customer customer = new Customer(
                        resultSet.getString("CUSTOMER_ID"),
                        resultSet.getString("CUSTOMER_NAME"),
                        resultSet.getString("GENDER")
                );
                customerMap.put(customer.getCustomerId(), customer);
            }
        }
        return customerMap;
    }

    private static Map<String, Product> loadProducts(Connection connection) throws SQLException {
        Map<String, Product> productMap = new HashMap<>();
        String query = "SELECT PRODUCT_ID, PRODUCT_NAME, PRODUCT_PRICE, SUPPLIER_ID, SUPPLIER_NAME, STORE_ID, STORE_NAME FROM PRODUCTS";
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(query)) {
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
        }
        return productMap;
    }

    private static void insertIntoDW(Connection connection, Transaction transaction, Customer customer, Product product, double sale) throws SQLException {
        String insertQuery = "INSERT INTO FACT_TRANSACTIONS (ORDER_ID, ORDER_DATE, PRODUCT_ID, CUSTOMER_ID, CUSTOMER_NAME, GENDER, PRODUCT_NAME, PRODUCT_PRICE, SUPPLIER_ID, SUPPLIER_NAME, STORE_ID, STORE_NAME, QUANTITY, SALE) " +
                             "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                             "ON DUPLICATE KEY UPDATE QUANTITY = VALUES(QUANTITY), SALE = VALUES(SALE)";
        try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
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
        }
    }

    private static void olapQueries(Connection connection) {
        try {
            topRevenueGeneratingProducts(connection, 2019); // Example year
            revenueGrowthRateQuarterly(connection, 2017);
            detailedSupplierSalesContribution(connection);
            seasonalAnalysisProductSales(connection);
            storeSupplierMonthlyRevenueVolatility(connection);
            topProductsPurchasedTogether(connection);
            yearlyRevenueTrends(connection, 2019);
            revenueVolumeAnalysisH1H2(connection);
            countHighRevenueSpikes(connection);
            createStoreQuarterlySalesView(connection);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void topRevenueGeneratingProducts(Connection connection, int year) throws SQLException {
        String query = "SELECT SUM(SALE) AS TOTAL_SALES " +
                       "FROM (SELECT PRODUCT_ID, SUM(SALE) AS SALE " +
                       "      FROM FACT_TRANSACTIONS " +
                       "      WHERE YEAR(ORDER_DATE) = ? " +
                       "      GROUP BY PRODUCT_ID " +
                       "      ORDER BY SALE DESC LIMIT 5) AS TOP_PRODUCTS";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, year);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                System.out.println("Total Sales of Top 5 Products: " + rs.getDouble("TOTAL_SALES"));
            }
        }
    }

    private static void revenueGrowthRateQuarterly(Connection connection, int year) throws SQLException {
        String query = "SELECT AVG(GROWTH_RATE) AS AVERAGE_GROWTH_RATE " +
                       "FROM (SELECT STORE_ID, " +
                       "             SUM(SALE) AS TOTAL_SALES, " +
                       "             LAG(SUM(SALE)) OVER (PARTITION BY STORE_ID ORDER BY QUARTER(ORDER_DATE)) AS PREV_QUARTER_SALES, " +
                       "             CASE WHEN LAG(SUM(SALE)) OVER (PARTITION BY STORE_ID ORDER BY QUARTER(ORDER_DATE)) IS NOT NULL " +
                       "                  THEN ((SUM(SALE) - LAG(SUM(SALE)) OVER (PARTITION BY STORE_ID ORDER BY QUARTER(ORDER_DATE))) / " +
                       "                        LAG(SUM(SALE)) OVER (PARTITION BY STORE_ID ORDER BY QUARTER(ORDER_DATE))) * 100 " +
                       "                  ELSE NULL END AS GROWTH_RATE " +
                       "      FROM FACT_TRANSACTIONS " +
                       "      WHERE YEAR(ORDER_DATE) = ? " +
                       "      GROUP BY STORE_ID, QUARTER(ORDER_DATE)) AS GROWTH_DATA";
        
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, year);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                System.out.println("Average Growth Rate: " + rs.getDouble("AVERAGE_GROWTH_RATE") + "%");
            }
        }
    }

    private static void detailedSupplierSalesContribution(Connection connection) throws SQLException {
        String query = "SELECT SUM(SALE) AS TOTAL_SALES " +
                       "FROM FACT_TRANSACTIONS";
        try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
                System.out.println("Total Sales Contribution from All Suppliers: " + rs.getDouble("TOTAL_SALES"));
            }
        }
    }

    private static void seasonalAnalysisProductSales(Connection connection) throws SQLException {
        String query = "SELECT SUM(SALE) AS TOTAL_SALES " +
                       "FROM FACT_TRANSACTIONS";
        try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
                System.out.println("Total Sales Across All Seasons: " + rs.getDouble("TOTAL_SALES"));
            }
        }
    }

    private static void storeSupplierMonthlyRevenueVolatility(Connection connection) throws SQLException {
        String query = "SELECT AVG(VOLATILITY) AS AVERAGE_VOLATILITY " +
                       "FROM (SELECT STORE_NAME, SUPPLIER_NAME, MONTH(ORDER_DATE) AS MONTH, " +
                       "             SUM(SALE) AS TOTAL_SALES, " +
                       "             LAG(SUM(SALE)) OVER (PARTITION BY STORE_NAME, SUPPLIER_NAME ORDER BY MONTH(ORDER_DATE)) AS PREV_MONTH_SALES, " +
                       "             ((SUM(SALE) - LAG(SUM(SALE)) OVER (PARTITION BY STORE_NAME, SUPPLIER_NAME ORDER BY MONTH(ORDER_DATE))) / " +
                       "              LAG(SUM(SALE)) OVER (PARTITION BY STORE_NAME, SUPPLIER_NAME ORDER BY MONTH(ORDER_DATE))) * 100 AS VOLATILITY " +
                       "      FROM FACT_TRANSACTIONS " +
                       "      GROUP BY STORE_NAME, SUPPLIER_NAME, MONTH) AS VOLATILITY_DATA";
        
        try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
                System.out.println("Average Revenue Volatility: " + rs.getDouble("AVERAGE_VOLATILITY") + "%");
            }
        }
    }

    private static void topProductsPurchasedTogether(Connection connection) throws SQLException {
        String query = "SELECT COUNT(*) AS PURCHASE_COUNT " +
                       "FROM (SELECT p1.PRODUCT_NAME AS PRODUCT1, p2.PRODUCT_NAME AS PRODUCT2 " +
                       "      FROM FACT_TRANSACTIONS ft " +
                       "      JOIN FACT_TRANSACTIONS ft2 ON ft.ORDER_ID = ft2.ORDER_ID AND ft.PRODUCT_ID != ft2.PRODUCT_ID " +
                       "      JOIN PRODUCTS p1 ON ft.PRODUCT_ID = p1.PRODUCT_ID " +
                       "      JOIN PRODUCTS p2 ON ft2.PRODUCT_ID = p2.PRODUCT_ID " +
                       "      GROUP BY p1.PRODUCT_NAME, p2.PRODUCT_NAME " +
                       "      ORDER BY COUNT(*) DESC LIMIT 1) AS TOP_PAIR";
        
        try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
                System.out.println("Most Frequently Purchased Together Count: " + rs.getInt("PURCHASE_COUNT"));
            }
        }
    }

    private static void yearlyRevenueTrends(Connection connection, int year) throws SQLException {
        String query = "SELECT SUM(SALE) AS TOTAL_REVENUE " +
                    "FROM FACT_TRANSACTIONS " +
                    "WHERE YEAR(ORDER_DATE) = ?"; // You can specify the year as needed
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, year); // Pass the year parameter as needed
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                System.out.println("Total Revenue for the Year: " + rs.getDouble("TOTAL_REVENUE"));
            }
        }
    }

    private static void revenueVolumeAnalysisH1H2(Connection connection) throws SQLException {
        String query = "SELECT SUM(CASE WHEN MONTH(ORDER_DATE) <= 6 THEN SALE ELSE 0 END) AS H1_REVENUE " +
                       "FROM FACT_TRANSACTIONS";
        try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
                System.out.println("Total Revenue for H1: " + rs.getDouble("H1_REVENUE"));
            }
        }
    }

    private static void countHighRevenueSpikes(Connection connection) throws SQLException {
        String query = 
            "WITH DailySales AS ( " +
            "    SELECT PRODUCT_ID, ORDER_DATE, SUM(SALE) AS TOTAL_SALES " +
            "    FROM FACT_TRANSACTIONS " +
            "    GROUP BY PRODUCT_ID, ORDER_DATE " +
            "), DailyAverages AS ( " +
            "    SELECT PRODUCT_ID, AVG(TOTAL_SALES) AS DAILY_AVG " +
            "    FROM DailySales " +
            "    GROUP BY PRODUCT_ID " +
            ") " +
            "SELECT COUNT(*) AS SPIKE_COUNT " +
            "FROM DailySales ds " +
            "JOIN DailyAverages da ON ds.PRODUCT_ID = da.PRODUCT_ID " +
            "WHERE ds.TOTAL_SALES > 2 * da.DAILY_AVG";
    
        try (PreparedStatement ps = connection.prepareStatement(query);
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                int spikeCount = rs.getInt("SPIKE_COUNT"); // Get the count of spikes
                System.out.println("Number of High Revenue Spikes: " + spikeCount); // Print the count
            } else {
                System.out.println("Number of High Revenue Spikes: 0"); // Print 0 if no spikes are found
            }
        }
    }

    // Method to create the STORE_QUARTERLY_SALES view (returns success message)
    private static String createStoreQuarterlySalesView(Connection connection) throws SQLException {
        String query = 
            "CREATE VIEW STORE_QUARTERLY_SALES AS " +
            "SELECT STORE_ID, " +
            "       YEAR(ORDER_DATE) AS YEAR, " +
            "       QUARTER(ORDER_DATE) AS QUARTER, " +
            "       SUM(SALE) AS TOTAL_SALES " +
            "FROM FACT_TRANSACTIONS " +
            "GROUP BY STORE_ID, YEAR, QUARTER " +
            "ORDER BY STORE_ID, YEAR, QUARTER";

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(query);
            return "View STORE_QUARTERLY_SALES created successfully."; // Return success message
        }
    }
    // Other OLAP query methods (Q2 to Q10) would follow a similar pattern...

    static class Transaction {
        private String orderId;
        private Date orderDate;
        private String productId;
        private String customerId;
        private int quantity;

        public Transaction(String orderId, Date orderDate, String productId, String customerId, int quantity) {
            this.orderId = orderId;
            this.orderDate = orderDate;
            this.productId = productId;
            this.customerId = customerId;
            this.quantity = quantity;
        }

        public String getOrderId() { return orderId; }
        public Date getOrderDate() { return orderDate; }
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