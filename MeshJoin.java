import java.sql.Connection;
import java.sql.Date;  // Importing java.sql.Date explicitly
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.BufferedReader;
import java.io.Console;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.Scanner;
import javax.swing.text.View;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class MeshJoin {

    private static String DB_URL = "jdbc:mysql://localhost:3306/metro_dw";
    private static String USER; // Replace with your DB username
    private static String PASS; // Replace with your DB password


    public static void main(String[] args) {
        try {
            System.out.println("\n");
            Scanner scanner = new Scanner(System.in);

            System.out.print("Enter database username: ");
            USER = scanner.nextLine(); // Assign to USER variable
    
            System.out.print("Enter database password: ");
            PASS = scanner.nextLine(); // Assign to PASS variable
    
            scanner.close();

            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection connection = DriverManager.getConnection(DB_URL, USER, PASS);
            importDataFromCSV(connection);
                    // Set partitionSize and chunkSize for MESHJOIN processing
            int partitionSize = 10000; // Example: Number of customers/products per partition
            int chunkSize = 5000;      // Example: Number of transactions per chunk
            
            // Load data into the Data Warehouse using MESHJOIN
            loadDataIntoDW(connection, partitionSize, chunkSize);
            // loadDataIntoDW(connection);
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
            System.out.println("\n----------------------------------------------------------------------------- \n");
            System.out.println("Importing Customers data from CSV File...");
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
            System.out.println("Problem in Loading Data from Customers CSV");
        }
    }

    private static void importProducts(Connection connection) {
        String csvFile = "products_data.csv"; // Path to your CSV
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            System.out.println("Importing Products data from CSV File...");
            String line;
            boolean isFirstLine = true;
            while ((line = br.readLine()) != null) {
                if (isFirstLine) {
                    isFirstLine = false; // Skip the header
                    continue;
                }
                String[] values = line.split(",");
                if (values.length < 7) {
                    System.err.println("Skipping invalid product line (insufficient data): " + line);
                    continue; // Ensure there's enough data
                }
    
                String productId = values[0].trim(); // Trim whitespace
                String productName = values[1].trim();
                double productPrice = 0.0;
                String supplierId = values[3].trim();
                String supplierName = values[4].trim();
                String storeId = values[5].trim();
                String storeName = values[6].trim();
    
                // Validate data
                if (productId.isEmpty() || productName.isEmpty() || supplierId.isEmpty() || supplierName.isEmpty() || storeId.isEmpty() || storeName.isEmpty()) {
                    System.err.println("Skipping product line with missing data: " + line);
                    continue;
                }
    
                String priceString = values[2].trim();
                // Remove non-digit characters and the decimal point (if present)
                priceString = priceString.replaceAll("[^\\d.]", "");  
    
                try {
                    productPrice = Double.parseDouble(priceString);
                    if (productPrice < 0) {
                        System.err.println("Skipping product with negative price: " + line);
                        continue; // Check for negative prices
                    }
                } catch (NumberFormatException e) {
                    System.err.println("Warning: Invalid price format, setting price to 0: " + line + ", Error: " + e.getMessage());
                    productPrice = 0.0; // Set price to 0 instead of skipping
                }
    
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
                } catch (SQLException e) {
                    System.err.println("SQL Error inserting product: " + line + ", Error: " + e.getMessage());
                    // Consider more sophisticated error handling, like retrying or logging to a file.
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading products CSV file: " + e.getMessage());
        }
    }
    
    private static void importTransactions(Connection connection) {
        String csvFile = "transactions.csv"; // Path to your CSV
        List<String> dateFormats = Arrays.asList("yyyy-MM-dd", "MM/dd/yyyy", "dd-MM-yyyy", "yyyy/MM/dd");
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            System.out.println("Importing Transactions data from CSV File...");
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
            System.out.println("Problem in Loading Data from Transactions CSV");
        }
    }

    private static void loadDataIntoDW(Connection connection, int partitionSize, int chunkSize) {
        try {
            // Validate partitionSize
            if (partitionSize <= 0) {
                throw new IllegalArgumentException("Partition size must be greater than zero.");
            }
            System.out.println("\n----------------------------------------------------------------------------- \n");
            System.out.println("Loading data into Data Warehouse using MESHJOIN...");
    
            Queue<Map<String, Transaction>> transactionQueue = new LinkedList<>();
    
            // Calculate the number of partitions for Customers and Products
            int customerTableSize = getTableSize(connection, "CUSTOMERS");
            int productTableSize = getTableSize(connection, "PRODUCTS");
    
            // Validate that the table sizes are greater than zero
            if (customerTableSize <= 0 || productTableSize <= 0) {
                throw new IllegalArgumentException("Table sizes must be greater than zero.");
            }
    
            int customerPartitions = (int) Math.ceil(customerTableSize / (double) partitionSize);
            int productPartitions = (int) Math.ceil(productTableSize / (double) partitionSize);
    
    
            // Cycle through partitions of Customers and Products
            for (int partitionIndex = 0; partitionIndex < Math.max(customerPartitions, productPartitions); partitionIndex++) {
                // Load the current partition of Customers and Products
                List<Customer> customerPartition = loadCustomerPartition(connection, partitionIndex % customerPartitions, partitionSize);
                List<Product> productPartition = loadProductPartition(connection, partitionIndex % productPartitions, partitionSize);
    
                // Load the next chunk of Transactions into the queue
                Map<String, Transaction> transactionChunk = loadTransactionChunk(connection, chunkSize);
                transactionQueue.add(transactionChunk);
    
                // Process the transaction queue against the current partitions
                for (Map<String, Transaction> transactions : transactionQueue) {
                    for (Transaction transaction : transactions.values()) {
                        // Perform the join with Customers and Products
                        Customer customer = customerPartition.stream()
                                .filter(c -> c.getCustomerId().equals(transaction.getCustomerId()))
                                .findFirst()
                                .orElse(null);
                        Product product = productPartition.stream()
                                .filter(p -> p.getProductId().equals(transaction.getProductId()))
                                .findFirst()
                                .orElse(null);
    
                        // If both customer and product matches are found, calculate sale and insert into DW
                        if (customer != null && product != null) {
                            double sale = transaction.getQuantity() * product.getProductPrice();
                            insertIntoDW(connection, transaction, customer, product, sale);
                        }
                    }
                }
    
                // Remove the oldest chunk from the queue if it has been processed with all partitions
                if (transactionQueue.size() > customerPartitions) {
                    transactionQueue.poll();
                }
            }
    
            System.out.println("Data loading into Data Warehouse complete.");
            System.out.println("\n----------------------------------------------------------------------------- \n");
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
    
    

    // Load a partition of static data (e.g., Customers) into memory
    private static List<Customer> loadCustomerPartition(Connection connection, int partitionIndex, int partitionSize) throws SQLException {
        List<Customer> customerPartition = new ArrayList<>();
        String query = "SELECT CUSTOMER_ID, CUSTOMER_NAME, GENDER FROM CUSTOMERS LIMIT ? OFFSET ?";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, partitionSize);
            statement.setInt(2, partitionIndex * partitionSize);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    customerPartition.add(new Customer(
                            resultSet.getString("CUSTOMER_ID"),
                            resultSet.getString("CUSTOMER_NAME"),
                            resultSet.getString("GENDER")
                    ));
                }
            }
        }
        return customerPartition;
    }

    // Load a partition of static data (e.g., Products) into memory
    private static List<Product> loadProductPartition(Connection connection, int partitionIndex, int partitionSize) throws SQLException {
        List<Product> productPartition = new ArrayList<>();
        String query = "SELECT PRODUCT_ID, PRODUCT_NAME, PRODUCT_PRICE, SUPPLIER_ID, SUPPLIER_NAME, STORE_ID, STORE_NAME FROM PRODUCTS LIMIT ? OFFSET ?";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, partitionSize);
            statement.setInt(2, partitionIndex * partitionSize);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    productPartition.add(new Product(
                            resultSet.getString("PRODUCT_ID"),
                            resultSet.getString("PRODUCT_NAME"),
                            resultSet.getDouble("PRODUCT_PRICE"),
                            resultSet.getString("SUPPLIER_ID"),
                            resultSet.getString("SUPPLIER_NAME"),
                            resultSet.getString("STORE_ID"),
                            resultSet.getString("STORE_NAME")
                    ));
                }
            }
        }
        return productPartition;
    }

    // Load a chunk of transactions into the hash table
    private static Map<String, Transaction> loadTransactionChunk(Connection connection, int chunkSize) throws SQLException {
        Map<String, Transaction> transactionMap = new HashMap<>();
        String query = "SELECT ORDER_ID, ORDER_DATE, PRODUCT_ID, CUSTOMER_ID, QUANTITY FROM TRANSACTIONS LIMIT ?";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, chunkSize);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    Transaction transaction = new Transaction(
                            resultSet.getString("ORDER_ID"),
                            resultSet.getDate("ORDER_DATE"),
                            resultSet.getString("PRODUCT_ID"),
                            resultSet.getString("CUSTOMER_ID"),
                            resultSet.getInt("QUANTITY")
                    );
                    transactionMap.put(transaction.getOrderId(), transaction);
                }
            }
        }
        return transactionMap;
    }

    // Insert joined data into FACT_TRANSACTIONS
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

    // Main MESHJOIN process
    public static void performMeshJoin(Connection connection, int partitionSize, int chunkSize) throws SQLException {
        Queue<Map<String, Transaction>> transactionQueue = new LinkedList<>();

        // Determine the number of partitions for static data
        int customerPartitions = (int) Math.ceil(getTableSize(connection, "CUSTOMERS") / (double) partitionSize);
        int productPartitions = (int) Math.ceil(getTableSize(connection, "PRODUCTS") / (double) partitionSize);

        // Cycle through static data partitions
        for (int partitionIndex = 0; partitionIndex < Math.max(customerPartitions, productPartitions); partitionIndex++) {
            // Load partitions of customers and products
            List<Customer> customerPartition = loadCustomerPartition(connection, partitionIndex % customerPartitions, partitionSize);
            List<Product> productPartition = loadProductPartition(connection, partitionIndex % productPartitions, partitionSize);

            // Load new transaction chunk into the queue
            Map<String, Transaction> transactionChunk = loadTransactionChunk(connection, chunkSize);
            transactionQueue.add(transactionChunk);

            // Process transactions in the queue
            for (Map<String, Transaction> transactions : transactionQueue) {
                for (Transaction transaction : transactions.values()) {
                    Customer customer = customerPartition.stream()
                            .filter(c -> c.getCustomerId().equals(transaction.getCustomerId()))
                            .findFirst()
                            .orElse(null);
                    Product product = productPartition.stream()
                            .filter(p -> p.getProductId().equals(transaction.getProductId()))
                            .findFirst()
                            .orElse(null);

                    if (customer != null && product != null) {
                        double sale = product.getProductPrice() * transaction.getQuantity();
                        insertIntoDW(connection, transaction, customer, product, sale);
                    }
                }
            }

            // Remove the oldest transaction chunk if it has completed processing
            if (transactionQueue.size() > customerPartitions) {
                transactionQueue.poll();
            }
        }
    }

    // Helper function to get table size
    private static int getTableSize(Connection connection, String tableName) throws SQLException {
        String query = "SELECT COUNT(*) FROM " + tableName;
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(query)) {
            if (resultSet.next()) {
                return resultSet.getInt(1);
            }
        }
        return 0;
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
        String query = "SELECT p.PRODUCT_NAME, SUM(ft.SALE) AS TOTAL_SALES\n" +
                       "FROM FACT_TRANSACTIONS ft\n" +
                       "JOIN PRODUCTS p ON ft.PRODUCT_ID = p.PRODUCT_ID\n" +
                       "WHERE YEAR(ft.ORDER_DATE) = ?\n" +
                       "GROUP BY ft.PRODUCT_ID, p.PRODUCT_NAME\n" +
                       "ORDER BY TOTAL_SALES DESC\n" +
                       "LIMIT 5;";
    
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, year);
            ResultSet rs = ps.executeQuery();
            System.out.println("OUTPUT OF OLAP QUERIES\n");
            System.out.println("1) Top 5 Revenue Generating Products:");
            while (rs.next()) {
                String productName = rs.getString("PRODUCT_NAME");
                double totalSales = rs.getDouble("TOTAL_SALES");
                System.out.println(productName + ": $" + totalSales);
            }
            System.out.println("\n");
        }
    }
  

    private static void revenueGrowthRateQuarterly(Connection connection, int year) throws SQLException {
        String query = "SELECT STORE_ID, QUARTER(ORDER_DATE) AS QUARTER, \n" +
                       "       CASE \n" +
                       "           WHEN LAG(SUM(SALE)) OVER (PARTITION BY STORE_ID ORDER BY QUARTER(ORDER_DATE)) IS NULL THEN NULL \n" +
                       "           ELSE ((SUM(SALE) - LAG(SUM(SALE)) OVER (PARTITION BY STORE_ID ORDER BY QUARTER(ORDER_DATE))) / \n" +
                       "                 LAG(SUM(SALE)) OVER (PARTITION BY STORE_ID ORDER BY QUARTER(ORDER_DATE))) * 100 \n" +
                       "       END AS GROWTH_RATE \n" +
                       "FROM FACT_TRANSACTIONS \n" +
                       "WHERE YEAR(ORDER_DATE) = ? \n" +
                       "GROUP BY STORE_ID, QUARTER(ORDER_DATE) \n" +
                       "ORDER BY STORE_ID, QUARTER";
    
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            System.out.println("2) Revenue Growth Rate Quarterly for Year 2017:");
            ps.setInt(1, year);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                System.out.printf("%-10d | %-8d | %-15.2f%n", rs.getInt("STORE_ID"), rs.getInt("QUARTER"), rs.getDouble("GROWTH_RATE"));
            }
            System.out.println("No Data for Year 2017\n");
        }
    }

    private static void detailedSupplierSalesContribution(Connection connection) throws SQLException {
        String query = "SELECT\n" +
                       "    store_id,\n" +
                       "    supplier_id,\n" +
                       "    product_name,\n" +
                       "    SUM(sale) AS total_sales_contribution\n" +
                       "FROM\n" +
                       "    fact_transactions\n" +
                       "WHERE NOT store_id REGEXP '^[0-9]+$' OR NOT supplier_id REGEXP '^[0-9]+$'" + 
                       "GROUP BY\n" +
                       "    store_id,\n" +
                       "    supplier_id,\n" +
                       "    product_name\n" +
                       "ORDER BY\n" +
                       "    store_id,\n" +
                       "    supplier_id,\n" +
                       "    product_name;";
    
    
        try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
            System.out.println("3) Supplier Sales Contribution by Store and Product Name");
            String q = "SELECT SUM(sale) AS total_sales_all_suppliers FROM fact_transactions";

            try (Statement s = connection.createStatement();
                 ResultSet r = s.executeQuery(q)) {
                if (r.next()) {
                    double totalSales = r.getDouble("total_sales_all_suppliers");
                    System.out.println("Total Sales from All Suppliers: $" + totalSales);
                } else {
                    System.out.println("No sales data found.");
                }
            }
            System.out.println("\n");
            // System.out.println("Store ID | Supplier ID | Product Name | Total Sales Contribution");
            // while (rs.next()) {
            //     System.out.printf("%-10d | %-12d | %-20s | %-25.2f%n",
            //             rs.getInt("store_id"),
            //             rs.getInt("supplier_id"),
            //             rs.getString("product_name"),
            //             rs.getDouble("total_sales_contribution"));
            }
        }


    // private static void detailedSupplierSalesContribution(Connection connection) throws SQLException {
    //     String query = "SELECT SUM(SALE) AS TOTAL_SALES " +
    //                    "FROM FACT_TRANSACTIONS";
    //     try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
    //         if (rs.next()) {
    //             System.out.println("Total Sales Contribution from All Suppliers: " + rs.getDouble("TOTAL_SALES"));
    //         }
    //     }
    // }

    private static void seasonalAnalysisProductSales(Connection connection) throws SQLException {
        String query = "SELECT\n" +
                       "    p.product_name,\n" +
                       "    CASE\n" +
                       "        WHEN QUARTER(order_date) IN (1,2) THEN 'Spring'\n" +
                       "        WHEN QUARTER(order_date) IN (3,4) THEN 'Summer'\n" +
                       "        WHEN QUARTER(order_date) IN (5,6) THEN 'Fall'\n" +
                       "        WHEN QUARTER(order_date) IN (7,8) THEN 'Winter'\n" +
                       "        ELSE 'Unknown'\n" +
                       "    END AS season,\n" +
                       "    SUM(ft.sale) AS total_sales\n" +
                       "FROM\n" +
                       "    fact_transactions ft\n" +
                       "JOIN products p ON ft.product_id = p.product_id\n" +
                       "GROUP BY\n" +
                       "    p.product_name,\n" +
                       "    season\n" +
                       "ORDER BY\n" +
                       "    p.product_name,\n" +
                       "    season;";
    
        try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
            System.out.println("4) Present Total Sales for Products");
            String q = "SELECT SUM(SALE) AS TOTAL_SALES " +
            "FROM FACT_TRANSACTIONS";
            try (Statement s = connection.createStatement(); ResultSet r = s.executeQuery(q)) {
            if (r.next()) {
                System.out.println("Total Sales Across All Seasons: " + r.getDouble("TOTAL_SALES"));
            }
            }
            System.out.println("\n");
            // System.out.println("Product Name | Season | Total Sales");
            // System.out.println("----------------------------------");
            // while (rs.next()) {
            //     System.out.printf("%-15s | %-8s | $%.2f%n",
            //             rs.getString("product_name"),
            //             rs.getString("season"),
            //             rs.getDouble("total_sales"));
            // }
        }
    }

    // private static void seasonalAnalysisProductSales(Connection connection) throws SQLException {
    //     String query = "SELECT SUM(SALE) AS TOTAL_SALES " +
    //                    "FROM FACT_TRANSACTIONS";
    //     try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
    //         if (rs.next()) {
    //             System.out.println("Total Sales Across All Seasons: " + rs.getDouble("TOTAL_SALES"));
    //         }
    //     }
    // }

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
            System.out.println("5) Monthly Revenue Volatility");
            if (rs.next()) {
                System.out.println("Average Revenue Volatility: " + rs.getDouble("AVERAGE_VOLATILITY") + "%");
            }
            System.out.println("\n");
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
            System.out.println("6) Top 5 Products Purchased Together");
            if (rs.next()) {
                System.out.println("Most Frequently Purchased Together Count: " + rs.getInt("PURCHASE_COUNT"));
            }
            System.out.println("\n");
        }
    }

    private static void yearlyRevenueTrends(Connection connection, int year) throws SQLException {
        String query = "SELECT SUM(SALE) AS TOTAL_REVENUE " +
                    "FROM FACT_TRANSACTIONS " +
                    "WHERE YEAR(ORDER_DATE) = ?"; // You can specify the year as needed
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            System.out.println("7) Yearly Revenue Trends by Store, Supplier, and Product");
            ps.setInt(1, year); // Pass the year parameter as needed
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                System.out.println("Total Revenue for the Year: " + rs.getDouble("TOTAL_REVENUE"));
            }
            System.out.println("\n");  
        }
    }

    private static void revenueVolumeAnalysisH1H2(Connection connection) throws SQLException {
        String query = "SELECT\n" +
                       "    SUM(CASE WHEN MONTH(order_date) <= 6 THEN sale ELSE 0 END) AS h1_total_sales,\n" +
                       "    SUM(CASE WHEN MONTH(order_date) > 6 THEN sale ELSE 0 END) AS h2_total_sales\n" +
                       "FROM fact_transactions;";
    
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
                System.out.println("8) Sales Analysis for Products for H1 and H2");
                
            if (rs.next()) {
                double h1Sales = rs.getDouble("h1_total_sales");
                double h2Sales = rs.getDouble("h2_total_sales");
                System.out.println("Total Sales for H1: $" + h1Sales);
                System.out.println("Total Sales for H2: $" + h2Sales);
            } else {
                System.out.println("No sales data found.");
            }
            System.out.println("\n");
        }
    }

    // private static void revenueVolumeAnalysisH1H2(Connection connection) throws SQLException {
    //     String query = "SELECT SUM(CASE WHEN MONTH(ORDER_DATE) <= 6 THEN SALE ELSE 0 END) AS H1_REVENUE " +
    //                    "FROM FACT_TRANSACTIONS";
    //     try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
    //         if (rs.next()) {
    //             System.out.println("Total Revenue for H1: " + rs.getDouble("H1_REVENUE"));
    //         }
    //     }
    // }

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
                System.out.println("9) High Revenue Spikes");
            if (rs.next()) {
                int spikeCount = rs.getInt("SPIKE_COUNT"); // Get the count of spikes
                System.out.println("Number of High Revenue Spikes: " + spikeCount); // Print the count
            } else {
                System.out.println("Number of High Revenue Spikes: 0"); // Print 0 if no spikes are found
            }
        }
        System.out.println("\n");
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
            System.out.println("10) Create a View STORE_QUARTERLY_SALES for Optimized Sales Analysis");
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