import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LoadCSVData2 {
    public static void main(String[] args) {

        System.out.println("Data from Customers Table:");
        List<String[]> csvData1 = loadCSV("customers_data.csv", ",", 5);
        printCSVData(csvData1);

        System.out.println("\nData from Products Table:");
        List<String[]> csvData2 = loadCSV("products_data.csv", ",", 5);
        printCSVData(csvData2);

        // Example of real-time row loading
        System.out.println("\nReal-time row loading:");
        try (RealTimeCSVReader realTimeReader = new RealTimeCSVReader("transactions_data.csv", ",")) {
            while (realTimeReader.hasNext()) {
                String[] row = realTimeReader.next();
                System.out.println("Row:");
                for (String value : row) {
                    System.out.println(value);
                }
            }
        } catch (IOException e) 
        {
            e.printStackTrace();
        }
    }

    public static List<String[]> loadCSV(String filePath, String delimiter, int maxRows) {
        List<String[]> data = new ArrayList<>(); // List to store rows of CSV data
        String line = "";
        int rowCount = 0; // Counter to track the number of rows read

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            // Read the header line
            String header = br.readLine();
            if (header != null) {
                System.out.println("\nHeader: " + header);
            }

            // Read and process each line in the file, but limit to maxRows
            while ((line = br.readLine()) != null && rowCount < maxRows) {
                // Split the line into columns and add to the data list
                String[] values = line.split(delimiter);
                data.add(values);
                rowCount++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return data;
    }

    public static void printCSVData(List<String[]> csvData) {
        for (String[] row : csvData) {
            System.out.println("Row:");
            for (String value : row) {
                System.out.println(value);
            }
        }
    }
}

class RealTimeCSVReader implements Iterator<String[]>, AutoCloseable {
    private BufferedReader br;
    private String delimiter;
    private String nextLine;

    public RealTimeCSVReader(String filePath, String delimiter) throws IOException {
        this.br = new BufferedReader(new FileReader(filePath));
        this.delimiter = delimiter;
        this.nextLine = br.readLine(); // Read the header line
        if (this.nextLine != null) {
            System.out.println("Header: " + this.nextLine);
            this.nextLine = br.readLine(); // Move to the first row
        }
    }

    @Override
    public boolean hasNext() {
        return nextLine != null;
    }

    @Override
    public String[] next() {
        if (nextLine == null) {
            throw new IllegalStateException("No more rows available");
        }
        String[] row = nextLine.split(delimiter);
        try {
            nextLine = br.readLine(); // Read the next line
        } catch (IOException e) {
            nextLine = null;
            e.printStackTrace();
        }
        return row;
    }

    @Override
    public void close() throws IOException {
        if (br != null) {
            br.close();
        }
    }
}