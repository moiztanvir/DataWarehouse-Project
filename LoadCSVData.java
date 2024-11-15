import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LoadCSVData {
    public static void main(String[] args) {

        System.out.println("Data from Customers Table:");
        List<String[]> csvData1 = loadCSV("customers_data.csv", ",", 5);
        printCSVData(csvData1);

        System.out.println("\nData from Products Table:");
        List<String[]> csvData2 = loadCSV("products_data.csv", ",", 5);
        printCSVData(csvData2);
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
