package example.mobile_price;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import java.io.*;
import java.sql.*;
import java.util.List;

public class CSVLoader {
	private static final String CSV_FILE_PATH = "D:/DW/staging/data/mobile_prices1.csv";
	private static final String CONTROL_DB = "jdbc:sqlserver://localhost:1433;databaseName=staging;integratedSecurity=true;encrypt=false;";

	public int loadCSVToStaging() {
		int recordsLoaded = 0;
		try (CSVReader reader = new CSVReader(new FileReader(CSV_FILE_PATH))) {
			List<String[]> records = reader.readAll();
			// Bỏ qua hàng tiêu đề
			records.remove(0);

			try (Connection conn = DriverManager.getConnection(CONTROL_DB)) {
				String sql = "INSERT INTO staging_mobile (" + "name, brand, model, battery_capacity, screen_size, "
						+ "touchscreen, resolution_x, resolution_y, processor, "
						+ "ram, internal_storage, rear_camera, front_camera, " + "operating_system, price) VALUES "
						+ "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

				PreparedStatement pstmt = conn.prepareStatement(sql);

				for (String[] record : records) {
					pstmt.setString(1, record[0]); // name
					pstmt.setString(2, record[1]); // brand
					pstmt.setString(3, record[2]); // model
					pstmt.setInt(4, Integer.parseInt(record[3])); // battery
					pstmt.setDouble(5, Double.parseDouble(record[4])); // screen
					pstmt.setBoolean(6, Boolean.parseBoolean(record[5])); // touchscreen
					pstmt.setInt(7, Integer.parseInt(record[6])); // res_x
					pstmt.setInt(8, Integer.parseInt(record[7])); // res_y
					pstmt.setString(9, record[8]); // processor
					pstmt.setInt(10, Integer.parseInt(record[9])); // ram
					pstmt.setInt(11, Integer.parseInt(record[10])); // storage
					pstmt.setString(12, record[11]); // rear_cam
					pstmt.setString(13, record[12]); // front_cam
					pstmt.setString(14, record[13]); // os
					pstmt.setDouble(15, Double.parseDouble(record[14])); // price

					pstmt.addBatch();
					recordsLoaded++; // Tăng số lượng bản ghi đã tải lên
				}
				pstmt.executeBatch();
			}
		} catch (IOException | CsvException | SQLException e) {
			e.printStackTrace();
		}
		return recordsLoaded;
	}

}
