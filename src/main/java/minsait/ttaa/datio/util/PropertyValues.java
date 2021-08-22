package minsait.ttaa.datio.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyValues {

	InputStream inputStream;
	Properties prop;

	public PropertyValues() {
		try {
			getPropValues();
		} catch (IOException e) {
			// Logg no encontró el properties
			e.printStackTrace();
		}
	}

	private Properties getPropValues() throws IOException {
		try {
			prop = new Properties();
			String propFileName = "params";

			inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

			if (inputStream != null) {
				prop.load(inputStream);
			} else {
				throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
			}

		} catch (Exception e) {
			System.out.println("Exception: " + e);
		} finally {
			inputStream.close();
		}
		return prop;
	}

	public String getProperty(String key) {
		return prop.getProperty(key);
	}
}