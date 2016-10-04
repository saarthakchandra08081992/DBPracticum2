package utils;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import error.SQLCustomErrorHandler;

/**
 * 
 * We use this class to read from the conf.properties file ... That property
 * file has some of the paths/sub-directories of our project
 * 
 * @authors 
 * Saarthak Chandra - sc2776 
 * Shweta Shrivastava - ss3646 
 * Vikas P Nelamangala - vpn6
 * 
 *
 */
public class PropertyFileReader extends Properties {
	private static String FILE_NAME = "conf.properties"; // Our property file
															// name
	private static String ROOT_PATH = "./src/";

	private static PropertyFileReader instance = null;

	/**
	 * Private constructor to prevent objects of this class from being made
	 */
	private PropertyFileReader() {
	}

	/**
	 * This function returns the propertyFile singleton object
	 * 
	 * @return Singleton instance of the PropertyFileReader class
	 */
	public static PropertyFileReader getInstance() {
		if (instance == null) {
			try {
				instance = new PropertyFileReader();
				setPropertiesFile();
				if (checkFile(ROOT_PATH + FILE_NAME)) {
					FileInputStream in = new FileInputStream(ROOT_PATH + FILE_NAME);
					instance.load(in);
					in.close();
				}
				else
					setPropertiesFile();
			} catch (Exception ex) {
				SQLCustomErrorHandler handler = new SQLCustomErrorHandler(ex.getMessage());
				return null;
			}
		}
		return instance;
	}

	/**
	 * 
	 * Return the status if propery file exists or not
	 * 
	 * @param filePathString
	 * 				File path of the properties file we check
	 * @return
	 * 		true if file exists, else false
	 */
	public static boolean checkFile(String filePathString) {
		File f = new File(filePathString);
		return f.exists();
	}

	/**
	 * Fallback if we are unable to read the property file, we set the properties here
	 * 
	 */
	public static void setPropertiesFile() {
		instance.setProperty("schemaFileName", "schema.txt");
		instance.setProperty("queriesFileName", "queries.sql");
		instance.setProperty("dataSubDirectory", "data");
		instance.setProperty("dbSubDirectory", "db");
		instance.setProperty("outputFilePrefix", "query");
	}
}