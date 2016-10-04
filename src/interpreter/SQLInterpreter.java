package interpreter;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintStream;

import catalog.DBCatalog;
import error.SQLCustomErrorHandler;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import utils.PropertyFileReader;
import utils.SelectExecutor;

/**
 * The SqlInterpreter class in the entry point for the SQLInterpreter.
 * The main() function takes in as arguments the location of the sample inputs
 * It reads the queries one by one and generates output
 * 
 * @authors 
 * Saarthak Chandra - sc2776
 * Shweta Shrivastava - ss3646
 * Vikas P Nelamangala - vpn6
 *
 */
public class SQLInterpreter {
	
	PropertyFileReader reader = PropertyFileReader.getInstance();
	private static String FILE_NAME ;
	/**
	 * Output of the parser with the given input / output directory.
	 * 
	 * @param inPath
	 *            input directory
	 * @param outPath
	 *            output directory
	 * 
	 */
	public void parse(String inPath, String outPath) {
		DBCatalog.createDirectories(inPath, outPath);
		DBCatalog.getCatalogInstance();

		try {
			CCJSqlParser sqlparser = new CCJSqlParser(new FileReader(DBCatalog.queryPath));
			Statement querystatement;
			int querycounter = 1;
			while ((querystatement = sqlparser.Statement()) != null) {
				try {
					File file = new File(DBCatalog.outputDirectory + File.separator + FILE_NAME + querycounter);
					PrintStream printstream = new PrintStream(new BufferedOutputStream(new FileOutputStream(file)));

					SelectExecutor selectExecutor = new SelectExecutor(querystatement);
					//System.out.println("Calling DUMP now.........");
                    selectExecutor.root.dump(printstream);
					// do some printing here
					printstream.close();

					// So we know number of query output files we created.
					querycounter++;

				} catch (Exception ex) {
					SQLCustomErrorHandler handler = new SQLCustomErrorHandler(ex, this.getClass().getSimpleName());
					//ex.printStackTrace();
					
					// Ensure one failure does not halt execution so use continue to process further
					continue;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	/**
	 * Set the output file prefix, read from the property file.
	 */
	private void setOutFilePath(){
		FILE_NAME = reader.getProperty("outputFilePrefix");
	}
	
	/**
	 * Create output directory folder if it does not exist
	 * 
	 * @param outputDirectory
	 * 						Directory provided as input from command line
	 * 
	 */
	private void createOutputDirecoryIfNotExists(String outputDirectory){
		new File(outputDirectory).mkdirs();
	}
	
	/**
	 * The main function invoked by jar.
	 * 
	 * @param args
	 *            Argument list
	 */
	public static void main(String[] args) {
		
		 if (args.length != 2) {
		 throw new IllegalArgumentException("Incorrect input format");
		 }
		
		
		SQLInterpreter sqlInterpret = new SQLInterpreter();
		sqlInterpret.setOutFilePath();
		sqlInterpret.createOutputDirecoryIfNotExists(args[1]);
		sqlInterpret.parse(args[0], args[1]);					

	}
}
