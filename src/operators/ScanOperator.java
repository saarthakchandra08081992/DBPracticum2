package operators;

import java.util.ArrayList;
import java.util.List;
import models.Tuple;
import models.Table;

/**
 * Scan Operator class which calls the Table object methods to read from the data files
 * The Scan Operator is ALWAYS created whenever a query is to be executed
 * It does not have any children and directly extends the Operator class
 * 
 * @author 
 * Saarthak Chandra - sc2776
 * Shweta Shrivastava - ss3646
 * Vikas P Nelamangala - vpn6
 *
 */
public class ScanOperator extends Operator {

	public Table currentTable;
	public List<String> currentSchema;
	
	/**
	 * Scan operator constructor 
	 * Takes in a table object and then constructs the schema for it by appending table name to every column name
	 * @param table 
	 * 			Table object for which Scan operator has to be created for
	 */
	public ScanOperator(Table table){
		this.currentTable = table;
		currentSchema = new ArrayList<String>();
		if (currentTable == null || currentTable.tableSchema == null){
			System.out.println("Table or schema is missing!");
		}
		generateSchema();		
	}
	
	/**
	 * Method which reads the next tuple of the current table object
	 * @return The next tuple
	 */
	@Override
	public Tuple getNextTuple() {
		return currentTable.getNextTuple();
	}

	@Override
	/**
	 * Method which resets the current tuple pointer to the beginning of the table file
	 */
	public void reset() {
		currentTable.reset();

	}
	
	/**
	 * Get the schema for the current table
	 * @return The schema for the current table
	 */
	public List<String> getSchema() {
		return currentSchema;
	}
	
	/**
	 * Generate schema for the current table object by appending table name to each column
	 * Add it to the currentSchema member
	 */
	public void generateSchema() {
		for (String column : currentTable.tableSchema) {
			//System.out.println("cols="+column);
			currentSchema.add(currentTable.tableName + '.' + column);
		}
	}

}
