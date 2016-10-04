package helpers;

import java.util.List;

/**
 * A helper function for Sort which helps it to identify the position of a given columnName in the table schema
 * 
 * @author 
 * Saarthak Chandra - sc2776
 * Shweta Shrivastava - ss3646
 * Vikas P Nelamangala - vpn6
 *
 */
public class SortHelper {	
	
	/**
	 * Identify the position of the given column in table schema
	 * To handle both plain column names and column names preceded by table names
	 * @param selectedAttribute
	 * 						Name of the column
	 * @param tableSchema
	 * 						Schema of the table
	 * @return The position of the column in the schema
	 */
	public static int getAttributePosition(String selectedAttribute, List<String> tableSchema) {
		int index = tableSchema.indexOf(selectedAttribute);
		if (index != -1) return index;
		else {
			for(int i = 0; i < tableSchema.size(); i++) {
				if (tableSchema.get(i).split("\\.")[1].equals(selectedAttribute)) return i;
			}
			return -1;
		}
	}
}
