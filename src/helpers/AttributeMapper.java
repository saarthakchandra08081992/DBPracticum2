package helpers;

import models.Tuple;
import java.util.List;

/**
 * The AttributeMapper maps is a singleton class
 * which implements a method to map the column name to the actual value 
 * in a given tuple using the tuple schema
 * 
 * @author 
 * Saarthak Chandra - sc2776
 * Shweta Shrivastava - ss3646
 * Vikas P Nelamangala - vpn6
 *
 */

public class AttributeMapper {

	/**
	 * Map the column name to the actual value by first identifying the index of the given columnName
	 * in the schema of the tuple and then retrieving the value at that index in the given tuple
	 * We also add another step here in order to handle columns which have an alias associated
	 * @param t
	 * 			Tuple from which the column value needs to be extracted
	 * @param tSchema
	 * 			Schema for the given tuple
	 * @param columnName
	 * 			Name of the column for which value is to be extracted
	 * @return	The value of that column in the given tuple
	 */
	public static Long getColumnActualValue(Tuple t, List<String> tSchema, String columnName){
		int columnIndexInSchema = tSchema.indexOf(columnName);
		if(columnIndexInSchema != -1)
			return (long)t.getValue(columnIndexInSchema);
		else{
			for(int i=0;i<tSchema.size();i++){
				if(columnName.equals((tSchema.get(i)).split("\\.")[1])){
					return (long)t.getValue(i);
				}
			}
		}
		return null;
	}
}
