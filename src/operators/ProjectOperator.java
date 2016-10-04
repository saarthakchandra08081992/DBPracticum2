package operators;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.*;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import helpers.AttributeMapper;
import models.Tuple;

/**
 * Class for Project Operator 
 * The ProjectOperator will be created when query does NOT have a SELECT * but instead chooses some specific columns
 * ProjectOperator extends the UnaryOperator and has one child which maybe the ScanOperator or SelectOperator
 * If query has a WHERE clause, the SelectOperator is the child. If it doesn't, ScanOperator is the child
 * 
 * @author
 * Saarthak Chandra - sc2776
 * Shweta Shrivastava - ss3646
 * Vikas P Nelamangala - vpn6
 *
 */

public class ProjectOperator extends UnaryOperator {

	HashSet<String> allColumnsOfTable = new HashSet<String>();
	List<String> childSchema = null;

	/**
	 * Constructor for Project Operator. It takes a list of SelectItem and child operator
	 * The SelectItem can be -
	 * SelectExpressionItem (SELECT A as X, SELECT S.A from S)
	 * AllColumns (SELECT *)
	 * 
	 * @param selectItemList
	 *            		List of columns that need to be projected
	 * @param child
	 *            Child operator
	 */
	public ProjectOperator(List<SelectItem> selectItemList, Operator child) {
		super(child);
		// TODO Auto-generated method stub
		//System.out.println("Came inside Project Operator Constructor");
		childSchema = child.getSchema();
		extractAllColumnsofTable(selectItemList);
	}
	
	/**
	 * Generates the list of projected columns from the given selectItemList 
	 * @param selectItemList
	 * 					List of selectItems from the input query
	 */
	public void extractAllColumnsofTable(List<SelectItem> selectItemList)
	{
		List<String> tempChildSchema = new ArrayList<String>();
		for (SelectItem selectItem : selectItemList) {
		
			//Check for SELECT * condition first, if yes, the schema remains as-is
			if (selectItem instanceof AllColumns) 
			{
				schema = childSchema;
				return;
			}	
			else {
				Column projectedColumn = (Column)((SelectExpressionItem) selectItem).getExpression();
				if (projectedColumn.getTable() != null && (projectedColumn.getTable().getName() != null)) 
				{
					String table = projectedColumn.getTable().getName();
					if (allColumnsOfTable.contains(table))
					{
						continue;
					}	
					tempChildSchema.add(table + '.' + projectedColumn.getColumnName());
				} 
				else {
					String projectedColumnName = projectedColumn.getColumnName();
					for (String childSchemaColumn : childSchema) {
						if (((String) childSchemaColumn.split("\\.")[1]).equals(projectedColumnName)) {
							tempChildSchema.add(childSchemaColumn);
							break;
						}
					}
				}
			}
		}
		
		if (allColumnsOfTable.isEmpty())
			schema = tempChildSchema;
		else {
			for (String childSchemaColumn : childSchema) {
				String tableName = childSchemaColumn.split(".")[0];
				if (allColumnsOfTable.contains(tableName) || tempChildSchema.contains(childSchemaColumn))
					schema.add(childSchemaColumn);
			}
		}
	}

	/**
	 * Read the next tuple via the getNextTuple method of the child
	 * Then generate a tuple which projects the desired columns from the schema
	 * @return The projected tuple
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple childNextTuple = child.getNextTuple();
		
		if (childNextTuple == null)
			return null;
		
		int[] projectedAttributes = new int[schema.size()];

		int k = 0;
		for (String schemaCol : schema) {
			Long projectedColumnValue = AttributeMapper.getColumnActualValue(childNextTuple, child.getSchema(), schemaCol);
			projectedAttributes[k++] = projectedColumnValue.intValue();
		}

		return new Tuple(projectedAttributes);
	}
}
