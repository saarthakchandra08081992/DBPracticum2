package helpers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.*;

/**
 * File with helpers needed by SelectExecutor, which builds the expression tree
 * 
 * @author 
 * Saarthak Chandra - sc2776
 * Shweta Shrivastava - ss3646
 * Vikas P Nelamangala - vpn6
 *
 */
public class SelectExecutorHelper {

	/**
	 * Get table name from the input FromItem type
	 * 
	 * @param fromItm
	 *            String that contains the table name, in the from part of an
	 *            expression
	 * @return First table name, that we get by splitting based on space
	 */
	public static String getSingleTableName(FromItem fromItm) {
		String returnTable = fromItm.toString().split(" ")[0];
		return returnTable;
	}

	/**
	 * 
	 * @param expression
	 *            	Expression to parse all and conditions
	 * @return List of smaller Expressions
	 */
	public static List<Expression> getListOfExpressionsAnds(Expression expression) {
		List<Expression> expressionList = new ArrayList<Expression>();
		while (expression instanceof AndExpression) {
			AndExpression andExpression = (AndExpression) expression;
			expressionList.add(andExpression.getRightExpression());
			expression = andExpression.getLeftExpression();
		}
		expressionList.add(expression);

		return expressionList;
	}

	/**
	 * In a binary expression, analyze table elements
	 * 
	 * @param expression
	 *            The binary expression
	 * @return The list of tables that are mentioned
	 * 
	 */

	public static List<String> getTabsInExpression(Expression expression) {
		List<String> expressionList = new ArrayList<String>();

		if ((expression instanceof BinaryExpression)) { // ONLY FOR BINARY
														// EXPRESSIONS

			BinaryExpression binaryExpression = (BinaryExpression) expression;
			Expression left = binaryExpression.getLeftExpression();
			Expression right = binaryExpression.getRightExpression();

			Column col;
			if (left instanceof Column) {
				col = (Column) left;
				expressionList.add(col.getTable().toString());
			}
			if (right instanceof Column) {
				col = (Column) right;
				// Add only if we have a different table
				if (!(expressionList.size() == 1 && expressionList.get(0).equals(col.getTable().toString())))
					expressionList.add(col.getTable().toString());
			}
		}
		return expressionList;
	}

	/**
	 * Join and expressions into one .
	 * 
	 * @param exps
	 *            The list of binary expressions
	 * @return The final AND expression
	 */
	public static Expression getConcatenatedAnds(List<Expression> expressions) {
		if (expressions.isEmpty())
			return null;
		
		Expression joinedExpression = expressions.get(0);
		
		int i = 1;
		while (i < expressions.size()) {
			// joins lhs and rhs
			i++;
			joinedExpression = new AndExpression(joinedExpression, expressions.get(i));
		}
		return joinedExpression;
	}

	/**
	 * Check if the ordered elements are not selected.
	 * We return FALSE if all columns we have in select are also part of orderBy
	 * We return TRUE if all columns we have in select are NOT part of orderBy
	 * 
	 * @param selectItems
	 *              List of selected items
	 * @param orderByElement
	 *              List of ordered elements
	 * @return True/False
	 */
	public static boolean selectAllOrderColumns(List<SelectItem> selectItems, List<OrderByElement> orderByElement) {

		HashSet<String> selectedColumns = new HashSet<String>();
		HashSet<String> orderedColumns = new HashSet<String>();

		if (selectItems.get(0) instanceof AllColumns)
			return false;

		
		// return if we get null/empty expression
		if (orderByElement == null || orderByElement.isEmpty())
			return false;

		for (OrderByElement orderByElements : orderByElement)
			orderedColumns.add(orderByElements.toString());

		for (SelectItem selectedItem : selectItems)
			selectedColumns.add(selectedItem.toString());

		orderedColumns.removeAll(selectedColumns);
		return !orderedColumns.isEmpty();
	}

}
