package utils;

import java.util.*;

import catalog.DBCatalog;
import helpers.SelectExecutorHelper;
import models.Table;
import net.sf.jsqlparser.statement.select.*;
import operators.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;

/**
 * This class builds the tree, handles all the aliases as well. This class has a
 * constructor called for every single statement that is provided as input
 * 
 * @authors Saarthak Chandra Shweta Shrivastava Vikas P Nelamangala
 *
 */
public class SelectExecutor {

	public Select selectStat;
	public Distinct distinctElements;
	public PlainSelect plainSelect;
	public List<SelectItem> selectElements;
	public FromItem from;
	public List<Join> joins;
	public Expression where;
	public ArrayList<OrderByElement> orderElements;

	public List<String> tableList = new ArrayList<String>();
	public List<Expression> ands = null;
	public HashMap<String, List<Expression>> selectCondition = null, joinCondition = null;
	public HashMap<String, Expression> selectConditionList, joinConditionList;

	public Operator root = null;

	/**
	 * Constructor. It extracts all the binary expressions and analyze the
	 * relevant ones at each joining stage.
	 * 
	 * @param statement
	 *            the SQL statement
	 */
	public SelectExecutor(Statement statement) {
		selectStat = (Select) statement;
		plainSelect = (PlainSelect) selectStat.getSelectBody();

		populateFromsWithJoinConditions();
		populateFroms();
		plainSelectInit();

		// Now tableList variable has all the select,join tables that are in
		// question
		for (String table : tableList) {
			selectCondition.put(table, new ArrayList<Expression>());
			joinCondition.put(table, new ArrayList<Expression>());
		}

		// After where we have list of Ands, so we loop through that
		ands = SelectExecutorHelper.getListOfExpressionsAnds(where);

		for (Expression exp : ands) {
			List<String> tables = SelectExecutorHelper.getTabsInExpression(exp);
			int ctr = lastIdx(tables);
			if (tables == null)
				joinCondition.get(tableList.get(tableList.size() - 1)).add(exp);

			// Only one table means we do a select
			else if (tables.size() <= 1)
				selectCondition.get(tableList.get(ctr)).add(exp);

			else

				joinCondition.get(tableList.get(ctr)).add(exp);
		}

		selectConditionList = new HashMap<String, Expression>();
		joinConditionList = new HashMap<String, Expression>();

		for (String tab : tableList) {
			selectConditionList.put(tab, SelectExecutorHelper.getConcatenatedAnds(selectCondition.get(tab)));
			joinConditionList.put(tab, SelectExecutorHelper.getConcatenatedAnds(joinCondition.get(tab)));
		}

		generateTree();

		clearArrays();
	}

	/**
	 * Return a table according to its index in the FROM clause.
	 * 
	 * @param idx
	 *            the index
	 * @return the table
	 */
	private Table getTableObject(int idx) {
		Table table = DBCatalog.getTableObject(tableList.get(idx));
		// System.out.println("Table Name="+table.tableName);
		return table;
	}

	/**
	 * The max index of a list of tables in FROM.
	 * 
	 * @param tabs
	 *            the list of tables
	 * @return the last index
	 */
	private int lastIdx(List<String> tables) {
		if (tables == null)
			return tableList.size() - 1;
		int pos = 0;
		for (String tab : tables) {
			pos = Math.max(pos, tableList.indexOf(tab));
		}
		return pos;
	}

	/**
	 * Get the select condition of the idx'th table.
	 * 
	 * @param idx
	 *            the index
	 * @return the final select condition
	 */
	private Expression getSelectCondition(int pos) {
		return selectConditionList.get(tableList.get(pos));
	}

	/**
	 * Get the join condition of the idx'th table with its precedents in FROM.
	 * 
	 * @param idx
	 *            the index
	 * @return the join condition
	 */
	private Expression getJoinCond(int pos) {
		return joinConditionList.get(tableList.get(pos));
	}

	/**
	 * Build the operator tree according to conditions in selectConditionList
	 * and joinConditionList. The tree is built bottom-up
	 */
	private void generateTree() {
		//System.out.println("Inside Genreate Tree....");
		// base of our tree is the scan operator

		Operator rootTemp = new ScanOperator(getTableObject(0));

		if (getSelectCondition(0) != null)
			rootTemp = new SelectOperator((ScanOperator) rootTemp, getSelectCondition(0));

		int i = 1;
		while (i < tableList.size()) {
			Operator scanOp = new ScanOperator(getTableObject(i));
			if (getSelectCondition(i) != null) {
				scanOp = new SelectOperator((ScanOperator) scanOp, getSelectCondition(i));
			}
			rootTemp = new JoinOperator(rootTemp, getJoinCond(i), scanOp);
			i++;
		}

		// After adding join/select nodes, look at operations like order and so
		// on

		boolean orderAllSelectedColumns = SelectExecutorHelper.selectAllOrderColumns(selectElements, orderElements);

		if (orderElements != null && orderAllSelectedColumns)
			rootTemp = new SortOperator(orderElements, rootTemp);

		if (selectElements != null)
			rootTemp = new ProjectOperator(selectElements, rootTemp);

		if (orderElements != null && !orderAllSelectedColumns)
			rootTemp = new SortOperator(orderElements, rootTemp);

		if (distinctElements != null) {
			if (orderElements == null)
				rootTemp = new SortOperator(new ArrayList<OrderByElement>(), rootTemp);

			if (orderAllSelectedColumns)
				rootTemp = new HashDuplicateEliminationOperator(rootTemp);
			else
				rootTemp = new DuplicateEliminationOperator(rootTemp);
		}
		root = rootTemp;
		// System.out.println("Came out of Generate Tree....");
	}

	/**
	 * Here, we add all the tables into a list of tables called tableList, -
	 * using the "from" elements of the query that jsqlParser returns We also
	 * take care of adding aliases here
	 */
	private void populateFroms() {
		DBCatalog.aliases.clear(); // reset previously set aliases
		if (from.getAlias() != null) {
			DBCatalog.aliases.put(from.getAlias(), SelectExecutorHelper.getSingleTableName(from));
			tableList.add(from.getAlias());
		} else
			// When we have the full table name
			tableList.add(from.toString());
	}

	/**
	 * Function to add all the tables, the ones that are involved in joins
	 */
	private void plainSelectInit() {
		if (joins != null) {
			for (Join join : joins) { // loop through all the joins
				FromItem item = join.getRightItem();
				if (item.getAlias() != null) { // check if we have an alias
					//System.out.println(item.toString());

					DBCatalog.aliases.put(item.getAlias(), SelectExecutorHelper.getSingleTableName(item));
					tableList.add(item.getAlias());
				} else
					tableList.add(item.toString());
			}
		}
		selectCondition = new HashMap<String, List<Expression>>();
		joinCondition = new HashMap<String, List<Expression>>();
	}

	/**
	 * Use jsqlParser , to get out all the parts of the current statement being
	 * executed
	 */
	private void populateFromsWithJoinConditions() {
		distinctElements = plainSelect.getDistinct();
		selectElements = plainSelect.getSelectItems();
		from = plainSelect.getFromItem();
		joins = plainSelect.getJoins();
		where = plainSelect.getWhere();
		orderElements = (ArrayList<OrderByElement>) plainSelect.getOrderByElements();
	}

	/**
	 * Clear up the list of arrays, so we start afresh for the next statement
	 * that is passed in
	 * 
	 */
	private void clearArrays() {
		selectCondition.clear();
		joinCondition.clear();
		selectConditionList.clear();
		joinConditionList.clear();
	}

}
