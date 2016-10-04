package operators;

import net.sf.jsqlparser.statement.select.OrderByElement;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import helpers.CustomTupleComparator;
import helpers.SortHelper;
import models.Tuple;


/**
 * Class for Sort Operator. The SortOperator is created when query has ORDER BY clause
 * It sorts the final output of the child and becomes the root operator in such case
 * (given query doesn't do a DISTINCT after that)
 * 
 * @author
 * Saarthak Chandra - sc2776
 * Shweta Shrivastava - ss3646
 * Vikas P Nelamangala - vpn6
 *
 */

public class SortOperator extends UnaryOperator {
	
	Tuple tuple;
	List<Tuple> tuplesList = new ArrayList<Tuple>();
	List<Integer> ordersList = new ArrayList<Integer>();
	int index = 0;
	
	/**
	 * Constructor for Sort Operator
	 * Iteratively reads next tuple of child and uses Collections.sort() for ordering
	 * @param orderElements
	 * 					The list of ORDER BY elements
	 * @param child
	 * 				Child operator
	 */
	public SortOperator(ArrayList<OrderByElement> orderElements,Operator child) {
		super(child);
		// TODO Auto-generated constructor stub
		
		while(true){
			if((tuple = child.getNextTuple()) != null){
					tuplesList.add(tuple);
				}
			else {
				break;
			}
			for (OrderByElement element : orderElements){
				this.ordersList.add(SortHelper.getAttributePosition(element.toString(),child.getSchema()));
			}	
		}
		
		Collections.sort(tuplesList, new CustomTupleComparator(this.ordersList));
			
	}

	/**
	 * To get the next tuple of the sorted list
	 * Uses index to keep track of the read head
	 * @return The next sorted tuple
	 */
	@Override
	public Tuple getNextTuple() {
		// TODO Auto-generated method stub
		if (index >= tuplesList.size()) {
			return null;
			}
		else {
			return tuplesList.get(index++);
			}
	}
	
	/**
	 * Reset index to 0
	 */
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		index = 0;
	}

}
