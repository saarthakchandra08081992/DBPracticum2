package operators;

import net.sf.jsqlparser.expression.Expression;
import java.util.List;
import expvisitors.ExpressionEvaluator;
import expvisitors.SelectVisitor;
import models.Tuple;


/**
 * Class for Select Operator
 * The SelectOperator will only be created when query has a WHERE condition in it
 * SelectOperator extends the UnaryOperator and has one child which is the ScanOperator
 *
 * @author 
 * Saarthak Chandra - sc2776
 * Shweta Shrivastava - ss3646
 * Vikas P Nelamangala - vpn6
 *
 */
public class SelectOperator extends UnaryOperator {	

	Expression selectCondition;
	Tuple currentTuple = null;
	List<String> tupleSchema = null;
	SelectVisitor selVisitor;
	
	/**
	 * Constructor for Select operator
	 * @param child 
	 * 			The Scan operator child object of Select operator 
	 * @param selectCondition 
	 * 			The Expression object which forms the select condition
	 */
	public SelectOperator(ScanOperator child,Expression selectCondition) {
		super(child);
		this.selectCondition = selectCondition;
		this.tupleSchema = child.getSchema();
	}

	/**
	 * Get the next tuple of the table which satisfies the given select condition
	 * Create a select visitor and call the select evaluator function to get the result of the expression
	 * @return The next tuple which satisfies the select condition
	 */
	@Override
	public Tuple getNextTuple() {
		while((currentTuple = child.getNextTuple()) != null){
			selVisitor = new SelectVisitor(currentTuple, tupleSchema);
			if(ExpressionEvaluator.evaluateSelectExpression(selectCondition,selVisitor)){
				return currentTuple;
			}
		}
			return null;
	}
	
}
