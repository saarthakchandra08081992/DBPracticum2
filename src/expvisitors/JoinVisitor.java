package expvisitors;

import java.util.List;
import net.sf.jsqlparser.schema.Column;
import helpers.AttributeMapper;
import models.Tuple;

/**
 * This is an ExpressionVisitor for the join condition evaluations
 * The Join Expression Visitor extends the ExpressionVisitorWrapper
 * Every JoinVisitor carries with itself the two tuples which are to be joined
 * and their corresponding schemas 
 * 
 * @author 
 * Saarthak Chandra - sc2776
 * Shweta Shrivastava - ss3646
 * Vikas P Nelamangala - vpn6
 *
 */
public class JoinVisitor extends ExpressionVisitorWrapper {

	private Tuple tuple1 = null, tuple2 = null;
	private List<String> schema1 = null, schema2 = null;

	/**
	 * Constructor for JoinVisitor
	 * @param tuple1
	 * 			Tuple 1 for join
	 * @param tuple2
	 * 			Tuple2 for join
	 * @param schema1
	 * 			Schema for Tuple1
	 * @param schema2
	 * 			Schema for Tuple2
	 */
	public JoinVisitor(Tuple tuple1, Tuple tuple2, List<String> schema1, List<String> schema2) {
		// TODO Auto-generated constructor stub
		this.tuple1 = tuple1;
		this.tuple2 = tuple2;
		this.schema1 = schema1;
		this.schema2 = schema2;

	}
	
	/**
	 * Visit function for a column
	 * Visit the column and extract the actual value for the given attribute
	 * @param arg0
	 * 			Column object
	 */
	@Override
	public void visit(Column arg0) {
		Long tempValue;
		expressionValue = ((tempValue = AttributeMapper.getColumnActualValue(tuple1, schema1, arg0.toString())) == null) ? (AttributeMapper.getColumnActualValue(tuple2, schema2, arg0.toString())) : tempValue;
	}
	
}
