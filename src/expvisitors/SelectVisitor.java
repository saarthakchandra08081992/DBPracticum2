package expvisitors;

import java.util.List;
import net.sf.jsqlparser.schema.Column;
import models.Tuple;
import helpers.AttributeMapper;

/**
 * This is an ExpressionVisitor for the select condition evaluations
 * The Select Expression Visitor extends the ExpressionVisitorWrapper
 * Every SelectVisitor carries with itself the tuple 
 * and its corresponding schemas 
 * 
 * @author 
 * Saarthak Chandra - sc2776
 * Shweta Shrivastava - ss3646
 * Vikas P Nelamangala - vpn6
 *
 */
 
public class SelectVisitor extends ExpressionVisitorWrapper {

	Tuple t;
	List<String> tSchema;
	
	public SelectVisitor(Tuple t, List<String> tSchema) {
		// TODO Auto-generated constructor stub
		this.t = t;
		this.tSchema = tSchema;

	}
	
	/**
	 * We implement the visit method for Column expression type separately for select and join
	 * as we will need to handle them both separately
	 * Here we map the column name to the actual value from the tuple using the tuple schema
	 * and assign it to the expression value
	 * @param arg0 
	 * 			Column object
	 * 
	 */
	public void visit(Column arg0){
		expressionValue = AttributeMapper.getColumnActualValue(t, tSchema, arg0.toString());
	}

}
