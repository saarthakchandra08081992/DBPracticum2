package operators;

import java.util.ArrayList;
import java.util.List;

/**
 * BinaryOperator abstract class for operators which will have two children
 * Currently only Join implements this
 * 
 * @author
 * Saarthak Chandra - sc2776
 * Shweta Shrivastava - ss3646
 * Vikas P Nelamangala - vpn6
 *
 */
public abstract class BinaryOperator extends Operator {

	public Operator leftchild;
	public Operator rightchild;
	
	/**
	 * Constructor for Binary Operator
	 * Also add schemas of both children in the corresponding schema object
	 * @param leftchild
	 * 				Operator which is the left child
	 * @param rightchild
	 * 				Operator which is the right child
	 */
	public BinaryOperator(Operator leftchild, Operator rightchild){
		this.leftchild = leftchild;
		this.rightchild = rightchild;
		addSchemas();
	}
	
	/**
	 * Method called by the constructor to add schemas of children to schema object
	 */
	public void addSchemas() {
		schema = new ArrayList<String>(leftchild.getSchema());
		schema.addAll(rightchild.getSchema());
	}

	/**
	 * Method to reset left and right child read heads
	 */
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		leftchild.reset();
		rightchild.reset();
	}

	/**
	 * Get the schema for the operator
	 * @return Schema object
	 */
	public List<String> getSchema() {
		return schema;
	}
}
