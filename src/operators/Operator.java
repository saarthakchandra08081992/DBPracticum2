package operators;

import java.io.PrintStream;
import java.util.List;
import models.Tuple;

/**
 * Base Abstract class for all Operators
 * 
 * @author
 * Saarthak Chandra - sc2776
 * Shweta Shrivastava - ss3646
 * Vikas P Nelamangala - vpn6
 * 
 */
public abstract class Operator {
	public abstract Tuple getNextTuple();	
	public abstract List<String> getSchema();
	public abstract void reset();
	protected List<String> schema = null;
	
	/**
	 * Print Every table row
	 * @param ps The PrintStream object
	 */
	public void dump(PrintStream ps) {
		Tuple tuple = null;
		while ((tuple = getNextTuple()) != null) {
			tuple.dump(ps);
		}
	}
	
}
