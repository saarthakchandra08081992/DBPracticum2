package operators;

import models.Tuple;

/**
 * Operator created when query has a DISTINCT 
 * It assumes that the output of child is SORTED therefore query must also have ORDER BY
 * DuplicateEliminationOperator extends the UnaryOperator and has one child
 * the output of which needs to be unique
 * 
 * @author
 * Saarthak Chandra - sc2776
 * Shweta Shrivastava - ss3646
 * Vikas P Nelamangala - vpn6
 *
 */
public class DuplicateEliminationOperator extends UnaryOperator {
	
	Tuple latestTuple,nonDuplicateTuple = null;
	
	/**
	 * Constructor initialization
	 * @param child
	 * 			Child operator
	 */
	public DuplicateEliminationOperator(Operator child) {
		super(child);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Since the input is assumed to be sorted, every tuple is checked against the previous tuple
	 * If they match, its not returned. If they don't return it.
	 * @return Next non-duplicate tuple
	 */
	@Override
	public Tuple getNextTuple() {
		// TODO Auto-generated method stub
		if(latestTuple == null){
			latestTuple = child.getNextTuple();
			return latestTuple;
		}
		else {
			nonDuplicateTuple = null;
			while((nonDuplicateTuple=child.getNextTuple())!=null){
				if(! (nonDuplicateTuple.getValue(0) == (latestTuple.getValue(0)))) 
					{break;}
			}
			latestTuple = nonDuplicateTuple;
			return nonDuplicateTuple;
			} 
		}
	}
