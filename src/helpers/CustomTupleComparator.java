package helpers;

import java.util.Comparator;
import java.util.List;
import models.Tuple;

/**
 * TODO: whoever has written this add comments
 * 
 * @author 
 * Saarthak Chandra - sc2776
 * Shweta Shrivastava - ss3646
 * Vikas P Nelamangala - vpn6
 */

public class CustomTupleComparator implements Comparator<Tuple> {
	List<Integer> orders = null;

	/*
	 * tupleValue1 can be 200,2,200 tupleValue2 can be 50,6,201
	 */
	@Override
	public int compare(Tuple t1, Tuple t2) {

		for (int i : orders) {
			if (t1.values[i] < t2.values[i]) {
				return -1;
			}
			if (t1.values[i] > t2.values[i]) {
				return 1;
			}
			// Just continue if the tuples match for the given order...
		}

		// Vikas check this.
		boolean isPresent = false;
		for (int i = 0; i < t1.values.length; i++) {
			for (int j = 0; j < orders.size(); j++) {
				if (orders.get(j) == i) {
					isPresent = true;
				}
			}
			if (isPresent == false) {
				if (t1.values[i] < t2.values[i])
					return -1;
				if (t1.values[i] > t2.values[i])
					return 1;
			}
		}

		return 0;
	}

	public CustomTupleComparator(List<Integer> orders) {
		this.orders = orders;
	}
}
