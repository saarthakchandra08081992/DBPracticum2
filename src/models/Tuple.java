package models;

import java.io.IOException;
import java.io.PrintStream;

/**
 * Tuple class to handle the table tuples
 * TODO: toString() override needed?
 * 
 * @author
 * Saarthak Chandra - sc2776
 * Shweta Shrivastava - ss3646
 * Vikas P Nelamangala - vpn6
 *
 */
public class Tuple {

	public int[] values;

	/**
	 * Constructor for tuple
	 * 
	 * @param tupleValues
	 *            Integer array read from file which represents tuple
	 */
	public Tuple(int[] tupleValues) {
		this.values = tupleValues;
	}

	/**
	 * Get the size of the tuple
	 * 
	 * @return Size of the tuple
	 */
	public int getSize() {
		return values.length;
	}

	/**
	 * Get the value at the specified index
	 * 
	 * @param i
	 *            Index
	 * @return Column value at i
	 */
	public int getValue(int i) {
		return values[i];
	}

	/**
	 * Print the tuple row entry line by line in the print stream.
	 * 
	 * @param printstream
	 *            The print stream
	 */
	public void dump(PrintStream printstream) {
		try {
			String str = toString() + '\n';
			printstream.write(str.getBytes());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * We override the toString method as each column needs to be separated by commas
	 */
	@Override
	public String toString() {
		if (values.length < 1) return "";
		StringBuilder sb = new StringBuilder(String.valueOf(values[0]));
		int i = 1;
		while (i < values.length) {
			sb.append(',');
			sb.append(String.valueOf(values[i++]));
		}
		return sb.toString();
	}

}
