/**
 * 
 */
package uk.ac.manchester.cs.snee.common;

/**
 * @author Praveen
 * 
 */
public enum TupleDropPolicy {
	/** THe tuples that are inserted first in the buffer are removed first */
	FIFO("First in First Out Policy"),
	/** The tuples that are inserted last are removed first */
	LIFO("Last in First Out Policy"),
	/** The duplicate tuples are removed to make space for new tuples */
	REM_DUP("Remove Duplicates Policy");

	private String tupleDropPolicy;

	private TupleDropPolicy(String policy) {
		this.tupleDropPolicy = policy;
	}

	public String toString() {
		return this.tupleDropPolicy;
	}
}
