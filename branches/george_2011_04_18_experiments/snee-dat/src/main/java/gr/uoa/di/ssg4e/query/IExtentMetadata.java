/**
 *
 * Provided by LebLab
 *
 * @author lebiathan
 *
 */
package gr.uoa.di.ssg4e.query;

import gr.uoa.di.ssg4e.dat.schema.DATMetadata;

/**
 * Interface for the Extent Metadata class. Provides access to the information
 * that any extent object carries.
 * 
 */
public interface IExtentMetadata {

	/**
	 * @author George Valkanas
	 * Returns whether this is an intensional extent
	 * 
	 * @return True if the extent is intensional, false otherwise
	 * */
	public boolean isIntensional();

	/**
	 * @author George Valkanas
	 * 
	 * Returns the metadata associated with the Data Analysis Technique
	 * information of this extent.
	 * */
	public DATMetadata getDATMetadata();

}
