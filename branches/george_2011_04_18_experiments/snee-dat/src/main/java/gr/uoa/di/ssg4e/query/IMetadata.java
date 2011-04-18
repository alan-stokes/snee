/**
 *
 * Provided by LebLab
 *
 * @author lebiathan
 *
 */
package gr.uoa.di.ssg4e.query;

/**
 * Interface for the methods that are required from the data analysis module.
 * Those methods will provide access to the 
 */
public interface IMetadata {

	/**
	 * Retrieve the metadata about a particular extent
	 * */
	public IExtentMetadata getExtentMetadata(String name) throws IException;
}
