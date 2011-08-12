package uk.ac.manchester.cs.snee.compiler.queryplan;

import org.apache.log4j.Logger;

public abstract class SNEEAlgebraicForm {

	/**
	 * Logger for this class.
	 */
	protected Logger logger = Logger.getLogger(SNEEAlgebraicForm.class.getName());
	
	/**
	 * The identifier of this *AF.
	 */
	private String id;

	/**
	 * The name of the query that this algebraic form is a representation of.
	 */
	private String queryName;
	
	/**
	 * Generates a systematic name for this query plan structure, 
	 * of the form
	 * {query-name}-{structure-type}-{counter}.
	 * @param queryName	The name of the query
	 * @return the generated name for the query plan structure
	 */
	public SNEEAlgebraicForm(String queryName) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER SNEEAlgebraicForm() with queryName="+queryName);
		this.queryName = queryName;
		this.id = generateID(queryName);
		logger.trace("*AF id="+this.id);
		if (logger.isDebugEnabled())
			logger.debug("RETURN SNEEAlgebraicForm()");
	}
	
	/**
	 * Generate a systematic name for instance of *AF.
	 * @param queryName
	 * @return
	 */
	protected abstract String generateID(String queryName);
	
	/**
	 * Sets a name for instance of *AF.
	 * @param newID
	 */
	public void setID(String newID) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER setID()");
		this.id = newID;
		if (logger.isDebugEnabled())
			logger.debug("RETURN setID()");
	}
	
	/**
	 * Gets the query name that this *AF is a representation of.
	 * @return
	 */
	public String getQueryName() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getQueryName()");
		if (logger.isDebugEnabled())
			logger.debug("RETURN getQueryName()");
		return this.queryName;
	}
	
	/**
	 * Returns an identifier for the instance of the *AF.
	 * @return
	 */
	public String getID() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getID()");
		if (logger.isDebugEnabled())
			logger.debug("RETURN getID()");
		return this.id;
	}
	
    /**
     * Returns string describing the descendants of this query plan structure.
     */
	public abstract String getDescendantsString();

}
