package uk.ac.manchester.cs.snee.compiler.queryplan;

import java.util.TreeMap;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;

/**
 * Utility class for displaying PAF.
 */
public class PAFUtils extends DLAFUtils {

	/**
	 * Logger for this class.
	 */
	private Logger logger = Logger.getLogger(PAFUtils.class.getName());
	
	/**
	 * PAF to be displayed.
	 */
	private PAF paf;
	
	/**
	 * Constructor for LAFUtils.
	 * @param laf
	 */	
	public PAFUtils(PAF paf) {
		super(paf.getDLAF());
		if (logger.isDebugEnabled())
			logger.debug("ENTER PAFUtils()"); 
		this.paf = paf;
		this.name = paf.getID();
		this.tree = paf.getOperatorTree();
		if (logger.isDebugEnabled())
			logger.debug("RETURN PAFUtils()"); 
	}
}
