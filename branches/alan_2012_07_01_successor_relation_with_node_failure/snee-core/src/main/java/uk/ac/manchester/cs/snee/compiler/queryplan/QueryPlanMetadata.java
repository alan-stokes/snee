package uk.ac.manchester.cs.snee.compiler.queryplan;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;

public class QueryPlanMetadata implements Serializable{

	/**
   * serialVersionUID
   */
  private static final long serialVersionUID = 5842446410837971953L;

  /**
	 * Logger for this class.
	 */
	private static final Logger logger = Logger.getLogger(QueryPlanMetadata.class.getName());

	private List<Attribute> attributes = 
		new ArrayList<Attribute>();

	public QueryPlanMetadata(List<Attribute> attrs) 
	throws SchemaMetadataException, TypeMappingException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER QueryPlanMetadata() #attrs=" + 
					attrs.size());
		}
		attributes = attrs;
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN QueryPlanMetadata()");
		}
	}

	/**
	 * Return the attributes that are output by this query.
	 * 
	 * @return a list containing details of the attributes returned by this query
	 */
	public List<Attribute> getOutputAttributes() {
		return attributes;
	}

}
