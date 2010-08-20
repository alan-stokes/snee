package uk.ac.manchester.cs.snee.compiler.queryplan;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.metadata.schema.Attribute;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;

public class QueryPlanMetadata {

	/**
	 * Logger for this class.
	 */
	private Logger logger = 
		Logger.getLogger(this.getClass().getName());

	private List<Attribute> attributes = new ArrayList<Attribute>();

	public QueryPlanMetadata(
			List<uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute> attrs) 
	throws SchemaMetadataException, TypeMappingException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER QueryPlanMetadata() #attrs=" + 
					attrs.size());
		}
		for (uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute attr : attrs) {
			Attribute attribute = new Attribute(attr.getLocalName(), 
					attr.getAttributeName(), attr.getType());
			attributes.add(attribute);
		}
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
