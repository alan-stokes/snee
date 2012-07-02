package uk.ac.manchester.cs.snee.compiler.params;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.common.UtilsException;
import uk.ac.manchester.cs.snee.compiler.params.qos.QoSException;
import uk.ac.manchester.cs.snee.compiler.params.qos.QoSExpectations;
import uk.ac.manchester.cs.snee.compiler.params.qos.QoSExpectationsReader;

public class QueryParameters {

	/**
     * Logger for this class.
     */
    private static final Logger logger = Logger.getLogger(QueryParameters.class.getName());

	
	QoSExpectations _qos;

	int _queryID;
	
	public QueryParameters(int queryID, String queryParamsFile) 
	throws QoSException, UtilsException {
		if (logger.isDebugEnabled()) {
            logger.trace("ENTER QueryParameters() with queryID=" +
                            queryID + " queryParamsFile=" + queryParamsFile);
		}
		this._queryID = queryID;
		String validatedQueryParamsFilename = 
			Utils.validateFileLocation(queryParamsFile);
		logger.info("Reading QoS expectations from "+validatedQueryParamsFilename);
		this._qos = new QoSExpectationsReader(queryID, validatedQueryParamsFilename);
		if (logger.isDebugEnabled()) {
            logger.trace("RETURN QueryParameters()");
		}
	}
	
	/**
	 * @return the _qos
	 */
	public QoSExpectations getQoS() {
		return _qos;
	}	
	
}
