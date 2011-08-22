/****************************************************************************\
 *                                                                            *
 *  SNEE (Sensor NEtwork Engine)                                              *
 *  http://snee.cs.manchester.ac.uk/                                          *
 *  http://code.google.com/p/snee                                             *
 *                                                                            *
 *  Release 1.x, 2009, under New BSD License.                                 *
 *                                                                            *
 *  Copyright (c) 2009, University of Manchester                              *
 *  All rights reserved.                                                      *
 *                                                                            *
 *  Redistribution and use in source and binary forms, with or without        *
 *  modification, are permitted provided that the following conditions are    *
 *  met: Redistributions of source code must retain the above copyright       *
 *  notice, this list of conditions and the following disclaimer.             *
 *  Redistributions in binary form must reproduce the above copyright notice, *
 *  this list of conditions and the following disclaimer in the documentation *
 *  and/or other materials provided with the distribution.                    *
 *  Neither the name of the University of Manchester nor the names of its     *
 *  contributors may be used to endorse or promote products derived from this *
 *  software without specific prior written permission.                       *
 *                                                                            *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS   *
 *  IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, *
 *  THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR    *
 *  PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR          *
 *  CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,     *
 *  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,       *
 *  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR        *
 *  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF    *
 *  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING      *
 *  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS        *
 *  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.              *
 *                                                                            *
\****************************************************************************/
package uk.ac.manchester.cs.snee.operators.evaluator;

import java.util.List;
import java.util.Observable;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.EvaluatorException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.FalseAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.FloatLiteral;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.IntLiteral;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.NoPredicate;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.StringLiteral;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.TrueAttribute;
import uk.ac.manchester.cs.snee.datasource.jdbc.JDBCSourceWrapperImpl;
import uk.ac.manchester.cs.snee.datasource.webservice.StoredSourceWrapperAbstract;
import uk.ac.manchester.cs.snee.datasource.webservice.WSDAIRSourceWrapperImpl;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.evaluator.types.Window;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentDoesNotExistException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;
import uk.ac.manchester.cs.snee.metadata.source.WebServiceSourceMetadata;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.ScanOperator;
import uk.ac.manchester.cs.snee.types.Duration;

public class ScanOperatorImpl extends EvaluatorPhysicalOperator {

	Logger logger = 
		Logger.getLogger(ScanOperatorImpl.class.getName());

	/**
	 * Stores the local extent name.
	 */
	private String extentName;

	/**
	 * Attributes that are to be returned by the scan operator
	 */
	List<Attribute> attributes ;

	/**
	 * The logical representation of the scan operator.
	 * Contains useful metadata
	 */
	private ScanOperator scanOp;

	/**
	 * String representing the SQL query to be posed to the stored data source
	 */
	private String sqlQuery;

	/**
	 * Duration between rescans.
	 * Note: Value is null if there is not a rescan
	 */
	private Duration rescanInterval;
	
	/**
	 * Timer that controls the execution of the rescans
	 */
	private Timer _rescanTaskTimer;

	/**
	 * Wrapper for interacting with the external data source
	 */
	private StoredSourceWrapperAbstract _sourceClient;

	/**
	 * Name of the resource on the external service to retrieve data from
	 */
	private String _resourceName;

	/**
	 * The query expression that should be passed down to the database source
	 */
	private Expression expression;
	
	/**
	 * Instantiates the scan operator
	 * @param op
	 * @throws SchemaMetadataException 
	 * @throws EvaluatorException 
	 * @throws SNEEConfigurationException 
	 */
	public ScanOperatorImpl(LogicalOperator op, int qid) 
	throws SchemaMetadataException, EvaluatorException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER ScanOperatorImpl() for query " + 
					qid + " " +
					" with " + op);
		}
		// Instantiate this as a scan operator
		scanOp = (ScanOperator) op;
		m_qid = qid;
		attributes = scanOp.getInputAttributes();
		extentName = scanOp.getExtentName();
		expression = scanOp.getPredicate();
		rescanInterval = scanOp.getRescanInterval();
		// Construct the SQL query to retrieve the data from the source
		sqlQuery = constructQuery();
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN ScanOperatorImpl() SQL query " +
					sqlQuery);
		}
	}

	/**
	 * Constructs the SQL query to pose to the external data source
	 * to retrieve the data.
	 * 
	 * @return String representation of the SQL query
	 * @throws EvaluatorException 
	 */
	protected String constructQuery() throws EvaluatorException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER constructQuery()");
		}
		StringBuffer queryBuffer = new StringBuffer();
		queryBuffer.append("SELECT ");
		for (Attribute attr : attributes) {
			queryBuffer.append(attr.getAttributeSchemaName()).append(", ");
		}
		queryBuffer.replace(queryBuffer.length() - 2, queryBuffer.length(), " ");
		queryBuffer.append("FROM ").append(extentName);
		if (expression != null && !(expression instanceof NoPredicate)) {
			queryBuffer.append(" WHERE ");
			queryBuffer.append(translateExpression(expression));
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN constructQuery() with " + 
					queryBuffer.toString());
		}		
		return queryBuffer.toString();
	}

	/**
	 * 
	 * @return 
	 * @throws EvaluatorException 
	 */
	private String translateExpression(Expression expr) throws EvaluatorException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER translateExprssion() with " + expr);
		}
		StringBuffer queryBuffer = new StringBuffer();
		if (expr instanceof MultiExpression) {
			queryBuffer.append(translateMultiExpression((MultiExpression) expr));
		} else if (expr instanceof Attribute) {
			Attribute attr = (Attribute) expr;
			queryBuffer.append(attr.getAttributeSchemaName());
		} else if (expr instanceof FalseAttribute) {
			FalseAttribute falseAttr = (FalseAttribute) expr;
			queryBuffer.append(falseAttr.getValue());
		} else if (expr instanceof FloatLiteral) {
			FloatLiteral floatLit = (FloatLiteral) expr;
			queryBuffer.append(floatLit.getValue());
		} else if (expr instanceof IntLiteral) {
			IntLiteral intLit = (IntLiteral) expr;
			queryBuffer.append(intLit.getValue());
		} else if (expr instanceof StringLiteral) {
			StringLiteral stringLit = (StringLiteral) expr;
			queryBuffer.append("'").append(stringLit.getValue()).append("'");
		} else if (expr instanceof TrueAttribute) {
			TrueAttribute trueAttr = (TrueAttribute) expr;
			queryBuffer.append(trueAttr.getValue());
		} else {
			String message = "Unknown expression " + expr;
			logger.warn(message);
			throw new EvaluatorException(message);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN translateExprssion() with " + queryBuffer.toString());
		}		
		return queryBuffer.toString();		
	}
	
	private String translateMultiExpression(MultiExpression mExpr) 
	throws EvaluatorException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER translateMultiExpression() with " + mExpr);
		}		
		StringBuffer queryBuffer = new StringBuffer();
		Expression[] expressions = mExpr.getExpressions();
		queryBuffer.append(translateExpression(expressions[0]));
		switch (mExpr.getMultiType()) {
		case ADD:
			queryBuffer.append(" + ");
			break;
		case AND:
			queryBuffer.append(" AND ");
			break;
		case DIVIDE:
			queryBuffer.append(" / ");
			break;
		case EQUALS:
			queryBuffer.append(" = ");
			break;
		case GREATERTHAN:
			queryBuffer.append(" > ");
			break;
		case GREATERTHANEQUALS:
			queryBuffer.append(" >= ");
			break;
		case LESSTHAN:
			queryBuffer.append(" < ");
			break;
		case LESSTHANEQUALS:
			queryBuffer.append(" <= ");
			break;
		case MINUS:
			queryBuffer.append(" - ");
			break;
		case MOD:
			queryBuffer.append(" % ");
			break;
		case MULTIPLY:
			queryBuffer.append(" * ");
			break;
		case NOTEQUALS:
			queryBuffer.append(" != ");
			break;
		case OR:
			queryBuffer.append(" OR ");
			break;				
		case POWER:
		case SQUAREROOT:
			String message = "Unsupported function in WHERE clause " + mExpr.getMultiType();
			logger.warn(message);
			throw new EvaluatorException(message);
		default:
			message = "Unknown function " + mExpr.getMultiType();
			logger.warn(message);
			throw new EvaluatorException(message);
		}
		queryBuffer.append(translateExpression(expressions[1]));
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN translateMultiExpression() with " + queryBuffer.toString());
		}		
		return queryBuffer.toString();		
	}

	@Override
	public void open() throws EvaluatorException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER open()");
		}
		try {
			initialiseStoredSourceClient();
		} catch (SourceMetadataException e) {
			logger.error("SourceMetadataException", e);
			throw new EvaluatorException(e);
		} catch (ExtentDoesNotExistException e) {
			logger.error("ExtentDoesNotExistException", e);
			throw new EvaluatorException(e);
		}
			RescanTask task = new RescanTask();
			if (rescanInterval == null) {
				// Execute query once
				task.run();
			} else {
				// Setup task to periodically rescan
				_rescanTaskTimer = new Timer();
				_rescanTaskTimer.schedule(task, 0, rescanInterval.getDuration());
			}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN open()");
		}
	}

	/**
	 * Ensure that the appropriate kind of wrapper is used to contact the 
	 * external data source.
	 * 
	 * @throws SourceMetadataException
	 * @throws ExtentDoesNotExistException
	 */
	private void initialiseStoredSourceClient() 
	throws SourceMetadataException, ExtentDoesNotExistException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER initialiseStoredSourceClient()");
		}
		SourceMetadataAbstract source = scanOp.getSource();
		SourceType sourceType = source.getSourceType();
		switch (sourceType) {
		case RELATIONAL:
			instantiateJDBCDataSource(source);
			break;
		case WSDAIR:
			instantiateWSDAIRDataSource(source);
			break;
		default:
			String msg = "Data source type " + sourceType +
			" unsupported by SCAN operator";
			logger.error(msg);
			throw new SourceMetadataException(msg);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN initialiseStoredSourceClient()");
		}		
	}

	/**
	 * Set parameters for interacting with a JDBC source wrapper.
	 * 
	 * @param source
	 * @throws ExtentDoesNotExistException
	 */
	private void instantiateJDBCDataSource(SourceMetadataAbstract source) 
	throws ExtentDoesNotExistException 
	{
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER instantiateJDBCDataSource() with " + 
					source.getSourceName());
		}
		WebServiceSourceMetadata webSource = (WebServiceSourceMetadata) source; 
		_sourceClient = (JDBCSourceWrapperImpl) webSource.getSource();
		_resourceName = webSource.getResourceName(extentName);
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN instantiateJDBCDataSource()");
		}
	}

	/**
	 * Set parameters for interacting with a WSDAIR source wrapper.
	 * 
	 * @param source
	 * @throws ExtentDoesNotExistException
	 */
	private void instantiateWSDAIRDataSource(SourceMetadataAbstract source) 
	throws ExtentDoesNotExistException 
	{
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER instantiateWSDAIRDataSource() with " + 
					source.getSourceName());
		}
		WebServiceSourceMetadata webSource = 
			(WebServiceSourceMetadata) source;
		_sourceClient = (WSDAIRSourceWrapperImpl) webSource.getSource();
		_resourceName = webSource.getResourceName(extentName);
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN instantiateWSDAIRDataSource()");
		}
	}

	@Override
	public void close() {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER close()");
		}
		_rescanTaskTimer.cancel();
		_rescanTaskTimer.purge();
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN close()");
		}
	}

	/**
	 * Task that poses the query to the external data source and returns
	 * a window of tuples.
	 * 
	 * Task can be scheduled to repeat at a desired rate, i.e. the rescan rate.
	 *
	 */
	class RescanTask extends TimerTask {

		public void run() {
			if (logger.isDebugEnabled()) {
				logger.debug("ENTER run() for query " + m_qid);
			}
			try {
				List<Tuple> tuples = 
					_sourceClient.executeQuery(_resourceName, sqlQuery);
				Window window = new Window(tuples);
				setChanged();
				notifyObservers(window);
			} catch (SNEEDataSourceException e) {
				logger.warn("Received a SNEEDataSourceException.", e);
				close();
			} catch (TypeMappingException e) {
				logger.warn("Received a TypeMappingException.", e);
				close();
			} catch (SchemaMetadataException e) {
				logger.warn("Received a SchemaMetadataException.", e);
				close();
			} catch (SNEEException e) {
				logger.warn("Received a SNEEException.", e);
				close();
			}
			if (logger.isDebugEnabled()) {
				logger.debug("RETURN run() time to next execution: " + 
						rescanInterval + " seconds.");
			}
		}		
	}

	@Override
	public void update(Observable obj, Object observed) {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER update() for query " + m_qid);
		}
		logger.error("Receiver cannot be the parent of another operator");
		if (logger.isDebugEnabled())
			logger.debug("RETURN update()");
	}
	
}
