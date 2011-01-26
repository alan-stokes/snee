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
package uk.ac.manchester.cs.snee.evaluator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.EvaluatorException;
import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.ResultStore;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.metadata.CostParameters;
import uk.ac.manchester.cs.snee.compiler.metadata.Metadata;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.queryplan.EvaluatorQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.LAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.sncb.SNCB;
import uk.ac.manchester.cs.snee.sncb.SNCBSerialPortReceiver;
import uk.ac.manchester.cs.snee.sncb.tos.CodeGenerationException;

public class Dispatcher {

	private Logger logger = 
		Logger.getLogger(Dispatcher.class.getName());
	
	private Metadata _schema;
	
	private Map<Integer,QueryEvaluator> _queryEvaluators;
	
	public Dispatcher(Metadata schema) {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER Dispatcher()");
		}
		_schema = schema;
		_queryEvaluators = new HashMap<Integer, QueryEvaluator>();
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN Dispatcher()");
		}
	}
	
	protected Set<Integer> getRunningQueryIds() {
		return _queryEvaluators.keySet();
	}
	
	/**
	 * Start the execution of a query.
	 * @param resultSet The storage to be used for query results
	 * @param queryID The identifier of the query
	 * 
	 * @param queryPlan query plan of the query to be evaluated
	 * @throws SNEEException Problem opening the query plan
	 * @throws SchemaMetadataException 
	 * @throws EvaluatorException 
	 * @throws SNEEConfigurationException 
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws IOException 
	 * @throws CodeGenerationException 
	 * @throws OptimizationException 
	 */
	public void startQuery(int queryID, ResultStore resultSet, 
			QueryExecutionPlan queryPlan) 
	throws SNEEException, MetadataException, EvaluatorException,
	SNEEConfigurationException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER queryID " + queryID + " " + queryPlan);
		}
		if (queryPlan instanceof EvaluatorQueryPlan) {
			// Create thread to evaluate query
			/*
			 * Using a method for constructing the query evaluator so that it can be
			 * overridden as a mock object for testing.
			 */
			LAF laf = queryPlan.getLAF(); //TODO: This will be a PAF in future
			try {
				QueryEvaluator queryEvaluator = 
					createQueryEvaluator(queryID, laf, resultSet);
				//		Thread evaluationThread = new Thread(queryEvaluator);
				//		// Start query evaluation
				//		evaluationThread.start();
				//		if (logger.isInfoEnabled()) {
				//			logger.info("Started evaluation of query " + queryID + ".");
				//		}
				//		// Add thread to set of query evaluators
				_queryEvaluators.put(queryID, queryEvaluator);
			} catch (SchemaMetadataException e) {
			logger.warn("Throwing a MetadataException. Cause " + e);
				logger.warn("Throwing a MetadataException. Cause " + e);
				throw new MetadataException(e.getLocalizedMessage());
			}
		} else {
			try {
				SensorNetworkQueryPlan snQueryPlan = (SensorNetworkQueryPlan)queryPlan;
				CostParameters costParams = snQueryPlan.getCostParameters();
				String sep = System.getProperty("file.separator");
				String outputDir = SNEEProperties.getSetting(
						SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR) +
						sep + queryPlan.getQueryName() + sep;
				SNCB sncb = snQueryPlan.getSNCB();
				SNCBSerialPortReceiver mr = sncb.register(snQueryPlan, outputDir, costParams);
				InNetworkQueryEvaluator queryEvaluator = new InNetworkQueryEvaluator(queryID, snQueryPlan, mr, resultSet);
				_queryEvaluators.put(queryID, queryEvaluator);
				sncb.start();
			} catch (Exception e) {
				logger.warn(e.getLocalizedMessage(), e);
				throw new EvaluatorException(e);
			}
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN");
		}
	}

	/**
	 * Using a method for constructing the query evaluator so that it can be
	 * overridden as a mock object for testing.
	 * @param queryId passed in as an argument for the purpose of testing
	 * @param queryPlan
	 * @param resultSet 
	 * @return
	 * @throws SNEEException Invalid query plan
	 * @throws SchemaMetadataException Problem with the schema
	 * @throws EvaluatorException 
	 * @throws SNEEConfigurationException 
	 */
	protected QueryEvaluator createQueryEvaluator(int queryId, 
			LAF queryPlan, ResultStore resultSet) 
	throws SNEEException, SchemaMetadataException,
	EvaluatorException, SNEEConfigurationException {
		QueryEvaluator queryEvaluator = 
			new QueryEvaluator(queryId, queryPlan, _schema, resultSet);
		return queryEvaluator;
	}
	
	public boolean stopQuery(int queryID) throws SNEEException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER with: " + queryID);
		}
		boolean success = false;
		if (_queryEvaluators.containsKey(queryID)) {
			QueryEvaluator queryEvaluator = _queryEvaluators.get(queryID);
			queryEvaluator.stopExecuting();
			if (logger.isInfoEnabled()) {
				logger.info("Stopped evaluation of query " + queryID);
			}
			_queryEvaluators.remove(queryID);
			if (queryEvaluator.hasFailed()) {
				String msg = "Query " + queryID + " evaluation failed. " +
						"See logs for details.";
				logger.warn(msg);
				throw new SNEEException(msg);
			}
			success = true;
		} else {
			logger.warn("Unknown query ID " + queryID + 
					". SNEEException thrown.");
			throw new SNEEException("Unknown query ID " + queryID);
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN with " + success);
		}
		return success;
	}
	
	public void close() {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER");
		}
		if (_queryEvaluators.isEmpty()) {
			logger.info("No queries are running, so nothing to stop.");
		} else {
			for (QueryEvaluator evaluator : _queryEvaluators.values()) {
				if (logger.isInfoEnabled()) {
					logger.info("Stopping query " + evaluator.getQueryId());
				}
				evaluator.stopExecuting();
			}
			_queryEvaluators.clear();
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN");
		}
	}

}
