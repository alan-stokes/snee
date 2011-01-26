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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Observable;
import java.util.Observer;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.EvaluatorException;
import uk.ac.manchester.cs.snee.ResultStore;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.metadata.Metadata;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.queryplan.LAF;
import uk.ac.manchester.cs.snee.evaluator.types.EvaluatorAttribute;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.evaluator.types.TaggedTuple;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.operators.evaluator.DeliverOperatorImpl;
import uk.ac.manchester.cs.snee.operators.evaluator.EvaluatorPhysicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public class QueryEvaluator implements Observer {
	
	protected boolean executing = false;
	
	protected boolean isExecuting() {
		return executing;
	}

	/**
	 * The identifier for the query
	 */
	protected int _queryId;
	
	protected ResultStore _results;

	/**
	 * The query plan to be evaluated
	 */
	private LAF _queryPlan;

	private Metadata _schema;

	private EvaluatorPhysicalOperator _rootOper;

	private Logger logger = 
		Logger.getLogger(QueryEvaluator.class.getName());

	private boolean failure = false;

	protected QueryEvaluator() {
		// Constructor for mock object test purposes
	}
	
	public QueryEvaluator(int queryId, LAF queryPlan, 
			Metadata schema, 
			ResultStore resultSet) 
	throws SNEEException, SchemaMetadataException, 
	EvaluatorException, SNEEConfigurationException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER QueryEvaluator() with queryID: " + 
					queryId + " " + 
					queryPlan.toString());
		}
		_queryId = queryId;
		_queryPlan = queryPlan;
		_schema = schema;
		_results = resultSet;
		openQueryPlan();
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN QueryEvaluator()");
		}
	}

	public int getQueryId() {
		return _queryId;
	}

	public void stopExecuting() {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER stopExecuting() on query " + _queryId);
		}
		// Protected to prevent null pointer exception
		if (_rootOper != null && executing) {
			// Stop the query evaluation
			_rootOper.close();
		}
		executing = false;	
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN stopExecuting()");
		}
	}

	public boolean hasFailed() {
		return failure;
	}
	
//	public void run() {
//		if (logger.isDebugEnabled()) {
//			logger.debug("ENTER run()");
//		}
//		executing = true;
//		// Execute query
//		while (executing) {
//			try {
//				Collection<Output> output = _rootOper.getNext();
//				_results.addAll(output);
//			} catch (ReceiveTimeoutException e) {
//				//TODO: When does a query time out?
//				logger.warn("Query execution timed out.");
//				if (logger.isInfoEnabled()) {
//					logger.info("Stopped query execution for query " + 
//							_queryId + ".");
//				}
//				stopExecuting();
//			} catch (SNEEException e) {
//				logger.warn("Problem get next stream item. Stopping execution.");
//				if (logger.isInfoEnabled()) {
//					logger.info("Stopped query execution for query " + 
//							_queryId + " due to some problem.");
//				}
//				failure = true;
//				stopExecuting();
//			} catch (EndOfResultsException e) {
//				executing=false;
//			}
//			logger.trace("Executing: " + executing);
//		}
//
//		if (logger.isDebugEnabled()) {
//			logger.debug("RETURN run()");
//		}
//	}

	private void openQueryPlan() 
	throws SNEEException, SchemaMetadataException, EvaluatorException,
	SNEEConfigurationException {
		if (logger.isDebugEnabled())
			logger.trace("ENTER openQueryPlan()");
		// Condition tested to allow for testing with a null plan
		if (_queryPlan != null) {
			// Get the root operator of the PAF
			_rootOper = getInstance(_queryPlan.getRootOperator());
			_rootOper.setSchema(_schema);
			_rootOper.open();
			_rootOper.addObserver(this);
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN openQueryPlan()");
	}

	private EvaluatorPhysicalOperator getInstance(LogicalOperator op) 
	throws SNEEException, SchemaMetadataException,
	SNEEConfigurationException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER getInstance() with " + op);
		EvaluatorPhysicalOperator phyOp = null;
		/* Query plans must have a deliver operator at their root */
		if (op instanceof DeliverOperator) {
			phyOp = new DeliverOperatorImpl(op, _queryId);
		} else {
			String msg = "Unsupported operator " + op.getOperatorName() +
				". Query plans should have a DeliverOperator as their root.";
			logger.warn(msg);
			throw new SNEEException(msg);
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN getInstance() with " + phyOp.getClass());
		return phyOp;
	}

	boolean firstTime = true;
	long currentEvalTime = -1;
	long prevEvalTime = -2;
	int currentEvalTimeCount = 0;
	
	private void logTupleCount(Output outputItem) {
		String sep = System.getProperty("file.separator");
		String outputDir = "/";
		try {
			outputDir = SNEEProperties.getSetting(
					SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR) +
					"/";
			String fname = outputDir + sep + "tupleCount.csv";
	
			if (firstTime) {
				//create file, add header
				PrintWriter out = new PrintWriter(new BufferedWriter(
						new FileWriter(fname)));
				out.println("time,numTuples");
				out.close();
				firstTime = false;
			}
			TaggedTuple tt = (TaggedTuple)outputItem;
			Tuple t = tt.getTuple();
			currentEvalTime = (Integer) t.getAttributeValue(0);
			if (currentEvalTime==prevEvalTime) {
				currentEvalTimeCount++;
			} else {
				//record currentEvalTime,currentEvalTimeCount
				PrintWriter out = new PrintWriter(new BufferedWriter(
						new FileWriter(fname, true)));
				out.println(currentEvalTime+","+currentEvalTimeCount);
				out.close();
				currentEvalTimeCount=1;
				prevEvalTime=currentEvalTime;
			}
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void logTupleCount(Collection<Output> outputItemList) {
		Iterator<Output> iter = outputItemList.iterator();
		while (iter.hasNext()) {
			logTupleCount(iter.next());
		}
	}
	
	@Override
	public void update(Observable obj, Object observed) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER update() for query " + _queryId + " " +
					" with " + observed);
		if (observed instanceof Output) {
			Output outputItem = (Output) observed;
			logTupleCount(outputItem);
			_results.add(outputItem);
		} else if (observed instanceof List<?>) {
			Collection<Output> outputItems = (Collection<Output>) observed;
			logTupleCount(outputItems);
			_results.addAll(outputItems);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN update()");
	}

}
