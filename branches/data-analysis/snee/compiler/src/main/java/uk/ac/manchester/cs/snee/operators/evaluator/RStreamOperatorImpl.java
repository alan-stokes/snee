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

import java.util.ArrayList;
import java.util.List;
import java.util.Observable;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.evaluator.types.TaggedTuple;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.evaluator.types.Window;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.RStreamOperator;

public class RStreamOperatorImpl extends EvaluatorPhysicalOperator {
	//XXX: Write test for rstream operator
	
	Logger logger = Logger.getLogger(RStreamOperatorImpl.class.getName());

	RStreamOperator rstreamOp;

	public RStreamOperatorImpl(LogicalOperator op, int qid) 
	throws SNEEException, SchemaMetadataException,
	SNEEConfigurationException {
		super(op, qid);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER RStreamOperatorImpl() " + op);
		}

		// Instantiate deliver operator
		rstreamOp = (RStreamOperator) op;
		
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN RStreamOperatorImpl()");
		}
	}
//
//	@Override
//	public Collection<Output> getNext() 
//	throws ReceiveTimeoutException, SNEEException, EndOfResultsException {
//		if (logger.isDebugEnabled()) {
//			logger.debug("ENTER getNext()");
//		}
//		// Create list to hold the result of this evaluation
//		Collection<Output> result = new ArrayList<Output>();
//		
//		// Get the next bag of stream items from the child operator
//		Collection<Output> bagOfItems = child.getNext();
//
//		for (Output item : bagOfItems) {
//			if (item instanceof Window) {
//				Window window = (Window)item;
//				// Stream all tuples out of the window
//				for (Tuple tuple : window.getTuples()) {
//					if (logger.isTraceEnabled()) {
//						logger.trace("Stream tuple: " + tuple);
//					}
//					//XXX-AG: Imposing a new tick on the tuple 
//					result.add(new TaggedTuple(tuple));
//				}
//
//			} else {
//				String msg = "Item type (" + item.getClass().getSimpleName() +
//					" is not supported or unknown.";
//				logger.warn(msg);
//				throw new SNEEException(msg);
//			}
//		}
//		if (logger.isDebugEnabled()) {
//			logger.debug("RETURN getNext() number of tuples " + bagOfItems.size());
//		}
//		return result;
//	}

	@Override
	public void update(Observable obj, Object observed) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER update() for query " + m_qid + " " +
					" with " + observed);
		List<Output> result = new ArrayList<Output>();
		if (observed instanceof List<?>) {
			for (Object ob : (List) observed) 
				processOutput(ob, result);
		} else if (observed instanceof Window) {
			processOutput(observed, result);
		} else {
			String msg = "Item type (" + observed.getClass().getSimpleName() +
				" is not supported or unknown.";
			logger.warn(msg);
//			throw new SNEEException(msg);
		}
		if (!result.isEmpty()) {
			setChanged();
			notifyObservers(result);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN update()");
	}

	private void processOutput(Object observed, 
			List<Output> result) {
		if (logger.isTraceEnabled())
			logger.trace("ENTER processOutput() with " + observed);
		Window window = (Window)observed;
		// Stream all tuples out of the window
		for (Tuple tuple : window.getTuples()) {
			if (logger.isTraceEnabled()) {
				logger.trace("Stream tuple: " + tuple);
			}
			//XXX-AG: Imposing a new tick on the tuple 
			result.add(new TaggedTuple(tuple));
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN processOutput()");
	}

}
