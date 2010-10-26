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
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public class DeliverOperatorImpl 
extends EvaluatorPhysicalOperator {
	//XXX: Write test for deliver operator
	
	Logger logger =
		Logger.getLogger(DeliverOperatorImpl.class.getName());

	DeliverOperator deliverOp;
	
	int nextIndex = 0;

	public DeliverOperatorImpl(LogicalOperator op) 
	throws SNEEException, SchemaMetadataException,
	SNEEConfigurationException {
		super(op);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER DeliverOperatorImpl() " + op);
			logger.debug("Attribute List: " + op.getAttributes());
			logger.debug("Expression List: " + op.getExpressions());
		}

		// Instantiate deliver operator
		deliverOp = (DeliverOperator) op;
		
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN DeliverOperatorImpl()");
		}
	}

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
//			item.setIndex(nextIndex);
//			nextIndex++;
//			result.add(item);
//		}
//		
//		if (logger.isDebugEnabled()) {
//			logger.debug("RETURN getNext() number of items: " + 
//					result.size());
//		}
//		return result;
//	}

	@Override
	public void update(Observable obj, Object observed) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER update() with " + observed);
		List<Output> resultList = new ArrayList<Output>();
		if (observed instanceof List<?>) {
			List<Output> outputList = (List<Output>) observed;
			for (Output output : outputList) {
				resultList.add(deliverResult(output));
			}
		} else if (observed instanceof Output) {
			resultList.add(deliverResult((Output) observed));
		}
		if (!resultList.isEmpty()) {
			setChanged();
			notifyObservers(resultList);
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN update()");
		}
	}

	private Output deliverResult(Output observed) {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER deliverResult() with " + observed);
		}
		observed.setIndex(nextIndex);
		nextIndex++;
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN deliverResult() with " + observed);
		}
		return observed;
	}

}
