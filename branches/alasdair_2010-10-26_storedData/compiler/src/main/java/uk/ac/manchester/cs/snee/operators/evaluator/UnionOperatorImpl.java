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

import java.util.Iterator;
import java.util.List;
import java.util.Observable;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.EvaluatorException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.UnionOperator;

public class UnionOperatorImpl extends EvaluatorPhysicalOperator {
	//XXX: Write test for union operator
	
	Logger logger = Logger.getLogger(UnionOperatorImpl.class.getName());

	UnionOperator unionOp;
	
	int nextIndex = 0;

	private EvaluatorPhysicalOperator leftOperator;

	private EvaluatorPhysicalOperator rightOperator;

	public UnionOperatorImpl(LogicalOperator op) 
	throws SNEEException, SchemaMetadataException,
	SNEEConfigurationException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER UnionOperatorImpl() " + op);
		}

		// Create connections to child operators
		Iterator<LogicalOperator> iter = op.childOperatorIterator();
		leftOperator = getEvaluatorOperator(iter.next());
		rightOperator = getEvaluatorOperator(iter.next());
		
		// Instantiate union operator
		unionOp = (UnionOperator) op;
		
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN UnionOperatorImpl()");
		}
	}

	public void open() throws EvaluatorException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER open()");
		}
		startChildReceiver(leftOperator);
		startChildReceiver(rightOperator);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN open()");
		}
	}

	private void startChildReceiver(EvaluatorPhysicalOperator op) 
	throws EvaluatorException 
	{
		if (logger.isTraceEnabled())
			logger.trace("ENTER startChildReceiver() " + op.toString());
		op.setSchema(getSchema());
		op.open();
		op.addObserver(this);
		if (logger.isTraceEnabled())
			logger.trace("RETURN startChildReceiver()");
	}

	public void close(){
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER close()");
		}
		leftOperator.close();
		rightOperator.close();
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN close()");
		}
	}
//	
//	@Override
//	public Collection<Output> getNext() 
//	throws ReceiveTimeoutException, SNEEException, EndOfResultsException 
//	{
//		if (logger.isDebugEnabled()) {
//			logger.debug("ENTER getNext()");
//		}
////FIXME: Make receive from children separate threads
////FIXME: How do we order when the sources are out of synchronisation?
//		// Get results from child operators
//		Collection<Output> leftStream = leftOperator.getNext();
//		Collection<Output> rightStream = rightOperator.getNext();
//		// Merge streams
//		Collection<Output> result = mergeStreams(leftStream, rightStream);
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
		if (observed instanceof Output || observed instanceof List<?>) {
			setChanged();
			notifyObservers(observed);
		} else {
			logger.warn("Unexpected item type " + observed.getClass());
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN update()");
	}
//
//	private Collection<Output> mergeStreams(Collection<Output> leftStream,
//			Collection<Output> rightStream) {
//		if (logger.isTraceEnabled())
//			logger.trace("ENTER mergeStreams() " +
//					"#left=" + leftStream.size() + " " +
//					"#right=" + rightStream.size());
//		// Create list to hold the result of this evaluation
//		Collection<Output> result = new ArrayList<Output>();
//		Iterator<Output> leftIt = leftStream.iterator();
//		Iterator<Output> rightIt = rightStream.iterator();
//		Output left = getNextItem(leftIt);
//		Output right = getNextItem(rightIt);
//		while (!(left == null && right == null)) {
//			if (logger.isTraceEnabled())
//				logger.trace("\n\tLeft: " + left + "\n\tRight: " + right);
//			if (left == null) {
//				logger.trace("Add right");
//				result.add(right);
//				right = getNextItem(rightIt);
//			} else if (right == null) {
//				logger.trace("Add left");
//				result.add(left);
//				left = getNextItem(leftIt);
//			} else if (left.getEvalTime() <= right.getEvalTime()) {
//				logger.trace("Add left");
//				result.add(left);
//				left = getNextItem(leftIt);
//			} else {
//				logger.trace("Add right");
//				result.add(right);
//				right = getNextItem(rightIt);
//			}
//		} 
//		if (logger.isTraceEnabled())
//			logger.trace("RETURN mergeStreams() size=" + result.size());
//		return result;
//	}
//
//	private Output getNextItem(Iterator<Output> it) {
//		if (logger.isTraceEnabled())
//			logger.trace("ENTER getNextItem() " + it.hasNext());
//		Output item = null;
//		if (it.hasNext())
//			item = it.next();
//		if (logger.isTraceEnabled())
//			logger.trace("RETURN getNextItem() " + item );
//		return item;
//	}

}
