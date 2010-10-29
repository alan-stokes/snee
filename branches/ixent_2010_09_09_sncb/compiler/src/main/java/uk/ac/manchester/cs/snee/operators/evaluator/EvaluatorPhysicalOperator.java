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
import java.util.Iterator;
import java.util.List;
import java.util.Observable;
import java.util.Observer;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.EvaluatorException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.metadata.Metadata;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.operators.logical.AggregationOperator;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.JoinOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.ProjectOperator;
import uk.ac.manchester.cs.snee.operators.logical.RStreamOperator;
import uk.ac.manchester.cs.snee.operators.logical.ReceiveOperator;
import uk.ac.manchester.cs.snee.operators.logical.SelectOperator;
import uk.ac.manchester.cs.snee.operators.logical.UnionOperator;
import uk.ac.manchester.cs.snee.operators.logical.WindowOperator;

public abstract class EvaluatorPhysicalOperator 
extends Observable 
implements Observer 
{

	protected Logger logger = Logger.getLogger(this.getClass().getName());

	private Metadata _schema;
	
	protected EvaluatorPhysicalOperator child;

	private LogicalOperator m_op;

	protected int m_qid;
	
	public static int RECEIVE_TIMEOUT = 50000;
	
	public EvaluatorPhysicalOperator() {
		
	}
	
	public EvaluatorPhysicalOperator(LogicalOperator op, int qid) 
	throws SNEEException, SchemaMetadataException,
	SNEEConfigurationException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER EvaluatorOperator() " + op);
		}
		m_op = op;
		m_qid = qid;
		// Instantiate the child operator
		Iterator<LogicalOperator> iter = op.childOperatorIterator();
		child = getEvaluatorOperator(iter.next());		
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN EvaluatorOperator()");
		}
	}
	
	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.evaluator.operators.EvaluatorOperator#open()
	 */

	public void open() throws EvaluatorException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER open()");
		}
		child.setSchema(getSchema());
		child.open();
		child.addObserver(this);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN open()");
		}
	}
	
	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.evaluator.operators.EvaluatorOperator#close()
	 */
	public void close(){
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER close()");
		}
		child.close();
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN close()");
		}
	}

//	/* (non-Javadoc)
//	 * @see uk.ac.manchester.cs.snee.evaluator.operators.EvaluatorOperator#getNext()
//	 */
//	public abstract Collection<Output> getNext() 
//	throws ReceiveTimeoutException, SNEEException, EndOfResultsException;

	/* (non-Javadoc)
	 * @see java.util.Observer#update(java.util.Observable, java.lang.Object)
	 */
	public abstract void update (Observable obj, Object observed);
	
	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.evaluator.operators.EvaluatorOperator#getSchema()
	 */
	public Metadata getSchema() {
		return _schema;
	}

	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.evaluator.operators.EvaluatorOperator#setSchema(uk.ac.manchester.cs.diasmc.schemas.Schema)
	 */
	public void setSchema(Metadata schema) {
		_schema = schema;
	}
	
	public EvaluatorPhysicalOperator getEvaluatorOperator(
			LogicalOperator op) 
	throws SNEEException, SchemaMetadataException,
	SNEEConfigurationException {
		EvaluatorPhysicalOperator phyOp = null;
		if (op instanceof ReceiveOperator) {
			phyOp = new ReceiveOperatorImpl(op, m_qid);
		} else if (op instanceof DeliverOperator) {
			phyOp = new DeliverOperatorImpl(op, m_qid);
		} else if (op instanceof ProjectOperator) {
			phyOp = new ProjectOperatorImpl(op, m_qid);
		} else if (op instanceof SelectOperator) {
			phyOp = new SelectOperatorImpl(op, m_qid);
		} else if (op instanceof WindowOperator) {
			if (((WindowOperator) op).isTimeScope()) {
				phyOp = new TimeWindowOperatorImpl(op, m_qid);
			} else {
				phyOp = new TupleWindowOperatorImpl(op, m_qid);
			}
			
		} else if (op instanceof RStreamOperator) {
			phyOp = new RStreamOperatorImpl(op, m_qid);
		} else if (op instanceof AggregationOperator) {
			phyOp = new AggregationOperatorImpl(op, m_qid);
		} else if (op instanceof JoinOperator) {
			phyOp = new JoinOperatorImpl(op, m_qid);
		} else if (op instanceof UnionOperator) {
			phyOp = new UnionOperatorImpl(op, m_qid);
		} else {
			String msg = "Unsupported operator " + op.getOperatorName();
			logger.warn(msg);
			throw new SNEEException(msg);
		}
		return phyOp;
	}

	public List<String> getAttributes() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getAttributes() " + m_op);
		List<Attribute> attrList = m_op.getAttributes();
		List<String> attrNames = new ArrayList<String>();
		for (Attribute attr : attrList) {
			String attributeName = 
				attr.getAttributeSchemaName() + "." + 
				attr.getAttributeDisplayName();
			logger.debug("Adding attribute " + attributeName);
			attrNames.add(attributeName);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN getAttributes() #attr=" + attrNames.size());
		return attrNames;
	}

}
