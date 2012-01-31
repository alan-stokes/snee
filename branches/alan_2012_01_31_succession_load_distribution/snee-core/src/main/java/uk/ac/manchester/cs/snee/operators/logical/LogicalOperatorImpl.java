/****************************************************************************\ 
 *                                                                            *
 *  SNEE (Sensor NEtwork Engine)                                              *
 *  http://code.google.com/p/snee                                             *
 *  Release 1.0, 24 May 2009, under New BSD License.                          *
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
package uk.ac.manchester.cs.snee.operators.logical;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.common.graph.NodeImplementation;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.NoPredicate;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;


/**
 * Base class for all logical operators. Specific operators
 * extend this class.
 */
public abstract class LogicalOperatorImpl extends NodeImplementation
implements LogicalOperator {

	/**
   * serialVersionUID
   */
  private static final long serialVersionUID = -765877135458742382L;

  /**
	 * See java.util.logging.Logger.
	 */
	private static final Logger logger = Logger.getLogger(LogicalOperatorImpl.class.getName());

	/**
	 * The name of the operator, e.g., ACQUIRE, JOIN.
	 */
	private String operatorName;
	
	/**
	 * Id to be given to the next Operator to be created.
	 */
	private static int opCount = 0;

	/**
	 * The output data type of the operator,
	 *  e.g., ALLTUPLES, RELATION, STREAM or WINDOW.
	 */
	private OperatorDataType operatorDataType;

	/**
	 * String representation of operator parameters.
	 */
	private String paramStr;

	/** 
	 * Predicate that this operator is expected to test data against.
	 */
	private Expression predicate = new NoPredicate();

	protected AttributeType _boolType;

	/**
	 * Constructs a new operator. 
	 */
	public LogicalOperatorImpl(AttributeType boolType) {
		/* Assign the operator an automatic ID */
		super(getNextID());
		_boolType = boolType;    
	}

	public static String getNextID() {
		opCount++;
		return new Integer(opCount).toString();
	}

	/**
	 * @return The output operator at index 0.
	 */
	public LogicalOperator getParent() {
		return (LogicalOperator) this.getOutput(0);
	}
	
	public void setOperatorName(String newName) {
		this.operatorName = newName;
	}
	
	public String getOperatorName() {
		return this.operatorName;
	}
	
	/**
	 * Constructs a new operator, building tree from leaves upwards.
	 * 
	 * @param children The children of an operator
	 */
	// TODO: Change children to arrayList type?
	protected void setChildren(LogicalOperator[] children) {
		if (logger.isTraceEnabled())
			logger.trace("ENTER setChildren() #children=" + children.length);
		for (LogicalOperator element : children) {
			this.addInput(element);
			element.addOutput(this);
		}

		//		//for now, but probably oversimplifying
		//		if (children.length > 0) {
		//			this.partitioningAttribute = children[0].getPartitioningAttribute();
		//		} else {
		//			throw new AssertionError("Unexpected point reached.");
		//		}
		if (logger.isTraceEnabled())
			logger.trace("ENTER setChildren()");
	}

	//CB: Used by logicalOptimiser
	/**
	 * Returns the input operator at the index
	 * specified.
	 * @param index Position of the operator to be returned.
	 * @return The child operator with this index.
	 */
	public LogicalOperator getInput(int index) {
		return (LogicalOperator) super.getInput(index);
	}

	/** 
	 * Gets the operator to which this operator send data.
	 * @param index Position of the operator to be returned.
	 * @return The parent operator with this index.
	 */
	public LogicalOperator getOutput(int index) {
		return (LogicalOperator) super.getOutput(index);
	}


	/**
	 * Returns a String representation of the operator. This is used
	 * for debugging.
	 * 
	 * @return A string representation of the operator including its children.
	 */
	public abstract String toString();


	/**
	 * Gets a description of this operator only.
	 * CB: Used by TreeDisplay and toString for debugging
	 * 
	 * @return A string representation of this operator but not its children
	 */
	public String getText() {
		StringBuffer s = new StringBuffer();
		s.append("TYPE: ");
		s.append(this.getOperatorDataType());
		s.append("   OPERATOR: ");
		s.append(this.getOperatorName());
		if (this.getParamStr() != null) {
			s.append(" (");
			s.append(this.getParamStr());
			s.append(" )");
		}
		s.append(" - cardinality: ");
		s.append(new Long(this.getCardinality(CardinalityType.MAX)).toString());
//		s.append("\n");
		return s.toString();
	}

	/**
	 * Iterator to traverse the immediate children of the current operator.
	 * @return An iterator over the children
	 */
	public Iterator<LogicalOperator> childOperatorIterator() {

		List<LogicalOperator> opList = new ArrayList<LogicalOperator>();

		for (int n = 0; n < this.getInDegree(); n++) {
			LogicalOperator op = this.getInput(n);
			opList.add(op);
		}

		return opList.iterator();
	}    

	/** {@inheritDoc} */
	public OperatorDataType getOperatorDataType() {
		assert (operatorDataType != null);
		return this.operatorDataType;
	}

	/**
	 * @param newOperatorDataType New value.
	 */
	protected void setOperatorDataType(
			OperatorDataType newOperatorDataType) {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER setOperatorDataType() with " +
					newOperatorDataType);
		}
		assert (newOperatorDataType != null);
		this.operatorDataType = newOperatorDataType;
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN setOperatorDataType()");
		}
	}

	/** {@inheritDoc} */    
	public String getParamStr() {
		return this.paramStr;
	}

	/**
	 * @param newParamStr new Value
	 */
	protected void setParamStr(String newParamStr) {
		this.paramStr = newParamStr;
	}    

	/** {@inheritDoc} 
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException */    
	public String getTupleAttributesStr(int maxPerLine) 
	throws SchemaMetadataException, TypeMappingException {
		List<Attribute> attributes = this.getAttributes();
		return LogicalOperatorImpl.getTupleAttributesStr(attributes, maxPerLine);
	}

	public static String getTupleAttributesStr(List<Attribute> attributes, int maxPerLine) {
		StringBuffer strBuff = new StringBuffer();
		strBuff.append("(");
		for (int i = 0; i < attributes.size(); i++) {
			//commas
			if (i > 0) {
				strBuff.append(", ");
				if ((i % maxPerLine == 0) && (i - 1) < attributes.size()) {  
					strBuff.append("\\n");
				}
			}
			strBuff.append(attributes.get(i).getAttributeDisplayName());
			strBuff.append(":");
			assert (attributes.get(i) != null);
			assert (attributes.get(i).getType() != null);            
			strBuff.append(Utils.capFirstLetter(
					attributes.get(i).getType().getName()));

		}
		strBuff.append(")");
		return strBuff.toString();
	}

	/** 
	 * List of the attribute returned by this operator.
	 * 
	 * @return List of the returned attributes.
	 */
	public List<Attribute> defaultGetAttributes() {
		return (this.getInput(0)).getAttributes();		
	}

	/** {@inheritDoc} */    
	public List<Expression> defaultGetExpressions() {
		List<Expression> expressions = new ArrayList<Expression>(); 
		expressions.addAll(getAttributes());
		return expressions;
	}

	/** {@inheritDoc} */    
	public Expression getPredicate() {
		return predicate;
	}

	/** {@inheritDoc} 
	 * @throws AssertionError 
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException */    
	public void setPredicate(Expression newPredicate) 
	throws SchemaMetadataException, AssertionError, TypeMappingException 
	{
		if (logger.isDebugEnabled())
			logger.debug("ENTER setPredicate() with " + newPredicate);
		if (newPredicate instanceof NoPredicate) {
			if (logger.isTraceEnabled())
				logger.trace("Instance of NoPredicate");
			this.predicate = newPredicate;
			this.paramStr = paramStr + predicate.toString();
			if (logger.isDebugEnabled())
				logger.debug("RETURN setPredicate()");
			return;
		}
		if (logger.isTraceEnabled())
			logger.trace("Predicate expression type: " + 
					newPredicate.getType() + 
					"\n\t\t\tBoolean type: " + _boolType);
		if (newPredicate.getType() != _boolType) {
			String msg = "Illegal attempt to use a none boolean " +
			"expression as a predicate.";
			logger.warn(msg);
			throw new AssertionError(msg);
		}
		if (this.acceptsPredicates()) {
			this.predicate = newPredicate;
			this.paramStr = paramStr + predicate.toString();
		} else {
			String msg = "Illegal call.";
			logger.warn(msg);
			throw new AssertionError(msg);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN setPredicate()");
	}
}

