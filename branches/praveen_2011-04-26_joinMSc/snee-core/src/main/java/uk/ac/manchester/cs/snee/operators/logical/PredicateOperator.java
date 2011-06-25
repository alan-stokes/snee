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
import java.util.List;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;


/**
 * Parent for Project, Aggregation and Having Operator.
 */
public abstract class PredicateOperator extends LogicalOperatorImpl {

	/**
	 * The expressions for building the attributes.
	 */
	private List <Expression> expressions;

	/**
	 * The names of the outgoing attributes.
	 */
	private List <Attribute> attributes;

	protected PredicateOperator (List<Expression> expressions2, 
			List<Attribute> attributes2, LogicalOperator inputOperator,
			AttributeType boolType) {
		super(boolType);
		this.expressions = expressions2;
		this.attributes = attributes2; 
		setChildren(new LogicalOperator[] {inputOperator});
		this.setOperatorDataType(inputOperator.getOperatorDataType());
		this.setOperatorSourceType(inputOperator.getOperatorSourceType());
		this.setSourceRate(inputOperator.getSourceRate());
	}

	/**
	 * Makes a copy of the operator using a new opCount.
	 * @param model Operator to get internal data from.
	 * @param newID boolean flag expected to be true. 
	 */
	protected PredicateOperator(PredicateOperator model, 
			boolean newID) {
		super(model, newID);
		this.expressions = model.expressions;
		this.attributes = model.attributes;
	}

	/**
	 * Constructor that creates a new operator
	 * based on a information from a model.
	 *
	 * @param newExpressions expressions from model.
	 * @param newAttributes attributes from model.
	 * @param operatorDataType data type from model.
	 * @param paramStr parameter string from model.
	 */
	protected PredicateOperator(List<Expression> newExpressions, 
			List<Attribute> newAttributes, 
			OperatorDataType operatorDataType,
			String paramStr, AttributeType boolType) {
		super(boolType);
		this.expressions = newExpressions;
		this.attributes = newAttributes;
	}

	/** 
	 * List of the attribute returned by this operator.
	 * 
	 * @return List of the returned attributes.
	 */ 
	public List<Attribute> getAttributes() {
		return attributes;
	}

    /** {@inheritDoc} */
	public boolean isLocationSensitive() {
		return false;
	}

	/** {@inheritDoc}
	 * @return false;
	 */
	public boolean acceptsPredicates() {
		return false;
	}

	/**
	 * @return a string representation of this function.
	 */
	public String toString() {
		return this.getText() + " [ " + 
		super.getInput(0).toString() + " ]";
	}

	/** {@inheritDoc} */
	public List<Expression> getExpressions() {
		return expressions;
	}
	//Call to default methods in OperatorImplementation

	/**
	 * Sets the expressions.
	 * @param newExpressions Values to set them to.
	 */
	protected void setExpressions(List<Expression> newExpressions) {
		this.expressions = newExpressions;
	}

	/**
	 * Copies attributes into the expressions.
	 * @param outputAttributes Values to set them to.
	 */
	protected void copyExpressions(List<Attribute> outputAttributes) {
		expressions = new ArrayList<Expression>(); 
		expressions.addAll(outputAttributes);
	}

	/**
	 * Sets the Attributes.
	 * @param newAttributes Values to set them to.
	 */
	public void setAttributes(List<Attribute> newAttributes) {
		this.attributes = newAttributes;
	}

	/**
	 * Checks if the attributes are in the list and removes those that are not. 
	 */
	public void checkAttributes(List<Attribute> projectAttributes) 
	throws OptimizationException {

		//remove unrequired attributes. No expressions to accept
		for (int i = 0; i < attributes.size(); ) {
			if (projectAttributes.contains(attributes.get(i)))
				i++;
			else {
				attributes.remove(i);
				expressions.remove(i);		
			}
		}
	}

	/** 
	 * {@inheritDoc}
	 */   
	public void pushLocalNameDown(String newLocalName) {
		//XXX-AG: Commented out method body
		/*
		 * This method was being used to relabel an extent in a 
		 * query. Have commented it out!
		 */
//		for (int i = 0; i < attributes.size(); i++) {
//			attributes.get(i).setLocalName(newLocalName);
//		}
	}

}
