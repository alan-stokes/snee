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

import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;

public class UnionOperator extends LogicalOperatorImpl 
implements LogicalOperator {

	private Logger logger = 
		Logger.getLogger(UnionOperator.class.getName());

	public UnionOperator(LogicalOperator left, LogicalOperator right, AttributeType boolType) {
		super(boolType);
		this.setOperatorName("UNION");
		setOperatorDataType(OperatorDataType.STREAM);
		setChildren(new LogicalOperator[] {left, right});
		this.setOperatorSourceType(getOperatorSourceType(left, right));
		this.setSourceRate(getSourceRate(left, right));
	}

	@Override
	public String toString() {
		return this.getText() + " [ " + getInput(0).toString() 
			+ ", " + getInput(1).toString() + " ]";
	}

	public int getCardinality(CardinalityType card) {
		int left = getInput(0).getCardinality(card);
		int right = getInput(1).getCardinality(card);
		return left + right;
	}

	public boolean isAttributeSensitive() {
		return false;
	}

	public boolean isLocationSensitive() {
		return false;
	}

	public boolean isRecursive() {
		return false;
	}

	/** {@inheritDoc}
	 * @return false;
	 */
	public boolean acceptsPredicates() {
		return false;
	}

	public boolean isRemoveable() {
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean pushProjectionDown(List<Expression> projectExpressions, 
			List<Attribute> projectAttributes) 
	throws OptimizationException {
		boolean accepted = false;
		if (getInput(0).pushProjectionDown(
				projectExpressions, projectAttributes) &&
			getInput(1).pushProjectionDown(
						projectExpressions, projectAttributes))
			accepted  = true;
		return accepted;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @return False. As scan can not handle predicate.
	 * @throws AssertionError 
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	public boolean pushSelectDown(Expression predicate) 
	throws SchemaMetadataException, AssertionError, TypeMappingException {
		boolean accepted = false;
		if (getInput(0).pushSelectDown(predicate) &&
				getInput(1).pushSelectDown(predicate))
			accepted = true;
		return accepted ;
	}

	/** 
	 * {@inheritDoc}
	 * Should never be called as there is always a project or aggregation 
	 * between this operator and the rename operator.
	 */   
	public void pushLocalNameDown(String newLocalName) {
		throw new AssertionError("Unexpected call to pushLocalNameDown()"); 
	}

	/** 
	 * {@inheritDoc} 
	 * Output attributes are the same as the left operator
	 */    
	public List<Attribute> getAttributes() {
		return getInput(0).getAttributes();
	}

	/** {@inheritDoc} */    
	public List<Expression> getExpressions() {
		return super.defaultGetExpressions();
	}
	
	/**
	 * This method compares the left and right operator based on its source type
	 * and returns the rate based on the precedence Pushed>pulled>Scanned
	 * @param left
	 * @param right
	 * @return
	 */
	protected double getSourceRate(LogicalOperator left, LogicalOperator right) {
		return left.getSourceRate() + right.getSourceRate();
	}

}
