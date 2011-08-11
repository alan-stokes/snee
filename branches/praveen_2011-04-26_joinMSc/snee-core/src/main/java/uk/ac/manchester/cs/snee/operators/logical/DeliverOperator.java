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

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;

/**
 * Encapsulates a Deliver operator, 
 * which should be placed on the destination site of query plan results.
 * @author Ixent Galpin, Christian Brenninkmeijer and Steven Lynden 
 */
public class DeliverOperator extends LogicalOperatorImpl {

	/** Standard Java Logger. */
	private Logger logger = 
		Logger.getLogger(DeliverOperator.class.getName());

	/**
	 * Constructor that places a Deliver at the top of a tree.
	 * @param inputOperator Previous operator.
	 */
	public DeliverOperator(LogicalOperator inputOperator, AttributeType boolType) {
		super(boolType);

		this.setOperatorName("DELIVER");
//		this.setNesCTemplateName("deliver");
		this.setOperatorDataType(inputOperator.getOperatorDataType());
		this.setOperatorSourceType(inputOperator.getOperatorSourceType());
		this.setSourceRate(inputOperator.getStreamRate());

		setChildren(new LogicalOperator[] {inputOperator});
		if (inputOperator == null) {
			throw new AssertionError("input operator can not be null.");
		}
	}  


	public String getParamStr() {
		return "";
	}

	/**
	 * {@inheritDoc}
	 * @throws SNEEConfigurationException 
	 */
	public boolean pushProjectionDown(List<Expression> projectExpressions, 
			List<Attribute> projectAttributes) 
	throws OptimizationException, SNEEConfigurationException {
		return getInput(0).pushProjectionDown(
				projectExpressions, projectAttributes);
	}

	/**
	 * {@inheritDoc}
	 * 
	 * Push is passed on to the child operator.
	 * 
	 * @return The result of the push to the child.
	 * @throws AssertionError 
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 * @throws SNEEConfigurationException 
	 */
	public boolean pushSelectIntoLeafOp(Expression predicate) 
	throws SchemaMetadataException, AssertionError, TypeMappingException, 
	SNEEConfigurationException {
		return this.getInput(0).pushSelectIntoLeafOp(predicate);
	}

	/** 
	 * {@inheritDoc}
	 * Should never be called as deliver is after the rename operator.
	 */   
	public void pushLocalNameDown(String newLocalName) {
		throw new AssertionError("Unexpected call to pushLocalNameDown()"); 
	}

	/**
	 * Calculated the cardinality based on the requested type. 
	 * 
	 * @param card Type of cardinality to be considered.
	 * 
	 * @return The Cardinality calculated as requested.
	 */
	public int getCardinality(CardinalityType card) {
		return (this.getInput(0)).getCardinality(card);
	}

//	/** {@inheritDoc} */
//	public int getCardinality(CardinalityType card, 
//			Site node, DAF daf) {
//		return getInputCardinality(card, node, daf, 0);
//	}

	//	/** {@inheritDoc} */
	//	public AlphaBetaExpression getCardinality(CardinalityType card, 
	//			Site node, DAF daf, boolean round) {
	//		return getInputCardinality(card, node, daf, round, 0);
	//    }

	/**
	 * Used to determine if the operator is Attribute sensitive.
	 * 
	 * @return false.
	 */
	public boolean isAttributeSensitive() {
		return false;
	}

	/** {@inheritDoc} */
	public boolean isLocationSensitive() {
		return true;
	}

	/** {@inheritDoc}
	 * @return false;
	 */
	public boolean acceptsPredicates() {
		return false;
	}

	/** {@inheritDoc} */
	public String toString() {
		return this.getText() + " [ " + 
		super.getInput(0).toString() + " ]";  
	}

	/**
	 * Some operators do not change the data in any way those could be removed.
	 * This operator does change the data so can not be. 
	 * 
	 * @return False. 
	 */
	public boolean isRemoveable() {
		return false;
	}

	/** {@inheritDoc} */    
	public List<Attribute> getAttributes() {
		return super.defaultGetAttributes();
	}

	/** {@inheritDoc} */    
	public List<Expression> getExpressions() {
		return super.defaultGetExpressions();
	}
}
