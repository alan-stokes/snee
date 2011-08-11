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

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;

/**
 * Operator for converting windows to stream 
 * by outputting tuples no longer in the window.
 * @author Christian
 *
 */
public class DStreamOperator extends LogicalOperatorImpl 
implements LogicalOperator {

//	/**
//	 * Constructs a new DStream operator.
//	 * 
//	 * @param token A DStream Operator token
//	 */
//	public DStreamOperator(AST token) {
//		super();
//		Operator inputOperator = 
//			OperatorFactory.convertAST(token.getFirstChild());
//
//		this.setOperatorName("ISTREAM");
//		this.setNesCTemplateName("istream never set");
//		this.setOperatorDataType(OperatorDataType.STREAM);
//		this.setParamStr("");
//
//		setChildren(new Operator[] {inputOperator});
//	}  

	/**
	 * Constructor.
	 * @param inputOperator Previous Operator in the plan.
	 */
	public DStreamOperator(LogicalOperator inputOperator, AttributeType boolType) {
		super(boolType);

		this.setOperatorName("DSTREAM");
//		this.setNesCTemplateName("istream never set");
		this.setOperatorDataType(OperatorDataType.STREAM);
		this.setOperatorSourceType(inputOperator.getOperatorSourceType());
		this.setSourceRate(inputOperator.getStreamRate());

		setChildren(new LogicalOperator[] {inputOperator});
	}  

	//used by clone method
//	/**
//	 * Constructor that creates a new operator 
//	 * based on a model of an existing operator.
//	 * 
//	 * Used by both the clone method and the constuctor of the physical methods.
//	 * @param model Operator to copy.
//	 */
//	protected DStreamOperator(DStreamOperator model) {
//		super(model);
//	}  

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

	//XXX: Removed by AG as metadata now handled in metadata object
//	/** 
//	 * {@inheritDoc}
//	 * Push passed on to child.
//	 */   
//	public void pushLocalNameDown(String newLocalName) {
//		getInput(0).pushLocalNameDown(newLocalName); 
//	}

	/**
	 * Calculated the cardinality based on the requested type. 
	 * 
	 * @param card Type of cardinailty to be considered.
	 * 
	 * @return The Cardinality calulated as requested.
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
//	}

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
		return false;
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

//	/** 
//	 * Creates a new operator based on this.
//	 * @return A new operator similar to this one.
//	 */
//	public DStreamOperator shallowClone() {
//		DStreamOperator clonedOp = new DStreamOperator(this);
//		return clonedOp;
//	}

//	/**
//	 * Stub.
//	 * @param card stub
//	 * @param node stub
//	 * @param daf stub.
//	 * @return error.
//	 */
//	public double getTimeCost(CardinalityType card, 
//			Site node, DAF daf) {
//		throw new AssertionError("Stub Method called");
//	}

//	/** {@inheritDoc} */
//	public double getTimeCost(CardinalityType card, int numberOfInstances) {
//		throw new AssertionError("Stub Method called");
//	}

//	/** {@inheritDoc} */
//	public AlphaBetaExpression getTimeExpression(
//			CardinalityType card, Site node, 
//			DAF daf, boolean round) {
//		throw new AssertionError("Stub Method called");
//	}

	/**
	 * Some operators do not change the data in any way those could be removed.
	 * This operator does not change the data so can be removed. 
	 * 
	 * @return true;
	 */
	public boolean isRemoveable() {
		return false; 
	}

	//Call to default methods in OperatorImplementation

//	/** {@inheritDoc} */
//	public int[] getSourceSites() {
//		return super.defaultGetSourceSites();
//	}

//	/** {@inheritDoc} */    
//	public int getOutputQueueCardinality(Site node, DAF daf) {
//		return super.defaultGetOutputQueueCardinality(node, daf);
//	}

//	/** {@inheritDoc} */    
//	public int getOutputQueueCardinality(int numberOfInstances) {
//		return super.defaultGetOutputQueueCardinality(numberOfInstances);
//	}

	/** {@inheritDoc} */    
	public List<Attribute> getAttributes() {
		return super.defaultGetAttributes();
	}

	/** {@inheritDoc} */    
	public List<Expression> getExpressions() {
		return super.defaultGetExpressions();
	}

//	/** {@inheritDoc} */    
//	public int getDataMemoryCost(Site node, DAF daf) {
//		return super.defaultGetDataMemoryCost(node, daf);
//	}

}
