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

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.NoPredicate;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;

/**
 * Project Operator.
 * @author Christian Brenninkmeijer, Ixent Galpin and Steven Lynden
 */
public class ProjectOperator extends PredicateOperator {

	public ProjectOperator (List <Expression> expressions, 
			List <Attribute> attributes, LogicalOperator input, 
			AttributeType boolType){
		super(expressions, attributes, input, boolType);
		this.setOperatorName("PROJECT");
//		this.setNesCTemplateName("project");
		setOperatorDataType(input.getOperatorDataType());
		setParamStr(expressions.toString());
	}

//	/** 
//	 * Sets the operator data type based on the AST token.
//	 * 
//	 * @param token AST token for the operator.
//	 */
//	private void setOperatorDataType(AST token) {
//		switch (token.getType()) {
//		case SNEEqlOperatorParserTokenTypes.RELPROJECT: 
//			setOperatorDataType(OperatorDataType.RELATION);
//			return;
//		case SNEEqlOperatorParserTokenTypes.STRPROJECT: 
//			setOperatorDataType(OperatorDataType.STREAM);
//			return;
//		case SNEEqlOperatorParserTokenTypes.WINPROJECT: 
//			setOperatorDataType(OperatorDataType.WINDOWS);
//			return;
//		default: 
//			throw new AssertionError("Unexpected AST token"); 
//		}
//	}

//	/**
//	 * Constructor that creates a new operator
//	 * based on a model of an existing operator.
//	 *
//	 * @param model Another ProjectOperator 
//	 *   upon which this new one will be cloned.
//	 */
//	protected ProjectOperator(ProjectOperator model) {
//		super(model);
//	}

	/**
	 * {@inheritDoc}
	 */
	public boolean pushProjectionDown(List<Expression> projectExpressions, 
			List<Attribute> projectAttributes) 
	throws OptimizationException {

		boolean accept = false;
		if (projectAttributes.size() > 0) {
			if (projectExpressions.size() > 0) {
				setExpressions(projectExpressions);
				setAttributes(projectAttributes);
				accept = true;
			} else {
				checkAttributes(projectAttributes);
			}
		}	
		if (getInput(0).pushProjectionDown(getExpressions(), getAttributes())) {
			//as previous will now return the attributes.
			copyExpressions(getAttributes());
		} 
		return accept;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * This first version does not allow predicates to be pushed through a project.
	 * 
	 * An alternative would be to rework the predicate to work with the output.
	 * For example if you have a project temp + pressure as myVal,
	 * Then a predicate myVal > 25.
	 * You could replace this with temp + pressure > 25 and push it down.
	 * 
	 * @return False Because I was too lazy to rewrite predicates.
	 * @throws AssertionError 
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	public boolean pushSelectDown(Expression predicate) 
	throws SchemaMetadataException, AssertionError, TypeMappingException {
		this.getInput(0).pushSelectDown(new NoPredicate());
		return false;
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
//	}

	/**
	 * Used to determine if the operator is Attribute sensitive.
	 *
	 * @return false.
	 */
	public boolean isAttributeSensitive() {
		return false;
	}

//	/** {@inheritDoc} */
//	public ProjectOperator shallowClone() {
//		//TODO: projectList needs to be properly cloned
//		ProjectOperator clonedOp = new ProjectOperator(this);
//		return clonedOp;
//	}

//	private double getTimeCost(int tuples) {
//		return getOverheadTimeCost()
//		+ CostParameters.getCopyTuple() * tuples;
//	}

//	/** {@inheritDoc} */
//	public double getTimeCost(CardinalityType card, 
//			Site node, DAF daf) {
//		int tuples = this.getInputCardinality(card, node, daf, 0);
//		return getTimeCost(tuples);
//	}

//	/** {@inheritDoc} */
//	public double getTimeCost(CardinalityType card, int numberOfInstances) {
//		int tuples = this.getInputCardinality(card, 0, numberOfInstances);
//		return getTimeCost(tuples);
//	}

//	/** {@inheritDoc} */
//	public AlphaBetaExpression getTimeExpression(
//			CardinalityType card, Site node, 
//			DAF daf, boolean round) {
//		AlphaBetaExpression result = new AlphaBetaExpression();
//		result.addBetaTerm(getOverheadTimeCost());
//		AlphaBetaExpression tuples 
//		= this.getInputCardinality(card, node, daf, round, 0);
//		tuples.multiplyBy(CostParameters.getCopyTuple());
//		result.add(tuples);
//		return result;
//	}

	/** {@inheritDoc} */
	public boolean isRemoveable() {
		return getExpressions().equals(this.getInput(0).getAttributes());
	}

//	/**
//	 * Displays the results of the cost functions.
//	 * @param node Physical mote on which this operator has been placed.
//	 * @param daf Distributed query plan this operator is part of.
//	 * @return the calculated time
//	 */
//	public double getTimeCost2(Site node, DAF daf) {
//		return SharedCostFunctions.dProject(getExpressions(), 
//				getCardinality(CardinalityType.MAX, node, daf)); 
//	}

//	/**
//	 * Displays the results of the cost functions.
//	 * @param node Physical mote on which this operator has been placed.
//	 * @param daf Distributed query plan this operator is part of.
//	 * @return OutputQueueCardinality * PhytsicalTuplesSize
//	 */
//	public double getEnergyCost2(Site node, DAF daf) {
//		return SharedCostFunctions.eProject(getExpressions(), 
//				getCardinality(CardinalityType.AVERAGE, node, daf)); 
//	}

//	/**
//	 * Displays the results of the cost functions.
//	 * @param node Physical mote on which this operator has been placed.
//	 * @param daf Distributed query plan this operator is part of.
//	 * @return OutputQueueCardinality * PhytsicalTuplesSize
//	 */
//	public int getDataMemoryCost2(Site node, DAF daf) {
//		return SharedCostFunctions.mProject(
//				getExpressions(), 
//				getCardinality(CardinalityType.MAX, node, daf)); 
//	}

}
