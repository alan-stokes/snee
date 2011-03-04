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

//STUB.
public class ScanOperator extends LogicalOperatorImpl 
implements LogicalOperator {

	private Logger logger = 
		Logger.getLogger(ScanOperator.class.getName());
	
	public ScanOperator(AttributeType boolType) 
	{
		super(boolType);
		String message = "Stub Method called";
		logger.warn(message);
		throw new AssertionError(message);
	}

	public LogicalOperatorImpl shallowClone() {
		String message = "Stub Method called";
		logger.warn(message);
		throw new AssertionError(message);
	}

	@Override
	public String toString() {
		String message = "Stub Method called";
		logger.warn(message);
		throw new AssertionError(message);
	}

	public int getCardinality(CardinalityType card) {
		String message = "Stub Method called";
		logger.warn(message);
		throw new AssertionError(message);
	}

//	/** {@inheritDoc} */
//	public int getCardinality(CardinalityType card, 
//			Site node, DAF daf) {
//		return getInputCardinality(card, node, daf, 0);
//	}

//	/** {@inheritDoc} */
//	public AlphaBetaExpression getCardinality(CardinalityType card, 
//			Site node, DAF daf, boolean round) {
//		return null;
//	}

//	public int getPhysicalMaxCardinality(Site node, DAF daf) {
//		throw new AssertionError("Stub Method called");
//	}

//	public double getTimeCost(CardinalityType card, Site node, DAF daf) {
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

	public boolean isAttributeSensitive() {
		String message = "Stub Method called";
		logger.warn(message);
		throw new AssertionError(message);
	}

	public boolean isLocationSensitive() {
		String message = "Stub Method called";
		logger.warn(message);
		throw new AssertionError(message);
	}

	/** {@inheritDoc}
	 * @return false;
	 */
	public boolean acceptsPredicates() {
		return false;
	}

	public boolean isRemoveable() {
		String message = "Stub Method called";
		logger.warn(message);
		throw new AssertionError(message);
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean pushProjectionDown(List<Expression> projectExpressions, 
			List<Attribute> projectAttributes) 
	throws OptimizationException {
		return false;
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
		return this.getInput(0).pushSelectDown(predicate);
	}

	/** 
	 * {@inheritDoc}
	 * Should never be called as there is always a project or aggregation 
	 * between this operator and the rename operator.
	 */   
	public void pushLocalNameDown(String newLocalName) {
		throw new AssertionError("Unexpected call to pushLocalNameDown()"); 
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
