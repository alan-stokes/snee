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

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;

public class ScanOperator extends LogicalOperatorImpl 
implements LogicalOperator {

	private Logger logger = 
		Logger.getLogger(ScanOperator.class.getName());
	
	/** List of attributes to be received. */
	private List<Attribute> scannedAttributes;

	private String extentName;

	private List<SourceMetadataAbstract> sources;

	private List<Attribute> outputAttributes;

	private List<Expression> expressions;
	
	public ScanOperator(ExtentMetadata extentMetadata,
			List<SourceMetadataAbstract> sources,
			AttributeType boolType) 
	{
		super(boolType);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER ScanOperator() with " +
					extentMetadata + " #sources=" + sources.size());
		}
		this.setOperatorName("SCAN");
		this.setOperatorDataType(OperatorDataType.RELATION);
		this.extentName = extentMetadata.getExtentName();
		this.sources = sources;
		addMetadataInfo(extentMetadata);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN ScanOperator()");
		}
	}
	
	/**
	 * Sets up the attribute based on the schema.
	 * @param extentMetaData DDL declaration for this extent.
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	private void addMetadataInfo(ExtentMetadata extentMetaData) 
	{
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER addMetaDataInfo() with " +
					extentName);
		}
		scannedAttributes = extentMetaData.getAttributes();
		outputAttributes = scannedAttributes;
		copyExpressions(outputAttributes);
		if (logger.isTraceEnabled())
			logger.trace("RETURN addMetaDataInfo()");
	}

	/**
	 * Copies attributes into the expressions.
	 * @param attributes Values to set them to.
	 */
	protected void copyExpressions(
			List<Attribute> attributes) {
		expressions = new ArrayList<Expression>(); 
		expressions.addAll(attributes);
	}

	@Override
	public String toString() {
		return this.getText();
	}
	
	/**
	 * Return the name of the extent as it appears in the schema.
	 * @return
	 */
	public String getExtentName() {
		return extentName;
	}
	
	/**
	 * Return details of the data sources
	 * @return
	 */
	public List<SourceMetadataAbstract> getSources() {
		return sources;
	}

	public int getCardinality(CardinalityType card) {
		return scannedAttributes.size();
	}

	public boolean isAttributeSensitive() {
		return false;
	}

	public boolean isLocationSensitive() {
		return true;
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
		return false;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @return False. As scan can not handle predicate.
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	public boolean pushSelectDown(Expression predicate) 
	throws SchemaMetadataException, TypeMappingException {
		return false;
	}

	/** 
	 * {@inheritDoc}
	 * Should never be called as there is always a project or aggregation 
	 * between this operator and the rename operator.
	 */   
	public void pushLocalNameDown(String newLocalName) {
		throw new AssertionError("Unexpected call to pushLocalNameDown()"); 
	}

	/** {@inheritDoc} */    
	public List<Attribute> getAttributes() {
		return scannedAttributes;
	}

	/** {@inheritDoc} */    
	public List<Expression> getExpressions() {
		return super.defaultGetExpressions();
	}

}
