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

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.Types;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;

public class AcquireOperator extends InputOperator {

	/**
	 *  Logger for this class.
	 */
	private Logger logger = 
		Logger.getLogger(AcquireOperator.class.getName());

    /** 
     * List of the attributes that are acquired by this operator. 
     * Includes sensed attributes as well as time, id 
     * and any control attributes such as EvalTime.
     * Includes attributes required for predicates.
     * All attributes are in the original format 
     * before any expressions are applied.
     */
    private List<Attribute> acquiredAttributes;

	/**
	 * Metadata about the types supported.
	 */
	Types _types;
	
	/**
	 * Constructs a new Acquire operator.
	 * 
	 * @param extentMetaData Schema data about the extent
	 * @param types type information as read in from the types file
	 * @param source Metadata about data sources for the acquire extent
	 * @param boolType type used for booleans
	 * @throws SchemaMetadataException
	 * @throws TypeMappingException
	 */
	public AcquireOperator(ExtentMetadata extentMetadata, 
			Types types, 
			SourceMetadataAbstract source,
			AttributeType boolType) 
	throws SchemaMetadataException, TypeMappingException {
		super(extentMetadata, source, boolType);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER AcquireOperator() with " + 
					extentMetadata + " source=" + source.getSourceName());
		}
		this.setOperatorName("ACQUIRE");
		this.setOperatorDataType(OperatorDataType.STREAM);
		this._types=types;

		this.extentName = extentMetadata.getExtentName();

		updateInputAttributes(); 
		updateMetadataInfo(extentMetadata); //WHICH ORDER??
		if (logger.isDebugEnabled())
			logger.debug("RETURN AcquireOperator()");
	} 

	/**
	 * Sets up the attribute based on the schema.
	 * @param extentMetadata DDL declaration for this extent.
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	private void updateMetadataInfo(ExtentMetadata extentMetadata) 
	throws SchemaMetadataException, TypeMappingException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER addMetaDataInfo() with " +
					extentMetadata);
		}
		outputAttributes = new ArrayList<Attribute>();
		inputAttributes = extentMetadata.getAttributes();
		outputAttributes.addAll(inputAttributes);
//		sites =  sourceMetaData.getSourceNodes();
		copyExpressions(outputAttributes);
		acquiredAttributes = (ArrayList<Attribute>) outputAttributes;
		if (logger.isTraceEnabled())
			logger.trace("RETURN addMetaDataInfo()");
	}

	/**
	 * Returns a string representation of the operator.
	 * @return Operator as a String.
	 */
	public String toString() {
		return this.getText();
	}

	/** {@inheritDoc} */
	public boolean acceptsPredicates() {
		try {
			return SNEEProperties.getBoolSetting(
					SNEEPropertyNames.LOGICAL_REWRITER_COMBINE_ACQUIRE_SELECT);
		} catch (SNEEConfigurationException e) {
			return true;
		}
	}

	/**
	 * Get the list of attributes acquired/ sensed by this operator.
	 * List is before projection is pushed down.
	 * @return list of acquired attributes.
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	public List<Attribute> getAcquiredAttributes() 
	throws SchemaMetadataException, TypeMappingException {
		assert (acquiredAttributes != null);
		return acquiredAttributes;
	}

	public String getParamStr() {
		return this.extentName + 
		" (cardinality=" + getCardinality(null) +
		" source=" + this.getSource().getSourceName() + ")\n " + 
		getPredicate() + "\n" +
		this.getExpressions().toString();
	}

}
