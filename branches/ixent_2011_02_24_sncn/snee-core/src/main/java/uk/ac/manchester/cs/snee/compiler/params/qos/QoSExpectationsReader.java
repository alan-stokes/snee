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
package uk.ac.manchester.cs.snee.compiler.params.qos;

import java.io.FileNotFoundException;

import javax.xml.xpath.XPathExpressionException;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.metadata.units.Units;
import uk.ac.manchester.cs.snee.metadata.units.UnrecognizedUnitException;


public class QoSExpectationsReader extends QoSExpectations {

    /**
     * Logger for this class.
     */
    private Logger logger = 
            Logger.getLogger(QoSExpectationsReader.class.getName());
	
    private String _queryParamsFile;

	/**
	 * Parses the QoS expectations in the query parameters file.
	 * @param queryID
	 * @param queryParamsFile
	 * @throws QoSException
	 */
    public QoSExpectationsReader(int queryID, String queryParamsFile) 
	throws QoSException {
    	if (logger.isDebugEnabled()) {
            logger.debug("ENTER QoSExpectationsReader() with "+
            		"queryID="+queryID+" queryParamsFile="+queryParamsFile);
    	}
    	this._queryParamsFile = queryParamsFile;
	    this.parseQoSXMLFile();
    	if (logger.isDebugEnabled()) {
            logger.debug("RETURN QoSExpectationsReader");
    	}
    }


    /**
     * Helper method for getting a range for a QoS variable
     * @param queryRoot
     * @return
     * @throws XPathExpressionException
     * @throws FileNotFoundException
     * @throws UnrecognizedUnitException
     */
    private QoSVariableRange getQoSVariableRange(final String queryRoot)
	throws XPathExpressionException, FileNotFoundException,
	UnrecognizedUnitException {	
    	if (logger.isTraceEnabled()) {
            logger.trace("ENTER getQoSVariableRange() with "+
            		"queryRoot="+queryRoot);
    	}
		final Units units = Units.getInstance();
		final String unitName = Utils.doXPathStrQuery(this._queryParamsFile,
			queryRoot + "/snee:units");
		final long scalingFactor = units.getScalingFactor(unitName);
	
		//less-equals
		final String lessEquals = Utils.doXPathStrQuery(
				this._queryParamsFile, queryRoot
				+ "/snee:constraint/snee:less-equals");
		if (lessEquals != null) {
		    return new QoSVariableRange(-1, new Long(lessEquals).longValue()
			    * scalingFactor);
		}
	
		//greater-equals
		final String greaterEquals = Utils.doXPathStrQuery(
				this._queryParamsFile, queryRoot
				+ "/snee:constraint/snee:greater-equals");
		if (greaterEquals != null) {
		    return new QoSVariableRange(new Long(greaterEquals).longValue()
			    * scalingFactor, -1);
		}
	
		//equals
		final String equals = Utils.doXPathStrQuery(this._queryParamsFile,
			queryRoot + "/snee:constraint/snee:equals");
		if (equals != null) {
		    return new QoSVariableRange(new Long(equals).longValue() * scalingFactor,
			    new Long(equals).longValue() * scalingFactor);
		}
	
		//range
		final String minVal = Utils.doXPathStrQuery(this._queryParamsFile,
			queryRoot + "/snee:constraint/snee:range/snee:min-val");
		final String maxVal = Utils.doXPathStrQuery(this._queryParamsFile,
			queryRoot + "/snee:constraint/snee:range/snee:max-val");
		if (minVal != null) {
		    return new QoSVariableRange(new Long(minVal).longValue() * scalingFactor,
			    new Long(maxVal).longValue() * scalingFactor);
		}
    	if (logger.isTraceEnabled()) {
            logger.trace("RETURN getQoSVariableRange()");
    	}
		return null;
	    }

    /**
     * Parses XML file to obtain lower and upper bounds for QoS variables.
     * @throws QoSException
     */
    private void parseQoSXMLFile() throws QoSException {
    	if (logger.isTraceEnabled()) {
            logger.trace("ENTER parseQoSXMLFile()");
    	}
    	
		try {
			logger.trace("Validating query parameters file "+
					this._queryParamsFile);
//			Utils.validateXMLFile(this._queryParamsFile, 
//					"../src/main/resources/schema/query-parameters.xsd");
			//Now read the data from the file
			logger.trace("Querying XML file");
			String xQoS = 
				"/snee:query-parameters/snee:qos-expectations";
			parseOptimizationGoalInfo(xQoS);

			QoSVariableRange alpha = this.getQoSVariableRange(
					xQoS+"/snee:acquisition-interval");			
			this.setAcquisitionIntervalRange(alpha);
			logger.trace(alpha.getMinValue()+"<=alpha<="+alpha.getMaxValue());		
			QoSVariableRange beta = this.getQoSVariableRange(
					xQoS+"/snee:buffering-factor");
			this.setBufferingFactorRange(beta);
			logger.trace(beta.getMinValue()+"<=beta<="+beta.getMaxValue());
			QoSVariableRange delta = this.getQoSVariableRange(
					xQoS+"/snee:delivery-time");
			this.setDeliveryTimeRange(delta);
			logger.trace(delta.getMinValue()+"<=delta<="+delta.getMaxValue());
			//TODO: Weightings, total energy, lifetime not parsed
		} catch (Exception e) {
			throw new QoSException("Error parsing QoS in parameters file :"+
					e.toString());
		}		
    	if (logger.isTraceEnabled()) {
            logger.trace("RETURN parseQoSXMLFile()");
    	}
    }


	private void parseOptimizationGoalInfo(String xQoS)
			throws XPathExpressionException, FileNotFoundException {
    	if (logger.isTraceEnabled()) {
            logger.trace("ENTER parseOptimizationGoalInfo() with "+xQoS);
    	}
		String xOptGoal = xQoS +
			"/snee:optimization-goal";
		String optGoalType = Utils.doXPathStrQuery(
				this._queryParamsFile, xOptGoal+"/snee:type");
		this.setOptimizationType(optGoalType);
		logger.trace("optGoalType="+optGoalType);

		String optVar = Utils.doXPathStrQuery(
				this._queryParamsFile, xOptGoal+"/snee:variable");
		this.setOptimizationVariable(optVar);
		logger.trace("optVar="+optVar);
		
		int optWeighting = Utils.doXPathIntQuery(
				this._queryParamsFile, xOptGoal+"/snee:weighting");
		this.setOptimizationGoalWeighting(optWeighting);
		logger.trace("optWeighting="+optWeighting);
		if (logger.isTraceEnabled()) {
	        logger.trace("RETURN parseOptimizationGoalInfo()");
		}
	}

}