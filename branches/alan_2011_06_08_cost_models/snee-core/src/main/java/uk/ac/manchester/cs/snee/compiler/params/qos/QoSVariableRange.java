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

import org.apache.log4j.Logger;

/**
 * Class to represent an interval for a QoS variable.  A point is 
 * represented by setting the lower and upper bounds to the same value.
 *
 */
public class QoSVariableRange {

	/**
     * Logger for this class.
     */
    private static final Logger logger = Logger.getLogger(QoSVariableRange.class.getName());
	
	/**
	 * The lower bound of the QoS Variable.
	 */
    private long minValue;

	/**
	 * The upper bound of the QoS Variable.
	 */
    private long maxValue;

    /**
     * Create an instance of the QoS variable range.
     * @param minValue
     * @param maxValue
     */
    public QoSVariableRange(final long minValue, final long maxValue) {
    	if (logger.isDebugEnabled())
            logger.debug("ENTER QoSVariableRange() with minValue=" + 
            		minValue+" maxValue="+maxValue);
    	this.minValue = minValue;
    	this.maxValue = maxValue;
    	if (logger.isDebugEnabled())
            logger.debug("RETURN QoSVariableRange()");
    }

    /**
     * get the upper bound
     * @return
     */
    public long getMaxValue() {
    	return this.maxValue;
    }

    /**
     * set the upper bound
     * @param maxValue
     */
    public void setMaxValue(final long maxValue) {
	this.maxValue = maxValue;
    }

    /**
     * Get the lower bound
     * @return
     */
    public long getMinValue() {
    	return this.minValue;
    }

    /**
     * Set the lower bound
     * @param minValue
     */
    public void setMinValue(final long minValue) {
    	this.minValue = minValue;
    }

}
