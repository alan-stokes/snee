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
package uk.ac.manchester.cs.snee.compiler.metadata;


import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;

public class CostParameters {

    private final Logger logger = Logger.getLogger(CostParameters.class
	    .getName());

    private float copyTuple;

    private float applyPredicate;

    private float checkTuple;

    //private float compareTuple;

    private float doCalculation;

    //private float concatenate;

    private float callMethod;

    private float signalEvent;

    private float acquireData;

    private float scanTuple;

    private float turnOnRadio;

    private float turnOffRadio;

    private float radioSyncWindow;

    private float sendPacket;

    private float setAValue;

    private float deliverTuple;

    private float minimumTimerInterval;

	private String paramFile;

	private int maxMessagePayloadSize;

	private int payloadOverhead;

	private int synchronizationError = 10;
	
	private int tossimSynchronizationPeriodLength = 3000;
	
	private int managementSection = 10000;
	
	private int endManagementSection = 100;
	
	/** Constant for length of per tuple overhead String. */ 
	private int deliverOverhead = 10;

	/** Maximum String length to represent an attribute. */
	private int attributeStringLength = 5;

	/** Maximum size of a deliver packet. */
	private int deliverPayloadSize = 28;

    public CostParameters(String paramFile) throws CostParametersException {
    	this.paramFile = paramFile;
    	try {
			Document doc = parseFile();
		    Element root = (Element) doc.getFirstChild();
		    copyTuple = getFloatValue(root, "CopyTuple", "time");
		    applyPredicate = getFloatValue(root, "ApplyPredicate", "time");
		    checkTuple = getFloatValue(root, "CheckTuple", "time");
		    //compareTuple = getFloatValue(root,"CompareTuple", "time");
		    doCalculation = getFloatValue(root, "DoCalculation", "time");
		    //concatenate = getFloatValue(root,"Concatenate", "time");
		    callMethod = getFloatValue(root, "CallMethod", "time");
		    signalEvent = getFloatValue(root, "SignalEvent", "time");
		    acquireData = getFloatValue(root, "AcquireData", "time");
		    scanTuple = getFloatValue(root, "ScanTuple", "time");
		    turnOnRadio = getFloatValue(root, "TurnOnRadio", "time");
		    turnOffRadio = getFloatValue(root, "TurnOffRadio", "time");
		    radioSyncWindow = getFloatValue(root, "RadioSyncWindow", "time");
		    sendPacket = getFloatValue(root, "SendPacket", "time");
		    setAValue = getFloatValue(root, "SetAValue", "time");
		    deliverTuple = getFloatValue(root, "DeliverTuple", "time");
		    minimumTimerInterval = getFloatValue(root, "MinimumTimerInterval", "time");
		    managementSection = (int)getFloatValue(root, "ManagementSection", "time");
		    endManagementSection = (int)getFloatValue(root, "EndManagementSection", "time");
		    //root = (Element) root.getNextSibling().getNextSibling();
		    maxMessagePayloadSize = (int)getFloatValue(root, "MaxMessagePayloadSize", "bytes");
		    payloadOverhead = (int)getFloatValue(root, "PayloadOverhead", "bytes");
		} catch (final Exception e) {
		    throw new CostParametersException(e);
		}
    }

    private float getFloatValue(final Element elem, final String name, 
    String attrName) throws SchemaMetadataException {
		final NodeList elements = elem.getElementsByTagName(name);
		if (elements.getLength() == 0) {
		    throw new SchemaMetadataException("Operator MetaData " + name
			    + " not found. Please add it to "
			    + paramFile);
		}
		if (elements.getLength() > 1) {
		    logger.warn("Operator MetaData " + name
			    + " found twice. Please check "
			    + paramFile);
		}
		final String temp = ((Element) elements.item(0)).getAttribute(attrName);
		if ((temp == null) || (temp.length() == 0)) {
		    throw new SchemaMetadataException("Operator MetaData " + name
			    + " does not have a time attribute. Please add it to "
			    + paramFile);
		}
		try {
		    logger.trace("" + name + " = " + Float.parseFloat(temp));
		    return Float.parseFloat(temp);
		} catch (final Exception e) {
		    throw new SchemaMetadataException("Operator MetaData " + name
			    + " can not be converted to a float. Please add it to "
			    + paramFile);
		}
    }

    private Document parseFile() throws CostParametersException {
	final DocumentBuilderFactory factory = DocumentBuilderFactory
		.newInstance();
	try {
	    final DocumentBuilder builder = factory.newDocumentBuilder();
	    logger.info("reading metadata from " + paramFile);
	    return builder.parse(paramFile);
	} catch (final Exception e) {
		throw new CostParametersException(e);
	}
    }

    public float getCopyTuple() {
	return copyTuple;
    }

    public float getApplyPredicate() {
	return applyPredicate;
    }

    /**/
    public float getCheckTuple() {
	return checkTuple;
    }

    /** /
     public float getConcatenate()
     {
     return concatenate;
     }
     /**/
    public float getDoCalculation() {
	return doCalculation;
    }

    public float getCallMethod() {
	return callMethod;
    }

    public float getSignalEvent() {
	return signalEvent;
    }

    public float getAcquireData() {
	return acquireData;
    }

    public float getScanTuple() {
	return scanTuple;
    }

    public float getTurnOnRadio() {
	return turnOnRadio;
    }

    public float getTurnOffRadio() {
	return turnOffRadio;
    }

    public float getRadioSyncWindow() {
	return radioSyncWindow;
    }

    public float getSendPacket() {
	return sendPacket;
    }

    public float getSetAValue() {
	return setAValue;
    }

    public float getDeliverTuple() {
	return deliverTuple;
    }

    public float getMinimumTimerInterval() {
	return minimumTimerInterval;
    }

    
    public int getSynchronizationError() {
    	return this.synchronizationError;
    }
    
    public int getTossimSynchronizationPeriodLength() {
    	return this.tossimSynchronizationPeriodLength;
    }
    
    public int getManagementSectionDuration() {
    	return this.managementSection;
    }
    
    public int getEndManagementSectionDuration() {
    	return this.endManagementSection;
    }
    
    /**
     * In bytes
     * @return
     */
	public int getMaxMessagePayloadSize() {
		return maxMessagePayloadSize;
	}

	/**
	 * In bytes
	 * @return
	 */
	public int getPayloadOverhead() {
		return payloadOverhead;
	}

	public int getDeliverOverhead() {
		return this.deliverOverhead;
	}

	
	public int getAttributeStringLength() {
		return this.attributeStringLength;
	}

	public int getDeliverPayloadSize() {
		return this.deliverPayloadSize;
	}


}
