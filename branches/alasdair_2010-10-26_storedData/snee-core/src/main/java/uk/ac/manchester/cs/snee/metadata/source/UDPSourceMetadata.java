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
package uk.ac.manchester.cs.snee.metadata.source;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import uk.ac.manchester.cs.snee.metadata.schema.ExtentDoesNotExistException;

/**
 * Implementation of the metadata imported from a UDP source.
 */
public class UDPSourceMetadata extends PullSourceMetadata {

	Logger logger = 
		Logger.getLogger(UDPSourceMetadata.class.getName());
	
	/**
	 * Host name of this UDP source
	 */
	private String _hostName;

	/**
	 * Port number from which each extent can be gathered
	 */
	private Map<String, Integer> _portNumbers = 
		new HashMap<String, Integer>();
	
	public UDPSourceMetadata(String sourceName, 
			List<String> extentNames, String hostName, 
			Element extentNodes) 
	throws SourceMetadataException {
		super(sourceName, extentNames, SourceType.UDP_SOURCE);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER UDPSourceMetadata() with " + hostName);
		}
		_hostName = hostName;
		parseRemainingMetadata(extentNodes);
		if (logger.isDebugEnabled()) {
			logger.trace("RETURN UDPSourceMetadata() " + this);
		}
	}

	private void parseRemainingMetadata(Element extentNodes) 
	throws SourceMetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER parseRemainingMetadata()");
		NodeList extents = extentNodes.getElementsByTagName("extent");
		for (String extent : _extentNames) {
			Element extentElement = findElement(extent, extents);
			Element pushSourceNode = (Element) extentElement.getElementsByTagName("push_source").item(0);
			if (logger.isTraceEnabled())
				logger.trace("Push_source: " + pushSourceNode);
			NodeList nodes = pushSourceNode.getChildNodes();
			//FIXME: Need to assign elements to local variables
			for (int i = 0; i < nodes.getLength(); i++) {
				Node node = nodes.item(i);
				if (node.getNodeName().equalsIgnoreCase("port")) {
					int port = Integer.parseInt(node.getFirstChild().getNodeValue());
					_portNumbers.put(extent, port);
				} else if (node.getNodeName().equalsIgnoreCase("rate")) {
					double rate = Double.parseDouble(node.getFirstChild().getNodeValue());
					_extentRates.put(extent, rate);
				}
			}	
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN parseRemainingMetadata()");
	}

	private Element findElement(String extentName, NodeList extents) 
	throws SourceMetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER findElement() with " + extentName +
					" extents size " + extents.getLength());
		Element element = null;
		boolean found = false;
		for (int i = 0; i < extents.getLength(); i++) {
			Element extent = (Element) extents.item(i);
			String attrValue = extent.getAttribute("name");
			if (attrValue.equalsIgnoreCase(extentName)) {
				if (logger.isTraceEnabled()) {
					logger.trace("Found " + extent + " element for " +
							attrValue);
				}
				found = true;
				element = extent;
				break;
			}               
		}
		if (!found) {
			String message = "No physical schema information found " +
				"for " + extentName;
			logger.warn(message);
			throw new SourceMetadataException(message);
		}
		if (logger.isTraceEnabled() && found)
			logger.trace("RETURN findElement() with " + 
					element.toString());
		return element;
	}

	//	public boolean equals(Object ob) {
	//		boolean result = false;
	//		if (ob instanceof WebServiceSourceMetadata) {
	//			WebServiceSourceMetadata source = (WebServiceSourceMetadata) ob;
	//			//XXX: Not necessarily a complete check of source metadata equality
	//			/*
	//			 * Equality assumed if source refer to the same source type
	//			 * and the same extent.
	//			 */
	//			if (logger.isTraceEnabled())
	//				logger.trace("Testing " + source + "\nwith " + this);
	//			if (source.getName().equals(_name) &&
	//					source.getSourceType() == _sourceType)
	//					result = true;
	//		}
	//		return result;
	//	}

	public String getHost() {
		return _hostName;
	}

	public int getPort(String extentName) 
	throws ExtentDoesNotExistException {
		if (_portNumbers.containsKey(extentName)) {
			return _portNumbers.get(extentName);
		} else {
			throw new ExtentDoesNotExistException("Unknown extent " + 
					extentName);
		}
	}

	@Override
	public String toString() {
		StringBuffer s = new StringBuffer(_sourceName);
		s.append("  Extent Type: " + _sourceType);
		s.append("  Host " + _hostName);
		for (String extent : _extentNames) {
			try {
				s.append("  " + extent + ": " + _portNumbers.get(extent) +
						" @ " + getRate(extent) + " tuples per second");
			} catch (SourceMetadataException e) {
				// XXX: Never reached
			}
		}
		return s.toString();
	}

}
