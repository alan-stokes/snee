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
package uk.ac.manchester.cs.snee.compiler.metadata.source;

import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.TopologyReader;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.TopologyReaderException;

/**
 * Maintains metadata required about a sensor network capable of
 * in-network query processing
 */
public class SensorNetworkSourceMetadata extends SourceMetadata {

	Logger logger = 
		Logger.getLogger(SensorNetworkSourceMetadata.class.getName());

	//TODO: Ixent doesn't understand what this is for.
	private int _cardinality = 100;

	private int[] _sourceNodes;

	//Sink node id of the sensor network
	private int gateway;

	//Sensor Network topology
	private Topology topology;
	
	public SensorNetworkSourceMetadata(String sourceName, 
			List<String> extentNames, Element xml,
			String topFile, String resFile, int gateway) 
	throws SourceMetadataException, TopologyReaderException {
		super(sourceName, extentNames, SourceType.SENSOR_NETWORK);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SensorNetworkSourceMetadata()");
		}
		topology = TopologyReader.readNetworkTopology(topFile, resFile);
		//TODO: Not sure that this will work.
		for(String extent : _extentNames) {
			setSourceSites(xml.getElementsByTagName("sites"), extent);
		}
		
		if (logger.isDebugEnabled())
			logger.debug("RETURN SensorNetworkSourceMetadata()");
	}

//	public boolean equals(Object ob) {
//		boolean result = false;
//		if (ob instanceof SensorNetworkSourceMetadata) {
//			SensorNetworkSourceMetadata source = (SensorNetworkSourceMetadata) ob;
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
	
	/**
	 * Set which sites the sensed extent is available from
	 * @param nodesxml configuration information
	 * @param extentName Name of the extent
	 * @throws SourceMetadataException
	 */
	private void setSourceSites(NodeList nodesxml, String extentName) 
	throws SourceMetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER setSourceSites() for " + extentName + 
					" number of sites " + nodesxml.getLength());
		if (nodesxml.getLength() == 0) {
			String message = "No sites information found for " + extentName;
			logger.warn(message);
			throw new SourceMetadataException(message);
		}
		String nodesText = null;
		for (int i = 0; i < nodesxml.getLength(); i++) {
			if (nodesText == null) {
				nodesText = nodesxml.item(i).getTextContent();
			} else {
				nodesText = nodesText + "," + nodesxml.item(i).getTextContent();
			}
		}
		if (logger.isTraceEnabled())
			logger.trace("sites text " + nodesText);
		_sourceNodes = convertNodes(nodesText);
		_cardinality = _sourceNodes.length;
		if (logger.isTraceEnabled())
			logger.trace("RETURN setSourceSites()");
	}

	//TODO: Change metadata definition to take a xs:list rather than comma separated string
	private int[] convertNodes(String text) 
	throws SourceMetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER convertNodes " + text);
		String temp;
		StringTokenizer tokens = new StringTokenizer(text, ",");
		int count = countTokens(tokens);
		//logger.trace("count = "+count);
		
		int[] list = new int[count];
		count = 0;
		tokens = new StringTokenizer(text, ",");
		while (tokens.hasMoreTokens()) {
			temp = tokens.nextToken();
			if (temp.indexOf('-') == -1) {
				list[count] = Integer.parseInt(temp);
				count++;
			} else {
				int start = Integer.parseInt(temp.substring(0, temp
						.indexOf('-')));
				int end = Integer.parseInt(temp.substring(temp
						.indexOf('-') + 1, temp.length()));
				for (int i = start; i <= end; i++) {
					list[count] = i;
					count++;
				}
			}
		}
		Arrays.sort(list);
		count = 0;
		for (int i = 0; i < list.length - 2; i++) {
			if (list[i] >= list[i + 1]) {
				list[i] = Integer.MAX_VALUE;
				count++;
			}
		}
		if (count > 0) {
			Arrays.sort(list);
			int[] newList = new int[list.length - count];
			for (int i = 0; i < newList.length; i++) {
				newList[i] = list[i];
			}
			list = newList;
		}
		String t = "";
		for (int element : list) {
			t = t + "," + element;
		}
		if (logger.isTraceEnabled()) {
			logger.trace("nodes = " + t);
			logger.trace("RETURN setSourceSites()");
		}
		return list;
	}

	/**
	 * Count the number of tokens.
	 * @param tokens
	 * @return
	 * @throws SourceMetadataException No tokens exist
	 */
	private int countTokens(StringTokenizer tokens)
	throws SourceMetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER countTokens() " + tokens);
		int count = 0;
		String temp;
		while (tokens.hasMoreTokens()) {
			temp = tokens.nextToken();
			if (temp.indexOf('-') == -1) {
				count++;
			} else {
				int start = Integer.parseInt(temp.substring(0, temp
						.indexOf('-')));
				int end = Integer.parseInt(temp.substring(temp
						.indexOf('-') + 1, temp.length()));
				//logger.trace(temp+" "+start+"-"+end);
				if (end < start) {
					String message = "Start less than end";
					logger.warn(message);
					throw new SourceMetadataException(message);
				}
				count = count + end - start + 1;
			}
		}
		if (count == 0) {
			String message = "No nodes defined";
			logger.warn(message);
			throw new SourceMetadataException(message);
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN countTokens() number of tokens " + count);
		return count;
	}

	protected void setCardinality (int cardinality)
	{
		_cardinality = cardinality;
	}

	public int getCardinality() {
		return _cardinality;
	}

	@Override
	public String toString() {
		StringBuffer s = new StringBuffer(super.toString());
		s.append("   Cardinality: " + _cardinality);
		return s.toString();
	}

	/**
	 * Returns the nodes in the sensor network which are capable of
	 * sensing data.
	 * @return an array of node identifiers
	 */
	public int[] getSourceNodes() {
		return _sourceNodes;
	}

}
