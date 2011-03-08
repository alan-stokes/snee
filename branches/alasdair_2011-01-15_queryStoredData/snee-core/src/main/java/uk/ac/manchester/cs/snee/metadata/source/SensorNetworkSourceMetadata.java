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

import java.util.Date;
import java.util.List;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReader;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.sncb.SNCB;
import uk.ac.manchester.cs.snee.sncb.SNCBException;

/**
 * Maintains metadata required about a sensor network capable of
 * in-network query processing
 */
public class SensorNetworkSourceMetadata extends SourceMetadataAbstract {

	Logger logger = 
		Logger.getLogger(SensorNetworkSourceMetadata.class.getName());

	/**
	 * Source sites in the sensor network.
	 */
	private int[] _sourceNodes;

	/**
	 * Sink node id of the sensor network
	 */
	private int[] _gateways;

	/**
	 * Sensor Network topology
	 */
	private Topology _topology;
	
	/**
	 * Maps extent names to source nodes
	 */
	private TreeMap<String,int[]> _extentToSitesMapping = 
		new TreeMap<String,int[]>(); 
	
	/**
	 * The Sensor Network Connectivity bridge to interface with this sensor network.
	 */
 	private SNCB sncb;	
		
	/**
	 * Constructor for Sensor Network Metadata.
	 * @param sourceName
	 * @param extentNames
	 * @param xml
	 * @param defaultTopFile
	 * @param defaultResFile
	 * @param gateways
	 * @throws SourceMetadataException
	 * @throws TopologyReaderException
	 * @throws SNCBException 
	 * @throws SNEEConfigurationException 
	 */
	public SensorNetworkSourceMetadata(String sourceName, List<String> 
	extentNames, Element xml, String defaultTopFile, String defaultResFile, int[] gateways,
	SNCB sncb) 
	throws SourceMetadataException, TopologyReaderException, SNCBException, 
	SNEEConfigurationException {
		super(sourceName, extentNames, SourceType.SENSOR_NETWORK);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SensorNetworkSourceMetadata()");
		}
		if (gateways.length==0) {
			throw new SourceMetadataException("No gateway nodes specified "+
					"for sensor network "+sourceName);
		}
		if (gateways.length>1) {
			throw new SourceMetadataException("More than one gateway node " +
					"specified for sensor network "+sourceName + "; this is " +
					"currently not supported.");
		}
		this.sncb = sncb;
		
		if (SNEEProperties.getBoolSetting(
			SNEEPropertyNames.SNCB_PERFORM_METADATA_COLLECTION)) {
			logger.info("Invoking network formation and metadata collection");
			Date now = new Date();
			String timeStampStr = now.toString().replaceAll(" ", "_").replaceAll(":", "_");
			String root = System.getProperty("user.dir")+"/output/metadata/";
			String topFile = root + "topology-"+this.getSourceName()+"-"+timeStampStr+".xml";
			String resFile = root + "resources-"+this.getSourceName()+"-"+timeStampStr+".xml";
			System.out.println(topFile + "\n" + resFile);
			this.sncb.init(topFile, resFile);
			this._topology = TopologyReader.readNetworkTopology(
					topFile, resFile);
		} else {
			logger.info("Using default topology file: "+defaultTopFile);
			this._topology = TopologyReader.readNetworkTopology(
					defaultTopFile, defaultResFile);			
		}

		this._gateways = gateways;
		setSourceSites(xml.getElementsByTagName("extent"));
		validateGateways();	
			
		if (logger.isDebugEnabled())
			logger.debug("RETURN SensorNetworkSourceMetadata()");
	}
	
	private void validateGateways() throws SourceMetadataException {

		for (int i=0; i<_gateways.length; i++) {
			int g = _gateways[i];
			if (_topology.getNode(g)==null) {
				throw new SourceMetadataException("Gateway node id "+g+
						" specified in the physical schema not found in "+
						"the topology file.");
			}
		}
	}

	/**
	 * Set which sites the sensed extents are available from
	 * @param nodesxml configuration information
	 * @throws SourceMetadataException
	 */
	private void setSourceSites(NodeList nodesxml) 
	throws SourceMetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER setSourceSites() for " +
					"number of extents " + nodesxml.getLength());
		StringBuffer sourceSitesText = new StringBuffer();
		for (int i = 0; i < nodesxml.getLength(); i++) {
			Node extentElem = nodesxml.item(i);
			NamedNodeMap attrs = extentElem.getAttributes();
			String extentName = attrs.getNamedItem("name").getNodeValue();
			logger.trace("extentName="+extentName);
			Node sitesElem = extentElem.getChildNodes().item(1);
			String sitesText = sitesElem.getFirstChild().getNodeValue();
			logger.trace("sites="+sitesText);
			if (sourceSitesText.length()==0) {
				sourceSitesText.append(sitesText);
			} else {
				sourceSitesText.append("," + sitesText);				
			}
			int[] sites = SourceMetadataUtils.convertNodes(sitesText);
			if (sites.length == 0) {
				String message = "No sites information found for "+extentName;
				logger.warn(message);
				throw new SourceMetadataException(message);
			}
			_extentToSitesMapping.put(extentName.toLowerCase(), sites);
			logger.trace("Extent "+extentName+": added source sites "+
					sites.toString());
		}
		if (logger.isTraceEnabled())
			logger.trace("sites text " + sourceSitesText);
		_sourceNodes = SourceMetadataUtils.convertNodes(
				sourceSitesText.toString());
		if (logger.isTraceEnabled())
			logger.trace("RETURN setSourceSites()");
	}

	@Override
	public String toString() {
		StringBuffer s = new StringBuffer(super.toString());
		return s.toString();
	}

	/**
	 * Returns the nodes in the sensor network which are capable of
	 * sensing data.
	 * @return an array of node identifiers
	 */
	public int[] getSourceSites() {
		return _sourceNodes;
	}

	/**
	 * Returns the nodes in the sensor network which are capable of
	 * sensing data for a given extent.
	 * @return an array of node identifiers
	 */
	public int[] getSourceSites(String extentName) {
		int sites[] = this._extentToSitesMapping.get(extentName.toLowerCase());
		return sites;
	}

	/**
	 * Returns the network topology.
	 * @return
	 */
	public Topology getTopology() {
		return this._topology;
	}

	public int getGateway() {
		return this._gateways[0];
	}
	
	public SNCB getSNCB() {
		return this.sncb;
	}
}
