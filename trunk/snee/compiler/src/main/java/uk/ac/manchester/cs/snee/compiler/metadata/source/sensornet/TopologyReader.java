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
package uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.w3c.dom.NodeList;

import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.metadata.units.Units;

public class TopologyReader {

    static Logger logger = Logger.getLogger(TopologyReader.class.getName());

    public static Topology readNetworkTopology(final String topologyFile,
	    final String siteResourceFile) throws TopologyReaderException {

		final Topology sensornet = new Topology("sensornet-topology", true);
	
		/** Read the topology graph information **/
		if (topologyFile.toUpperCase().endsWith("NSS")) {
			readTopologyNSSFile(sensornet, topologyFile); //backwards compatibility				
		} else {
		    readTopologyXMLFile(sensornet, topologyFile);
		}
	
		/** Read site resources file **/
		readSiteResourceXMLFile(sensornet, siteResourceFile);
	
		return sensornet;
    }

    private static void readTopologyXMLFile(final Topology sensornet,
	    final String topologyFile) throws TopologyReaderException {
    	
    	try {
    		//TODO: get maven to do this?
//			Utils.validateXMLFile(topologyFile, 
//    			"../src/main/resources/schema/network-topology.xsd");
		
			final Units units = Units.getInstance();
			final long energyScalingFactor = units.getEnergyScalingFactor(Utils
				.doXPathStrQuery(topologyFile,
					"/snee:network-topology/snee:units/snee:energy"));
			final long timeScalingFactor = units.getTimeScalingFactor(Utils
				.doXPathStrQuery(topologyFile,
					"/snee:network-topology/snee:units/snee:time"));
		
			final NodeList nodeList = Utils.doXPathQuery(topologyFile,
				"/snee:network-topology/snee:radio-links/snee:radio-link");
			for (int i = 0; i < nodeList.getLength(); i++) {
			    final org.w3c.dom.Node node = nodeList.item(i);
			    final String sourceID = Utils.doXPathStrQuery(node, "@source");
			    final String destID = Utils.doXPathStrQuery(node, "@dest");
			    final boolean bidirectional = Boolean.parseBoolean(Utils
				    .doXPathStrQuery(node, "@bidirectional"));
			    final double radioLossCost = Double.parseDouble(
			    		Utils.doXPathStrQuery(node, "@radio-loss"));
			    final double energyCost = Double.parseDouble(Utils.doXPathStrQuery(
				    node, "@energy"));
			    final double latencyCost = Double.parseDouble(Utils.doXPathStrQuery(
			    	node, "@time"));
		
			    RadioLink link = sensornet.addRadioLink(
			    		sourceID, destID, false, radioLossCost);
			    link.setEnergyCost(energyCost * energyScalingFactor);
			    link.setLatencyCost(latencyCost * timeScalingFactor);
			     
			    if (bidirectional) {
			    	link = sensornet.addRadioLink(
			    			destID, sourceID, true, radioLossCost);
				    link.setEnergyCost(energyCost * energyScalingFactor);
				    link.setLatencyCost(latencyCost * timeScalingFactor);
			    }
			}
    	} catch (Exception e) {
    		throw new TopologyReaderException(e.getMessage());
    	}
    }

    /**
     * Reads a graph from an NSS file (the format used by tossim in TinyOS 1.x).
     * @param	fname 	the name of the input file
     * 
     */
    public static void readTopologyNSSFile(final Topology sensornet,
	    final String fname) throws TopologyReaderException {

		try {
			Reader r = new BufferedReader(new FileReader(fname));

			final StreamTokenizer st = new StreamTokenizer(r);
		    st.parseNumbers();
		    st.commentChar('#');
		    while (st.nextToken() != StreamTokenizer.TT_EOF) {
		    	String id1;
			if (st.ttype == StreamTokenizer.TT_NUMBER) {
			    id1 = new Integer((int) st.nval).toString();
			} else {
			    id1 = st.sval;
			}
			logger.info("ID1 = " + id1 + " from " + st.ttype);
			st.nextToken();
			if (st.ttype != 58) { // ":"
			    throw new TopologyReaderException(
				    "Unexpected Network topology format in file "
					    + fname + " Expected : as second token");
			}
			st.nextToken();
			String id2;
			if (st.ttype == StreamTokenizer.TT_NUMBER) {
			    id2 = new Integer((int) st.nval).toString();
			} else {
			    id2 = st.sval;
			}
			logger.info("ID2 = " + id2 + " from " + st.ttype);
			st.nextToken();
			if (st.ttype != 58) {
			    throw new TopologyReaderException(
				    "Unexpected Network topology format in file "
					    + fname + " Expected : as fourth token");
			}
			st.nextToken();
			final double w = st.nval; //weight
			logger.info("W = " + w + " from " + st.ttype);
			sensornet.addRadioLink(id1, id2, false, w); 
			//for now, add edges parsed are assumed to be undirectional
		    }
	
		    logger.info("Graph \"" + sensornet.getName() + "\" parsed.");
		    logger.info("The graph has " + sensornet.getNumEdges()
			+ " edges and " + sensornet.getNumNodes() + " vertices");
		    
		} catch (IOException e) {
			throw new TopologyReaderException("IO Error reading topology file.");
		}

    }

    private static void readSiteResourceXMLFile(final Topology sensornet,
	    final String siteResourceFile) throws TopologyReaderException {

    	try {
    		
//			Utils.validateXMLFile(siteResourceFile, 
//					"../src/main/resources/schema/site-resources.xsd");
		
			final Units units = Units.getInstance();
			final long energyScalingFactor = units.getEnergyScalingFactor(Utils
				.doXPathStrQuery(siteResourceFile,
					"/snee:site-resources/snee:units/snee:energy"));
			final long memoryScalingFactor = units.getMemoryScalingFactor(Utils
				.doXPathStrQuery(siteResourceFile,
					"/snee:site-resources/snee:units/snee:memory"));
			final long defaultEnergyStock = energyScalingFactor
				* Long
					.parseLong(Utils
						.doXPathStrQuery(siteResourceFile,
							"/snee:site-resources/snee:sites/snee:default/snee:energy-stock"));
			final long defaultRAM = memoryScalingFactor
				* Long
					.parseLong(Utils
						.doXPathStrQuery(siteResourceFile,
							"/snee:site-resources/snee:sites/snee:default/snee:ram"));
		
			final Iterator<Node> siteIter = sensornet.getNodes().iterator();
			while (siteIter.hasNext()) {
		
			    final Site site = (Site) siteIter.next();
			    final String energyStockStr = Utils.doXPathStrQuery(
				    siteResourceFile,
				    "/snee:site-resources/snee:sites/snee:site[@id="
					    + site.getID() + "]/snee:energy-stock");
			    long energyStock;
			    if (energyStockStr == null) {
				energyStock = defaultEnergyStock;
			    } else {
				energyStock = Long.parseLong(energyStockStr)
					* energyScalingFactor;
			    }
			    site.setEnergyStock(energyStock);
		
			    final String ramStr = Utils.doXPathStrQuery(siteResourceFile,
				    "/snee:site-resources/snee:sites/snee:site[@id="
					    + site.getID() + "]/snee:ram");
			    long ram;
			    if (ramStr == null) {
				ram = defaultRAM;
			    } else {
				ram = Long.parseLong(ramStr) * memoryScalingFactor;
			    }
			    site.setRAM(ram);
		
			    //TODO: in future, flash memory as well...
			}
    	} catch (Exception e) {
    		throw new TopologyReaderException(e.getMessage());
    	}

    }

}
