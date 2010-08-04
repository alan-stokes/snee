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

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import uk.ac.manchester.cs.snee.common.Constants;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentDoesNotExistException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentType;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.Types;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceMetadata;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.UDPSourceMetadata;
import uk.ac.manchester.cs.snee.compiler.metadata.source.WebServiceSourceMetadata;
import uk.ac.manchester.cs.snee.data.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.data.webservice.PullSourceWrapper;

public class Metadata {

	private Logger logger = 
		Logger.getLogger(Metadata.class.getName());

	/**
	 * Details of the logical schema. Extent name used as the
	 * key by which to extract schema metadata items
	 */
	private Map<String, ExtentMetadata> _schema =
		new HashMap<String, ExtentMetadata>();

	/**
	 * Metadata about the sources
	 */
	private List<SourceMetadata> _sources = 
		new ArrayList<SourceMetadata>();

	private Types _types;

	private AttributeType timeType;

	private AttributeType idType;

	/**
	 * Metadata about logical and physical schema
	 * @throws TypeMappingException
	 * @throws SchemaMetadataException
	 * @throws SNEEConfigurationException Problem reading configuration from properties file
	 * @throws UnsupportedAttributeTypeException 
	 * @throws MetadataException 
	 * @throws SourceMetadataException 
	 */
	//FIXME: Check what circumstances each of these exceptions is thrown
	public Metadata() 
	throws TypeMappingException, SchemaMetadataException, SNEEConfigurationException, MetadataException, UnsupportedAttributeTypeException, SourceMetadataException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER Metadata()");
		String typesFile = SNEEProperties.getFilename(SNEEPropertyNames.INPUTS_TYPES_FILE);
		_types = new Types(typesFile);
		timeType = _types.getType(Constants.TIME_TYPE);
		timeType.setSorted(true);
		idType = _types.getType(Constants.ID_TYPE);
		if (SNEEProperties.isSet(SNEEPropertyNames.INPUTS_LOGICAL_SCHEMA_FILE)) {
			processLogicalSchema(
					SNEEProperties.getFilename(SNEEPropertyNames.INPUTS_LOGICAL_SCHEMA_FILE));
		}
		if (SNEEProperties.isSet(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE)) {
			processPhysicalSchema(
					SNEEProperties.getFilename(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE));
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN Metadata()");
	}

	private void processLogicalSchema(String logicalSchemaFile) 
	throws SchemaMetadataException, MetadataException, 
	TypeMappingException, UnsupportedAttributeTypeException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER processLogicalSchema() with " +
					logicalSchemaFile);
		Document doc = parseFile(logicalSchemaFile);
		Element root = (Element) doc.getFirstChild();
		/* Process stream extents */
		NodeList xmlSources = root.getElementsByTagName("stream");
		for (int i = 0; i < xmlSources.getLength(); i++) {
			Element element = (Element) xmlSources.item(i);
			String typeAttr = element.getAttribute("type");
			if (typeAttr.equals("pull")) {				
				/* Process pull streams */
				addExtent(ExtentType.SENSED, element);
			} else if (typeAttr.equals("push")) {
				/* Process push streams */
				addExtent(ExtentType.PUSHED, element);
			}
		}				
		/* Process stored relations */
		xmlSources = root.getElementsByTagName("table");
		for (int i = 0; i < xmlSources.getLength(); i++) {
			Element element = (Element) xmlSources.item(i);
			addExtent(ExtentType.TABLE, element);
		}				
		/* Log a warning for the use of the old declaration of stream */
		xmlSources = root.getElementsByTagName("stream");
		if (xmlSources.getLength() > 0) {
			logger.warn("Old schema declaration used for stream. " +
					"You should update your schema to use " +
			"push_stream or pull_stream as appropriate.");
		}	
		if (logger.isInfoEnabled())
			logger.info("Logical schema successfully read in from " + 
					logicalSchemaFile + ". Number of extents=" + 
					_schema.size() + "\n\tExtents: " + _schema.keySet());
		if (logger.isTraceEnabled()) {
			logger.trace("Available extents:\n\t" + _schema.keySet());
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN processLogicalSchema() #extents=" + 
					_schema.size());
	}
	
	/**
	 * Add a given extent object to the schema 
	 * @param extent the object that represents the extent
	 * @throws MetadataException extent with the same name exists
	 */
	public void addExtent(ExtentMetadata extent) 
	throws MetadataException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER addExtent() with " + extent);
		String extentName = extent.getExtentName();
		if (_schema.containsKey(extentName)) {
			String message = "Extent " + extentName + " already exists.";
			logger.warn(message);
			throw new MetadataException(message);
		}
		_schema.put(extentName, extent);
		if (logger.isTraceEnabled()) {
			logger.trace("Extent " + extentName + 
				" successfully added to the schema.");
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN addExtent()");
	}

	/**
	 * Generate an ExtentMetadata object to represent logical extent
	 * 
	 * @param extentType Type of data source extent: sensed, pushed, or table 
	 * @param element XML element for the source metadata 
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	private void addExtent(ExtentType extentType, Element element) 
	throws TypeMappingException, SchemaMetadataException 
	{
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER addExtent() of type " + extentType);
		}
		String extentName = element.getAttribute("name").toLowerCase();
		Map<String, AttributeType> attributes = 
			parseAttributes(element.getElementsByTagName("column"));
		if (extentType == ExtentType.SENSED) {
			attributes.put("id", idType);
			attributes.put("time", timeType);
		}
		ExtentMetadata extent =
			new ExtentMetadata(extentName, attributes, extentType);
		_schema.put(extentName, extent);
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN addExtent() " + extentName);
		}
	}

	private Map<String, AttributeType> parseAttributes(NodeList columns) 
	throws TypeMappingException, SchemaMetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER parseAttributes() number of columns " + 
					columns.getLength());
		Map<String,AttributeType> attributes = 
			new HashMap<String, AttributeType>();
		for (int i = 0; i < columns.getLength(); i++) {
			Element tabElement = (Element) columns.item(i);
			String attributeName = 
				tabElement.getAttribute("name").toLowerCase();
			if (logger.isTraceEnabled()) 
				logger.trace("Reading attribute " + attributeName);
			NodeList xmlType = tabElement.getElementsByTagName("type");
			String sqlType = 
				((Element) xmlType.item(0)).getAttribute("class").toLowerCase();
			if (logger.isTraceEnabled()) 
				logger.trace("sqlType = " + sqlType);
			AttributeType type = _types.getType(sqlType);
			if (logger.isTraceEnabled()) 
				logger.trace("Type = " + type);
			type.setLength(((Element) xmlType.item(0)).getAttribute("length"));
			attributes.put(attributeName, type);
		}
		if (logger.isTraceEnabled()) 
			logger.trace("RETURN parseAttributes(), #attr=" + 
					attributes.size());
		return attributes;
	}

	private void processPhysicalSchema(String physicalSchemaFile) 
	throws MetadataException, SourceMetadataException 
	{
		if (logger.isTraceEnabled())
			logger.trace("ENTER processPhysicalSchema() with " +
					physicalSchemaFile);
		Document physicalSchemaDoc = parseFile(physicalSchemaFile);
		Element root = (Element) physicalSchemaDoc.getFirstChild();
		logger.trace("Root:" + root);
		addSensorNetworkSources(root.getElementsByTagName("sensor_network"));
		addUdpSources(root.getElementsByTagName("udp_source"));
		if (logger.isInfoEnabled())
			logger.info("Physical schema successfully read in from " + 
					physicalSchemaFile + ". Number of sources=" + 
					_sources.size());		
		if (logger.isTraceEnabled())
			logger.trace("RETURN processPhysicalSchema() #sources=" + 
					_sources.size());
	}

		private void addSensorNetworkSources(NodeList wsnSources) 
		throws MetadataException {
			if (logger.isTraceEnabled())
				logger.trace("ENTER addSensorNetworkSources() #=" + 
						wsnSources.getLength());
			logger.trace("Create sensor network source");
			for (int i = 0; i < wsnSources.getLength(); i++) {
				logger.trace("Create Sensor Network Source");
				SourceMetadata source =
					new SensorNetworkSourceMetadata(sourceName, extentNames, xml);
				_sources.add(source);
			}
			if (logger.isTraceEnabled())
				logger.trace("RETURN addSensorNetworkSources()");
		}

	private void addUdpSources(NodeList udpSources) 
	throws SourceMetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER addUdpSources() #sources=" + udpSources.getLength());
		for (int i = 0; i < udpSources.getLength(); i++) {
			logger.trace("Create udp source");
			Node udpNode = udpSources.item(i);
			NamedNodeMap attrs = udpNode.getAttributes();
			String sourceName = attrs.getNamedItem("name").getNodeValue();
			NodeList nodes = udpNode.getChildNodes();
			List<String> extentNames = new ArrayList<String>();
			String hostName = "";
			Element extentNodes = null;
			for (int j = 0; j < nodes.getLength(); j++) {
				Node node = nodes.item(j);
				if (node.getNodeName().equalsIgnoreCase("host")) {
					hostName = node.getFirstChild().getNodeValue();
					if (logger.isTraceEnabled())
						logger.trace("Host name:" + hostName);
				} else if (node.getNodeName().equalsIgnoreCase("extents")) {					
					extentNodes = (Element) node.getChildNodes();
					extentNames.addAll(parseExtents(extentNodes));
				}
			}
			SourceMetadata source = 
				new UDPSourceMetadata(sourceName, extentNames, hostName, extentNodes);
			_sources.add(source);
		}
		if (logger.isTraceEnabled()) 
			logger.trace("RETURN addUdpSources() #sources=" + _sources.size());
	}

	private List<String> parseExtents(Element extentsElement) {
		if (logger.isTraceEnabled())
			logger.trace("ENTER parseExtents()");
		List<String> extentNames = new ArrayList<String>();
		NodeList extentNodes = extentsElement.getElementsByTagName("extent");
		for (int i = 0; i < extentNodes.getLength(); i++) {
			Node node = extentNodes.item(i);
			if (node.getNodeName().equalsIgnoreCase("extent")) {
				NamedNodeMap attrs = node.getAttributes();
				String name = 
					attrs.getNamedItem("name").getNodeValue().toLowerCase();
				if (logger.isTraceEnabled())
					logger.trace("Adding extent name: " + name);
				extentNames.add(name);
			}
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN parseExtents() #extents=" + 
					extentNames.size());
		return extentNames;
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

	private Document parseFile(String schemaFile) 
	throws MetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER parseFile() with filename " + schemaFile);
		Document document;
		try {
			DocumentBuilderFactory factory = 
				DocumentBuilderFactory.newInstance();
			DocumentBuilder builder;
			builder = factory.newDocumentBuilder();
			document = builder.parse(schemaFile);
		} catch (ParserConfigurationException e) {
			String msg = "Unable to configure document parser.";
			logger.warn(msg);
			throw new MetadataException(msg, e);
		} catch (SAXException e) {
			String msg = "Unable to parse schema.";
			logger.warn(msg + e);
			throw new MetadataException(msg, e);
		} catch (IOException e) {
			String msg = "Unable to access schema file " + 
			schemaFile + ".";
			logger.warn(msg);
			throw new MetadataException(msg, e);
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN parseFile()");
		return document;
	}

	/**
	 * Adds a web service source. The schema of the source is read
	 * and added to the logical schema. The source is added to the set
	 * of data sources.
	 * @param url URL for the web service interface 
	 * @throws MalformedURLException invalid url passed to method
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws SNEEDataSourceException 
	 */
	public void addWebServiceSource(String url) 
	throws MalformedURLException, SNEEDataSourceException, 
	SchemaMetadataException, TypeMappingException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER addWebServiceSource() with " + url);
		PullSourceWrapper pullSource = createPullSource(url);
		List<String> resources = pullSource.getResourceNames();
		List<String> extentNames = new ArrayList<String>();
		//FIXME: Why do I have 44 resources but only 43 extents?
		//FIXME: Read stream rate from property document!
		Map<String, String> resourcesByExtent = 
			new HashMap<String, String>();
		for (String resource : resources) {
			if (logger.isTraceEnabled())
				logger.trace("Retrieving schema for " + resource);
			ExtentMetadata extent = 
				pullSource.getSchema(resource);
			String extentName = extent.getExtentName();
			_schema.put(extentName, extent);
			extentNames.add(extentName);
			resourcesByExtent.put(extentName, resource);
			if (logger.isTraceEnabled())
				logger.trace("Added extent " + extentName + 
						" of extent type " + extent.getExtentType());
		}
		WebServiceSourceMetadata source = 
			new WebServiceSourceMetadata("", extentNames, url, 
					resourcesByExtent, pullSource);
		_sources.add(source);
		if (logger.isInfoEnabled())
			logger.info("Web service successfully added from " + 
					url + ". Number of extents=" + _schema.size());
		if (logger.isTraceEnabled()) {
			logger.trace("Available extents:\n\t" + _schema.keySet());
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN addWebServiceSource() #extents=" +
					_schema.size() + " #sources=" + _sources.size());
	}

	protected PullSourceWrapper createPullSource(String url)
	throws MalformedURLException {
		PullSourceWrapper ccoClient = new PullSourceWrapper(url, _types);
		return ccoClient;
	}

	/**
	 * Retrieve the metadata about a particular extent
	 * 
	 * @param name Name of the extent
	 * @return Metadata about the extent
	 * @throws ExtentDoesNotExistException The given name does not exist
	 */
	public ExtentMetadata getExtentMetadata(String name)
	throws ExtentDoesNotExistException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getExtentMetadata() with name " + name);
		ExtentMetadata schemaMetadata;
		if (_schema.containsKey(name.toLowerCase())) {
			schemaMetadata = _schema.get(name.toLowerCase());
		} else {
			String msg = "Extent " + name + " not found in Schema";
			logger.warn(msg);
			throw new ExtentDoesNotExistException(msg);
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN getExtentMetadata()");
		}
		return schemaMetadata;
	}

	public List<ExtentMetadata> getAcquireExtents() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getAcquireExtents()");
		List<ExtentMetadata> result = getExtentsOfType(ExtentType.SENSED);
		if (logger.isDebugEnabled())
			logger.debug("RETURN getAcquireExtents() " +
					"number of sensed sources: " + result.size());
		return result;
	}

	public List<ExtentMetadata> getPushedExtents() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getPushedExtents()");
		List<ExtentMetadata> result = getExtentsOfType(ExtentType.PUSHED);
		if (logger.isDebugEnabled())
			logger.debug("RETURN getPushedExtents() " +
					"number of pushed sources: " + result.size());
		return result;
	}

	public List<ExtentMetadata> getStoredExtents() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getStoredExtents()");
		List<ExtentMetadata> result = getExtentsOfType(ExtentType.TABLE);
		if (logger.isDebugEnabled())
			logger.debug("RETURN getStoredExtents() " +
					"number of stored sources: " + result.size());
		return result;
	}

	private List<ExtentMetadata> getExtentsOfType(ExtentType extentType) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getExtentsOfType() " + extentType);
		List<ExtentMetadata> result = new ArrayList<ExtentMetadata>(); 
		Set<String> extentNames = _schema.keySet();
		for (String name : extentNames) {
			ExtentMetadata schema = _schema.get(name);
			if (schema.getExtentType() == extentType) 
				result.add(schema);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN getExtentsOfType() " +
					"number of extents: " + result.size());
		return result;
	}

	public Types getTypes() {
		return _types;
	}

	public List<SourceMetadata> getSources(String extentName) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getSources() for " + extentName);
		List<SourceMetadata> extentSources = 
			new ArrayList<SourceMetadata>();
		for (SourceMetadata source : _sources) {
			logger.trace(source.getExtentNames());
			if (source.getExtentNames().contains(extentName))
				extentSources.add(source);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN getSources() #sources=" + 
					extentSources.size());
		return extentSources;
	}

	/**
	 * Return the names of the available extents
	 * @return
	 */
	public Collection<String> getExtentNames() {
		return _schema.keySet();
	}

}