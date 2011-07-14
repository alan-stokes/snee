package uk.ac.manchester.cs.snee.metadata.source;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.datasource.webservice.PullSourceWrapper;
import uk.ac.manchester.cs.snee.datasource.webservice.PullSourceWrapperImpl;
import uk.ac.manchester.cs.snee.datasource.webservice.SourceWrapper;
import uk.ac.manchester.cs.snee.datasource.webservice.WSDAIRSourceWrapperImpl;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.Types;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.sncb.SNCB;
import uk.ac.manchester.cs.snee.sncb.SNCBException;
import uk.ac.manchester.cs.snee.sncb.SensorType;

public class SourceManager {

	private Logger logger = 
		Logger.getLogger(this.getClass().getName());

	/**
	 * Metadata about the sources
	 */
	private List<SourceMetadataAbstract> _sources = 
		new ArrayList<SourceMetadataAbstract>();

	private Map<String, ExtentMetadata> _schema;

	private Types _types;

	private Map<String, SensorType> _sensorTypes = 
		new HashMap<String, SensorType>();
	
	public SourceManager(Map<String, ExtentMetadata> schema, Types types) {
		_schema = schema;
		_types = types;
	}

	/**
	 * Read in a physical schema file to instantiate the available
	 * data sources.
	 * 
	 * @param physicalSchemaFile
	 * @throws MetadataException
	 * @throws SourceMetadataException
	 * @throws TopologyReaderException
	 * @throws MalformedURLException
	 * @throws SNEEDataSourceException
	 * @throws SchemaMetadataException
	 * @throws TypeMappingException
	 */
	public void processPhysicalSchema(String physicalSchemaFile, SNCB sncb) 
	throws MetadataException, SourceMetadataException, 
	TopologyReaderException, MalformedURLException,
	SNEEDataSourceException, SchemaMetadataException, 
	TypeMappingException, SNCBException, SNEEConfigurationException 
	{
		if (logger.isTraceEnabled())
			logger.trace("ENTER processPhysicalSchema() with " +
					physicalSchemaFile);
		Document physicalSchemaDoc = parseFile(physicalSchemaFile);
		Element root = (Element) physicalSchemaDoc.getFirstChild();
		logger.trace("Root:" + root);
		addSensorNetworkSources(root.getElementsByTagName("sensor_network"), sncb);
		addUdpSources(root.getElementsByTagName("udp_source"));
		addServiceSources(root.getElementsByTagName("service_source"));
		if (logger.isInfoEnabled())
			logger.info("Physical schema successfully read in from " + 
					physicalSchemaFile + ". Number of sources=" + 
					_sources.size());		
		if (logger.isTraceEnabled())
			logger.trace("RETURN processPhysicalSchema() #sources=" + 
					_sources.size());
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

	private void addSensorNetworkSources(NodeList wsnSources, SNCB sncb) 
	throws MetadataException, SourceMetadataException, 
	TopologyReaderException, SNCBException, SNEEConfigurationException {
		//FIXME: This should be abstracted out into a data source module for sensor networks
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER addSensorNetworkSources() #=" + 
					wsnSources.getLength());
			logger.trace("Create sensor network sources");
		}			
		for (int i = 0; i < wsnSources.getLength(); i++) {
			Element wsnElem = (Element) wsnSources.item(i);
			NamedNodeMap attrs = wsnElem.getAttributes();
			String sourceName = attrs.getNamedItem("name").getNodeValue();
			if (logger.isTraceEnabled()) {
				logger.trace("WSN sourceName="+sourceName);
			}
			Node topologyElem = wsnElem.getElementsByTagName("topology").
			item(0).getFirstChild();
			String topologyFile = Utils.getResourcePath(topologyElem.getNodeValue());
			if (logger.isTraceEnabled()) {
				logger.trace("topologyFile="+topologyFile);
			}
			Node resElem = wsnElem.getElementsByTagName(
				"site-resources").item(0).getFirstChild();
			String resFile = Utils.getResourcePath(resElem.getNodeValue());
			if (logger.isTraceEnabled()) {
				logger.trace("resourcesFile="+resFile);
			}
			Node gatewaysElem = wsnElem.getElementsByTagName("gateways").
			item(0).getFirstChild();
			if (logger.isTraceEnabled()) {
				logger.trace("gateway="+gatewaysElem.getNodeValue());
			}
			int[] gateways = SourceMetadataUtils.convertNodes(
					gatewaysElem.getNodeValue());
			List<String> extentNames = new ArrayList<String>();
			Element extentsElem = parseSensorNetworkExtentNames(wsnElem,
					extentNames);
			SourceMetadataAbstract source = new SensorNetworkSourceMetadata(
					sourceName, extentNames, extentsElem, topologyFile, 
					resFile, gateways, sncb);
			_sources.add(source);
			
			parseAttributeSensorTypes(wsnElem);
			
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN addSensorNetworkSources()");
	}

	private void parseAttributeSensorTypes(Element wsnElem) 
	throws MetadataException {
		NodeList extentElems = wsnElem.getElementsByTagName("extent");
		for (int j=0; j<extentElems.getLength(); j++) {
			Element extentElem = (Element)extentElems.item(j);
			String extentName = ""+extentElem.getAttributeNode("name").getNodeValue().toLowerCase();
			if (!_schema.containsKey(extentName)) {
				throw new MetadataException("Physical schema refers "+
						"to extent '"+extentName+"' which is not "+
				"present in the logical schema.");
			}
			ExtentMetadata em = this._schema.get(extentName);
			
			NodeList sensorAttrsElems = extentElem.getElementsByTagName("attribute");
			for (int k=0; k<sensorAttrsElems.getLength(); k++) {
				Element attrSensorTypeElem = (Element)sensorAttrsElems.item(k);
				
				String attrName = ""+attrSensorTypeElem.getAttributeNode("name").getNodeValue().toLowerCase();
				if (!em.hasAttribute(attrName)) {
					throw new MetadataException("Physical schema refers "+
							"to attribute '"+attrName+"' in extent '"+extentName+
							"' which is not present in the logical schema.");					
				}
				Attribute attr = em.getAttribute(attrName);
				
				String attrSensorType = ""+attrSensorTypeElem.getAttributeNode("sensorType").getNodeValue().toLowerCase();
				SensorType sensorType = SensorType.parseSensorType(attrSensorType);
				if (sensorType == null) {
					throw new MetadataException("Physical schema refers "+
							"to invalid sensor type '"+attrSensorType+
							"' for attribute '"+attrName+"' in extent '"+extentName+
							"'.");
				}
				
				_sensorTypes.put(generateAttributeKey(attr), sensorType);
			}
		}
	}

	private Element parseSensorNetworkExtentNames(Element wsnElem,
			List<String> extentNames) throws MetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER parseSensorNetworkExtentNames() ");
		Element extentsElem = (Element) wsnElem.
		getElementsByTagName("extents").item(0);
		extentNames.addAll(parseExtents(extentsElem));
		for (String extentName : extentNames) {
			if (!_schema.containsKey(extentName)) {
				throw new MetadataException("Physical schema refers "+
						"to extent '"+extentName+"' which is not "+
				"present in the logical schema.");
			}
			ExtentMetadata em =_schema.get(extentName);
			if (em.getExtentType()!=ExtentType.SENSED) {
				throw new MetadataException(extentName+" is has extent " +
						"type "+em.getExtentName()+", and therefore "+
						"cannot use a sensor network capable of "+
				"in-network processing as a data source.");
			}
		}
		logger.trace("extentNames="+extentNames.toString());
		if (logger.isTraceEnabled())
			logger.trace("RETURN parseSensorNetworkExtentNames() ");
		return extentsElem;
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

	private void addServiceSources(NodeList serviceSources) 
	throws SourceMetadataException, MalformedURLException,
	SNEEDataSourceException, SchemaMetadataException, 
	TypeMappingException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER addServiceSources() #sources=" + 
					serviceSources.getLength());
		}
		for (int i = 0; i < serviceSources.getLength(); i++) {
			logger.trace("Create service source");
			Node serviceNode = serviceSources.item(i);
			NamedNodeMap attrs = serviceNode.getAttributes();
			String sourceName = 
				attrs.getNamedItem("name").getNodeValue();
			NodeList nodes = serviceNode.getChildNodes();
			String epr = "";
			SourceType interfaceType = null;
			for (int j = 0; j < nodes.getLength(); j++) {
				Node node = nodes.item(j);
				if (node.getNodeName().equalsIgnoreCase("interface-type")) {
					String it = node.getFirstChild().getNodeValue();
					if (it.equals("pull-stream")) {
						interfaceType = SourceType.PULL_STREAM_SERVICE;
					} else if (it.equals("push-stream")) {
						interfaceType = SourceType.PUSH_STREAM_SERVICE;
					} else if (it.equals("wsdair")) {
						interfaceType = SourceType.WSDAIR;
					} else {
						String message = 
							"Unsupported interface type " + it;
						logger.warn(message);
						throw new SourceMetadataException(message);
					}
					if (logger.isTraceEnabled())
						logger.trace("Interface type:" + interfaceType);
				} else if (node.getNodeName().equalsIgnoreCase(
						"endpoint-reference")) {					
					epr = node.getFirstChild().getNodeValue();
				}
			}
			createServiceSource(interfaceType, epr, sourceName);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN addServiceSources() #sources=" +
					_sources.size());
		}
	}

	/**
	 * Adds a web service source. The schema of the source is read
	 * and added to the logical schema. The source is added to the set
	 * of data sources.
	 * @param name name used to refer to the source
	 * @param url URL for the web service interface 
	 * @param sourceType the service type of the interface
	 * @throws MalformedURLException invalid url passed to method
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws SNEEDataSourceException 
	 * @throws SourceMetadataException 
	 */
	public void addServiceSource(String name, String url, 
			SourceType sourceType) 
	throws MalformedURLException, SNEEDataSourceException, 
	SchemaMetadataException, TypeMappingException,
	SourceMetadataException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER addServiceSource() with name=" +
					name + " sourceType=" + sourceType + 
					" epr= "+ url);
		createServiceSource(sourceType, url, name);
		if (logger.isInfoEnabled())
			logger.info("Web service successfully added from " + 
					url + ". Number of extents=" + _schema.size());
		if (logger.isDebugEnabled()) {
			logger.debug("Available extents:\n\t" + _schema.keySet());
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN addServiceSource() #extents=" +
					_schema.size() + " #sources=" + _sources.size());
	}

	private void createServiceSource(SourceType interfaceType,
			String epr, String serviceName) 
	throws SourceMetadataException, MalformedURLException,
	SNEEDataSourceException, SchemaMetadataException, 
	TypeMappingException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER createServiceSource() with name=" +
					serviceName + " type=" + interfaceType + " epr=" +
					epr);
		}
		SourceWrapper sourceWrapper;
		switch (interfaceType) {
		case PULL_STREAM_SERVICE:
			sourceWrapper = createPullSource(epr);
			break;
		case PUSH_STREAM_SERVICE:
			String message = 
				"Push-stream services are not currently supported.";
			logger.warn(message);
			throw new SourceMetadataException(message);
		case WSDAIR:
			sourceWrapper = createWSDAIRSource(epr);
			break;
		default:
			message = 
				"Unknown interface type.";
			logger.warn(message);
			throw new SourceMetadataException(message);
		}
		createServiceSourceMetadata(serviceName, epr, sourceWrapper);
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN createServiceSource()");
		}
	}

	protected PullSourceWrapper createPullSource(String url)
	throws MalformedURLException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER createPullSource() with " + url);
		}
		PullSourceWrapper pullClient = 
			new PullSourceWrapperImpl(url, _types);
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN createPullSource()");
		}
		return pullClient;
	}

	protected WSDAIRSourceWrapperImpl createWSDAIRSource(String url)
	throws MalformedURLException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER createWSDAIRSource() with " + url);
		}
		WSDAIRSourceWrapperImpl wsdairClient = 
			new WSDAIRSourceWrapperImpl(url, _types);
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN createWSDAIRSource()");
		}
		return wsdairClient;
	}

	private void createServiceSourceMetadata(
			String sourceName,
			String url, SourceWrapper sourceWrapper) 
	throws SNEEDataSourceException, SchemaMetadataException,
	TypeMappingException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER createServiceSourceMetadata() with " +
					"name=" + sourceName +
					" url=" + url + " sourceWrapper=" + sourceWrapper);
		}
		List<String> resources = sourceWrapper.getResourceNames();
		List<String> extentNames = new ArrayList<String>();
		//FIXME: Read stream rate from property document!
		Map<String, String> resourcesByExtent = 
			new HashMap<String, String>();
		for (String resource : resources) {
			if (logger.isTraceEnabled())
				logger.trace("Retrieving schema for " + resource);
			List<ExtentMetadata> extents = 
				sourceWrapper.getSchema(resource);
			//TODO: Should really qualify extent names with the source!
			for (ExtentMetadata extent : extents) {
				String extentName = extent.getExtentName();
				if (_schema.containsKey(extentName)) {
					logger.warn("Extent " + extentName + 
							" already exists in logical schema.");
				} else {
					_schema.put(extentName, extent);
					extentNames.add(extentName);
					resourcesByExtent.put(extentName, resource);
					if (logger.isTraceEnabled()) {
						logger.trace("Added extent " + extentName + 
								" of extent type " + extent.getExtentType());
					}
				}
			}
		}
		WebServiceSourceMetadata source = 
			new WebServiceSourceMetadata(sourceName, extentNames, url, 
					resourcesByExtent, sourceWrapper);
		_sources.add(source);
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN createServiceSourceMetadata()");
		}
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
			SourceMetadataAbstract source = 
				new UDPSourceMetadata(sourceName, extentNames, hostName,
						extentNodes, _schema);
			_sources.add(source);
		}
		if (logger.isTraceEnabled()) 
			logger.trace("RETURN addUdpSources() #sources=" + _sources.size());
	}

	/**
	 * Return the number of sources.
	 * 
	 * @return number of available sources
	 */
	public int size() {
		return _sources.size();
	}
	
	/**
	 * Retrieve the details of all data sources.
	 * 
	 * @return details of all data sources
	 */
	public List<SourceMetadataAbstract> getSources() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER/RETURN getSources() #sources=" + 
					_sources.size());
		return _sources;
	}

	/**
	 * Retrieve details of the sources that publish data for the 
	 * given extent.
	 * 
	 * @param extentName the name of the extent to find the sources for
	 * @return details of the sources
	 * @throws SourceDoesNotExistException no data source found for the extent
	 */
	public SourceMetadataAbstract getSource(String extentName) 
	throws SourceDoesNotExistException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getSource() for " + extentName);
		if (logger.isTraceEnabled()) {
			logger.trace("Number of sources: " + _sources.size());
		}
		for (SourceMetadataAbstract source : _sources) {
			if (logger.isTraceEnabled()) {
				logger.trace(source);
//				logger.trace(source + ": " + source.getExtentNames());
			}
			if (source.getExtentNames().contains(extentName)) {
				if (logger.isDebugEnabled()) {
					logger.debug("RETURN getSource() source=" + 
							source.getSourceName());
				}
				return source;
			}
		}

		/* FIXME: If the extent is intensional, we should NOT expect to find a source */

		String msg = "No source found for extent " + extentName;
		logger.warn(msg);
		throw new SourceDoesNotExistException(msg);
	}
	
	/**
	 * Given an attribute, returns the corresponding sensor type it is mapped to.
	 * @param attr
	 * @return
	 */
	public SensorType getAttributeSensorType(Attribute attr) {
		return this._sensorTypes.get(generateAttributeKey(attr));
	}
	
	private String generateAttributeKey(Attribute attr) {
		return attr.getExtentName()+ "_"+ attr.getAttributeSchemaName();
	}
}
