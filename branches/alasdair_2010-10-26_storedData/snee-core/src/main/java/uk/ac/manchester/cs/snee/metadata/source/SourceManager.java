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
import uk.ac.manchester.cs.snee.datasource.webservice.PullSourceWrapper;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.Types;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;

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
	public void processPhysicalSchema(String physicalSchemaFile) 
	throws MetadataException, SourceMetadataException, 
	TopologyReaderException, MalformedURLException,
	SNEEDataSourceException, SchemaMetadataException, 
	TypeMappingException 
	{
		if (logger.isTraceEnabled())
			logger.trace("ENTER processPhysicalSchema() with " +
					physicalSchemaFile);
		Document physicalSchemaDoc = parseFile(physicalSchemaFile);
		Element root = (Element) physicalSchemaDoc.getFirstChild();
		logger.trace("Root:" + root);
		addSensorNetworkSources(root.getElementsByTagName("sensor_network"));
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

	private void addSensorNetworkSources(NodeList wsnSources) 
	throws MetadataException, SourceMetadataException, 
	TopologyReaderException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER addSensorNetworkSources() #=" + 
					wsnSources.getLength());
		logger.trace("Create sensor network sources");
		for (int i = 0; i < wsnSources.getLength(); i++) {
			Element wsnElem = (Element) wsnSources.item(i);
			NamedNodeMap attrs = wsnElem.getAttributes();
			String sourceName = attrs.getNamedItem("name").getNodeValue();
			logger.trace("WSN sourceName="+sourceName);
			Node topologyElem = wsnElem.getElementsByTagName("topology").
			item(0).getFirstChild();
			String topologyFile = topologyElem.getNodeValue();
			logger.trace("topologyFile="+topologyFile);
			Node resElem = wsnElem.getElementsByTagName(
			"site-resources").item(0).getFirstChild();
			String resFile = resElem.getNodeValue();
			logger.trace("resourcesFile="+resFile);
			Node gatewaysElem = wsnElem.getElementsByTagName("gateways").
			item(0).getFirstChild();
			logger.trace("gateway="+gatewaysElem.getNodeValue());
			int[] gateways = SourceMetadataUtils.convertNodes(
					gatewaysElem.getNodeValue());
			List<String> extentNames = new ArrayList<String>();
			Element extentsElem = parseSensorNetworkExtentNames(wsnElem,
					extentNames);
			SourceMetadataAbstract source = new SensorNetworkSourceMetadata(
					sourceName, extentNames, extentsElem, topologyFile, 
					resFile, gateways);
			_sources.add(source);
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN addSensorNetworkSources()");
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
	 * @param name TODO
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
		createServiceSource(sourceType, url, "");
		if (logger.isInfoEnabled())
			logger.info("Web service successfully added from " + 
					url + ". Number of extents=" + _schema.size());
		if (logger.isTraceEnabled()) {
			logger.trace("Available extents:\n\t" + _schema.keySet());
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
		PullSourceWrapper sourceWrapper;
		switch (interfaceType) {
		case PULL_STREAM_SERVICE:
			sourceWrapper = createPullSource(epr);
			break;
		case PUSH_STREAM_SERVICE:
			String message = 
				"Push-stream services are not currently supported.";
			logger.warn(message);
			throw new SourceMetadataException(message);
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
		PullSourceWrapper pullClient = new PullSourceWrapper(url, _types);
		return pullClient;
	}

	private void createServiceSourceMetadata(
			String sourceName,
			String url, PullSourceWrapper pullSource) 
	throws SNEEDataSourceException, SchemaMetadataException,
	TypeMappingException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER createServiceSourceMetadata() with " +
					"name=" + sourceName +
					" url=" + url + " sourceWrapper=" + pullSource);
		}
		List<String> resources = pullSource.getResourceNames();
		List<String> extentNames = new ArrayList<String>();
		//FIXME: Read stream rate from property document!
		Map<String, String> resourcesByExtent = 
			new HashMap<String, String>();
		for (String resource : resources) {
			if (logger.isTraceEnabled())
				logger.trace("Retrieving schema for " + resource);
			List<ExtentMetadata> extents = 
				pullSource.getSchema(resource);
			for (ExtentMetadata extent : extents) {
				String extentName = extent.getExtentName();
				_schema.put(extentName, extent);
				extentNames.add(extentName);
				resourcesByExtent.put(extentName, resource);
				if (logger.isTraceEnabled()) {
					logger.trace("Added extent " + extentName + 
							" of extent type " + extent.getExtentType());
				}
			}
		}
		WebServiceSourceMetadata source = 
			new WebServiceSourceMetadata(sourceName, extentNames, url, 
					resourcesByExtent, pullSource);
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
				new UDPSourceMetadata(sourceName, extentNames, hostName, extentNodes);
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
	 */
	public List<SourceMetadataAbstract> getSources(String extentName) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getSources() for " + extentName);
		List<SourceMetadataAbstract> extentSources = 
			new ArrayList<SourceMetadataAbstract>();
		for (SourceMetadataAbstract source : _sources) {
			logger.trace(source.getExtentNames());
			if (source.getExtentNames().contains(extentName))
				extentSources.add(source);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN getSources() #sources=" + 
					extentSources.size());
		return extentSources;
	}
	
}
