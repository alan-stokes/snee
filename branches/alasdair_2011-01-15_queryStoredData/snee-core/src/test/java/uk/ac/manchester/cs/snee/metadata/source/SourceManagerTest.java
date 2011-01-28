package uk.ac.manchester.cs.snee.metadata.source;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.easymock.classextension.EasyMockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.datasource.webservice.PullSourceWrapper;
import uk.ac.manchester.cs.snee.datasource.webservice.WSDAIRSourceWrapperImpl;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.Types;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.sncb.SNCBException;

public class SourceManagerTest extends EasyMockSupport {

	private Properties props;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				SourceManagerTest.class.getClassLoader().getResource(
					"etc/log4j.properties"));
	}

	@Before
	public void setUp() throws Exception {
		props = new Properties();
		props.setProperty(SNEEPropertyNames.INPUTS_TYPES_FILE, "etc/Types.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_UNITS_FILE, "etc/units.xml");
		props.setProperty(SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR, "output");
		props.setProperty(SNEEPropertyNames.SNCB_PERFORM_METADATA_COLLECTION, "false");
		props.setProperty(SNEEPropertyNames.SNCB_GENERATE_COMBINED_IMAGE, "false");
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test(expected=MalformedURLException.class)
	public void testAddWebServiceSource_invalidURL() 
	throws TypeMappingException, SchemaMetadataException, 
	MalformedURLException, SNEEDataSourceException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException, 
	TopologyReaderException, CostParametersException, SNCBException 
	{
		SourceManager schema = new SourceManager(null, null);
		schema.addServiceSource("bad url", "bad url", 
				SourceType.PULL_STREAM_SERVICE);
	}

	/**
	 * @throws TypeMappingException
	 * @throws SchemaMetadataException
	 * @throws SNEEConfigurationException
	 * @throws MetadataException
	 * @throws UnsupportedAttributeTypeException
	 * @throws SourceMetadataException
	 * @throws TopologyReaderException
	 * @throws SNEEDataSourceException 
	 * @throws MalformedURLException 
	 * @throws CostParametersException 
	 * @throws SNCBException 
	 */
	@Test
	public void testPullStreamServiceSource() 
	throws TypeMappingException, SchemaMetadataException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException, 
	TopologyReaderException, MalformedURLException,
	SNEEDataSourceException, CostParametersException, SNCBException {
		final PullSourceWrapper mockWrapper = 
			createMock(PullSourceWrapper.class);
		List<String> mockResourceList = new ArrayList<String>();
		mockResourceList.add("resource1");
		mockResourceList.add("resource2");
		expect(mockWrapper.getResourceNames()).andReturn(mockResourceList);
		expect(mockWrapper.getSourceType()).
			andReturn(SourceType.PULL_STREAM_SERVICE);
		ExtentMetadata mockExtent = createMock(ExtentMetadata.class);
		List<ExtentMetadata> extents = new ArrayList<ExtentMetadata>();
		extents.add(mockExtent);
		expect(mockWrapper.getSchema("resource1")).andReturn(extents);
		expect(mockExtent.getExtentName()).andReturn("extent1");
		expect(mockExtent.getExtentType()).andReturn(ExtentType.PUSHED);
		expect(mockWrapper.getSchema("resource2")).andReturn(extents);
		expect(mockExtent.getExtentName()).andReturn("extent2");
		expect(mockExtent.getExtentType()).andReturn(ExtentType.PUSHED);
		replayAll();

		props.setProperty(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE, 
			"etc/physical-schema_pull-stream-source.xml");
		SNEEProperties.initialise(props);

		String typesFile = 
			SNEEProperties.getFilename(SNEEPropertyNames.INPUTS_TYPES_FILE);
		Types types = new Types(typesFile);
		Map<String,ExtentMetadata> schema = 
			new HashMap<String, ExtentMetadata>(); 
		String physSchemaFile = SNEEProperties.getFilename(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE);

		SourceManager sources = new SourceManager(schema, types){
			protected PullSourceWrapper createPullSource(String url)
			throws MalformedURLException {
				return mockWrapper;
			}
		};

		sources.processPhysicalSchema(physSchemaFile, null);
		//Expect 1 source which provides 2 extents
		assertEquals(1, sources.size());
		assertEquals(2, sources.getSources().get(0).getExtentNames().size());
		verifyAll();
	}

	/**
	 * SourceMetadataException expected since push-stream services are not supported
	 * as a data source at this time.
	 * 
	 * @throws TypeMappingException
	 * @throws SchemaMetadataException
	 * @throws SNEEConfigurationException
	 * @throws MetadataException
	 * @throws UnsupportedAttributeTypeException
	 * @throws SourceMetadataException
	 * @throws TopologyReaderException
	 * @throws MalformedURLException
	 * @throws SNEEDataSourceException
	 * @throws CostParametersException
	 * @throws SNCBException
	 */
	@Test(expected=SourceMetadataException.class)
	public void testPushStreamServiceSource() 
	throws TypeMappingException, SchemaMetadataException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException, 
	TopologyReaderException, MalformedURLException,
	SNEEDataSourceException, CostParametersException, SNCBException {
		props.setProperty(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE, 
			"etc/physical-schema_push-stream-source.xml");
		SNEEProperties.initialise(props);

		String typesFile = 
			SNEEProperties.getFilename(SNEEPropertyNames.INPUTS_TYPES_FILE);
		Types types = new Types(typesFile);
		Map<String,ExtentMetadata> schema = 
			new HashMap<String, ExtentMetadata>(); 
		String physSchemaFile = 
			SNEEProperties.getFilename(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE);

		SourceManager sources = new SourceManager(schema, types);
		sources.processPhysicalSchema(physSchemaFile, null);
	}

	/**
	 * SourceMetadataException expected since query services are not supported
	 * as a data source at this time.
	 * @throws TypeMappingException
	 * @throws SchemaMetadataException
	 * @throws SNEEConfigurationException
	 * @throws MetadataException
	 * @throws UnsupportedAttributeTypeException
	 * @throws SourceMetadataException
	 * @throws TopologyReaderException
	 * @throws MalformedURLException
	 * @throws SNEEDataSourceException
	 * @throws CostParametersException
	 * @throws SNCBException
	 */
	@Test(expected=SourceMetadataException.class)
	public void testQueryServiceSource() 
	throws TypeMappingException, SchemaMetadataException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException, 
	TopologyReaderException, MalformedURLException,
	SNEEDataSourceException, CostParametersException, SNCBException {
		props.setProperty(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE, 
			"etc/physical-schema_query-source.xml");
		SNEEProperties.initialise(props);

		String typesFile = 
			SNEEProperties.getFilename(SNEEPropertyNames.INPUTS_TYPES_FILE);
		Types types = new Types(typesFile);
		Map<String,ExtentMetadata> schema = 
			new HashMap<String, ExtentMetadata>(); 
		String physSchemaFile = SNEEProperties.getFilename(
				SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE);

		SourceManager sources = new SourceManager(schema, types);
		sources.processPhysicalSchema(physSchemaFile, null);
	}

	@Test
	public void testWSDAIRStoredDataServiceSource() 
	throws TypeMappingException, SchemaMetadataException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException, 
	TopologyReaderException, MalformedURLException,
	SNEEDataSourceException, CostParametersException, SNCBException {
		final WSDAIRSourceWrapperImpl mockWrapper = 
			createMock(WSDAIRSourceWrapperImpl.class);
		// Record 1 resource
		List<String> mockResourceList = new ArrayList<String>();
		mockResourceList.add("wsdair-resource");
		expect(mockWrapper.getResourceNames()).andReturn(mockResourceList);
		// Record the type of the service
		expect(mockWrapper.getSourceType()).andReturn(SourceType.WSDAIR);
		//Record 2 extents for the single resource
		ExtentMetadata mockExtent = createMock(ExtentMetadata.class);
		List<ExtentMetadata> extents = new ArrayList<ExtentMetadata>();
		extents.add(mockExtent);
		extents.add(mockExtent);
		expect(mockWrapper.getSchema("wsdair-resource")).andReturn(extents);
		expect(mockExtent.getExtentName()).andReturn("extent1");
		expect(mockExtent.getExtentType()).andReturn(ExtentType.TABLE);
		expect(mockExtent.getExtentName()).andReturn("extent2");
		expect(mockExtent.getExtentType()).andReturn(ExtentType.TABLE);
		replayAll();

		//Read property files
		props.setProperty(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE, 
			"etc/physical-schema_wsdair-source.xml");
		SNEEProperties.initialise(props);
		String typesFile = 
			SNEEProperties.getFilename(SNEEPropertyNames.INPUTS_TYPES_FILE);
		Types types = new Types(typesFile);
		Map<String,ExtentMetadata> schema = 
			new HashMap<String, ExtentMetadata>(); 
		String physSchemaFile = SNEEProperties.getFilename(
				SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE);

		SourceManager sources = new SourceManager(schema, types) {
			protected WSDAIRSourceWrapperImpl createWSDAIRSource(String url)
			throws MalformedURLException {
				return mockWrapper;
			}
		};
		
		sources.processPhysicalSchema(physSchemaFile, null);
		//Expect 1 source which provides 2 extents
		assertEquals(1, sources.getSources().size());
		assertEquals(2, sources.getSources().get(0).getExtentNames().size());
		verifyAll();
	}

	@Test(expected=MalformedURLException.class)
	public void testAddServiceSource_malformedURL() 
	throws SNEEDataSourceException, SchemaMetadataException, 
	TypeMappingException, MalformedURLException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException,
	TopologyReaderException, CostParametersException 
	{
		SNEEProperties.initialise(props);

		String typesFile = 
			SNEEProperties.getFilename(SNEEPropertyNames.INPUTS_TYPES_FILE);
		Types types = new Types(typesFile);
		Map<String,ExtentMetadata> schema = 
			new HashMap<String, ExtentMetadata>(); 

		SourceManager sources = new SourceManager(schema, types);
		sources.addServiceSource("", "notaurl", SourceType.PULL_STREAM_SERVICE);
	}

	/**
	 * SourceMetadataException expected since push-stream services are not supported
	 * as a data source at this time.
	 * 
	 * @throws SNEEDataSourceException
	 * @throws SchemaMetadataException
	 * @throws TypeMappingException
	 * @throws MalformedURLException
	 * @throws SNEEConfigurationException
	 * @throws MetadataException
	 * @throws UnsupportedAttributeTypeException
	 * @throws SourceMetadataException
	 * @throws TopologyReaderException
	 * @throws CostParametersException
	 */
	@Test(expected=SourceMetadataException.class)
	public void testAddServiceSource_pushStreamSource() 
	throws SNEEDataSourceException, SchemaMetadataException, 
	TypeMappingException, MalformedURLException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException,
	TopologyReaderException, CostParametersException 
	{
		SNEEProperties.initialise(props);

		String typesFile = 
			SNEEProperties.getFilename(SNEEPropertyNames.INPUTS_TYPES_FILE);
		Types types = new Types(typesFile);
		Map<String,ExtentMetadata> schema = 
			new HashMap<String, ExtentMetadata>(); 

		SourceManager sources = new SourceManager(schema, types);
		sources.addServiceSource(
				"PushStreamService", "http://example.net", 
				SourceType.PUSH_STREAM_SERVICE);
	}

	/**
	 * SourceMetadataException expected since query services are not supported
	 * as a data source at this time.
	 * 
	 * @throws SNEEDataSourceException
	 * @throws SchemaMetadataException
	 * @throws TypeMappingException
	 * @throws MalformedURLException
	 * @throws SNEEConfigurationException
	 * @throws MetadataException
	 * @throws UnsupportedAttributeTypeException
	 * @throws SourceMetadataException
	 * @throws TopologyReaderException
	 * @throws CostParametersException
	 */
	@Test(expected=SourceMetadataException.class)
	public void testAddServiceSource_querySource() 
	throws SNEEDataSourceException, SchemaMetadataException, 
	TypeMappingException, MalformedURLException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException,
	TopologyReaderException, CostParametersException 
	{
		SNEEProperties.initialise(props);

		String typesFile = 
			SNEEProperties.getFilename(SNEEPropertyNames.INPUTS_TYPES_FILE);
		Types types = new Types(typesFile);
		Map<String,ExtentMetadata> schema = 
			new HashMap<String, ExtentMetadata>(); 

		SourceManager sources = new SourceManager(schema, types);
		sources.addServiceSource(
				"QueryService", "http://example.net", 
				SourceType.QUERY_SERVICE);
	}

	@Test
	public void testAddServiceSource_pullStreamSource() 
	throws SNEEDataSourceException, SchemaMetadataException, 
	TypeMappingException, MalformedURLException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException,
	TopologyReaderException, CostParametersException 
	{
		//Instantiate mock
		final PullSourceWrapper mockSourceWrapper = 
			createMock(PullSourceWrapper.class);
		final ExtentMetadata mockExtent = 
			createMock(ExtentMetadata.class);
		String resourceName = "cco:pull:HerneBay_met";
		List<String> mockResourceList = new ArrayList<String>();
		mockResourceList.add(resourceName);
		String streamName = "envdata_hernebay_met";
		//Record mocks
		expect(mockSourceWrapper.getResourceNames()).
		andReturn(mockResourceList);
		expect(mockSourceWrapper.getSourceType()).
		andReturn(SourceType.PULL_STREAM_SERVICE);
		List<ExtentMetadata> extents = new ArrayList<ExtentMetadata>();
		extents.add(mockExtent);
		expect(mockSourceWrapper.getSchema(resourceName)).andReturn(extents);
		expect(mockExtent.getExtentName()).andReturn(streamName);
		expect(mockExtent.getExtentType()).andReturn(ExtentType.PUSHED);
		//Test
		replayAll();

		SNEEProperties.initialise(props);

		String typesFile = 
			SNEEProperties.getFilename(SNEEPropertyNames.INPUTS_TYPES_FILE);
		Types types = new Types(typesFile);
		Map<String,ExtentMetadata> schema = 
			new HashMap<String, ExtentMetadata>(); 

		SourceManager sources = new SourceManager(schema, types){
			protected PullSourceWrapper createPullSource(String url)
			throws MalformedURLException {
				return mockSourceWrapper;
			}
		};
		sources.addServiceSource("CCO-WS",
				"http://webgis1.geodata.soton.ac.uk:8080/CCO/services/PullStream?wsdl", 
				SourceType.PULL_STREAM_SERVICE);
		assertEquals(1, sources.getSources().size());
		assertEquals(1, sources.getSources().get(0).getExtentNames().size());
		verifyAll();
	}

	@Test
	public void testAddServiceSource_wsdairStreamSource() 
	throws SNEEDataSourceException, SchemaMetadataException, 
	TypeMappingException, MalformedURLException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException,
	TopologyReaderException, CostParametersException 
	{
		//Instantiate mock
		final WSDAIRSourceWrapperImpl mockSourceWrapper = 
			createMock(WSDAIRSourceWrapperImpl.class);
		String resourceName = "wsdair:CCODataAccess";
		List<String> mockResourceList = new ArrayList<String>();
		mockResourceList.add(resourceName);
		final ExtentMetadata mockExtent = 
			createMock(ExtentMetadata.class);
		String tableName = "location";
		//Record mocks
		expect(mockSourceWrapper.getResourceNames()).andReturn(mockResourceList);
		expect(mockSourceWrapper.getSourceType()).andReturn(SourceType.WSDAIR);
		List<ExtentMetadata> extents = new ArrayList<ExtentMetadata>();
		extents.add(mockExtent);
		expect(mockSourceWrapper.getSchema(resourceName)).andReturn(extents);
		expect(mockExtent.getExtentName()).andReturn(tableName);
		expect(mockExtent.getExtentType()).andReturn(ExtentType.TABLE);
		//Test
		replayAll();

		SNEEProperties.initialise(props);

		String typesFile = 
			SNEEProperties.getFilename(SNEEPropertyNames.INPUTS_TYPES_FILE);
		Types types = new Types(typesFile);
		Map<String,ExtentMetadata> schema = 
			new HashMap<String, ExtentMetadata>(); 

		SourceManager sources = new SourceManager(schema, types){
			protected WSDAIRSourceWrapperImpl createWSDAIRSource(String url)
			throws MalformedURLException {
				return mockSourceWrapper;
			}
		};
		sources.addServiceSource("CCO-Stored",
				"http://webgis1.geodata.soton.ac.uk:8080/dai/services/", 
				SourceType.WSDAIR);
		assertEquals(1, sources.getSources().size());
		assertEquals(1, sources.getSources().get(0).getExtentNames().size());
		verifyAll();
	}

}
