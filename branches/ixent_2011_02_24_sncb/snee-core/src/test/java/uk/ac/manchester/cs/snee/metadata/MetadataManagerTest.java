package uk.ac.manchester.cs.snee.metadata;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.easymock.classextension.EasyMockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.datasource.webservice.PullSourceWrapper;
//import uk.ac.manchester.cs.snee.datasource.webservice.WSDAIRSourceWrapper;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentDoesNotExistException;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SourceDoesNotExistException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.sncb.SNCBException;

public class MetadataManagerTest extends EasyMockSupport {

	String[] colTypes = {"integer", "float", "string", "boolean"};
	private Properties props;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				MetadataManagerTest.class.getClassLoader().getResource(
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

	@Test
	public void testPushStreamMetaData() 
	throws TypeMappingException, SchemaMetadataException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException, 
	TopologyReaderException, MalformedURLException,
	SNEEDataSourceException, CostParametersException, SNCBException {
		props.setProperty(SNEEPropertyNames.INPUTS_LOGICAL_SCHEMA_FILE, "etc/logical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE, "etc/physical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_COST_PARAMETERS_FILE, "etc/cost-parameters.xml");
		SNEEProperties.initialise(props);

		MetadataManager schema = new MetadataManager(null);
		assertEquals(2, schema.getPushedExtents().size());
	}

	@Test
	public void testPullStreamMetaData() 
	throws TypeMappingException, SchemaMetadataException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException, 
	TopologyReaderException, MalformedURLException,
	SNEEDataSourceException, CostParametersException, SNCBException {
		props.setProperty(SNEEPropertyNames.INPUTS_LOGICAL_SCHEMA_FILE, "etc/logical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE, "etc/physical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_COST_PARAMETERS_FILE, "etc/cost-parameters.xml");
		SNEEProperties.initialise(props);

		MetadataManager schema = new MetadataManager(null);
		assertEquals(2, schema.getAcquireExtents().size());
	}

	@Test
	public void testTableMetaData() 
	throws TypeMappingException, SchemaMetadataException, 
	MetadataException, UnsupportedAttributeTypeException, 
	SNEEConfigurationException, SourceMetadataException,
	TopologyReaderException, MalformedURLException, 
	SNEEDataSourceException, CostParametersException, SNCBException {
		props.setProperty(SNEEPropertyNames.INPUTS_LOGICAL_SCHEMA_FILE, "etc/logical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE, "etc/physical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_COST_PARAMETERS_FILE, "etc/cost-parameters.xml");
		SNEEProperties.initialise(props);

		MetadataManager schema = new MetadataManager(null); 
		assertEquals(1, schema.getStoredExtents().size());
	}

	@Test
	public void testGetExtentMetaData_validExtentName() 
	throws TypeMappingException, SchemaMetadataException, 
	MetadataException, UnsupportedAttributeTypeException, 
	SNEEConfigurationException, SourceMetadataException,
	TopologyReaderException, MalformedURLException,
	SNEEDataSourceException, CostParametersException, SNCBException {
		props.setProperty(SNEEPropertyNames.INPUTS_LOGICAL_SCHEMA_FILE, "etc/logical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE, "etc/physical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_COST_PARAMETERS_FILE, "etc/cost-parameters.xml");
		SNEEProperties.initialise(props);

		MetadataManager schema = new MetadataManager(null); 
		ExtentMetadata extentMetadata = 
			schema.getExtentMetadata("PushStream");
		assertEquals("PushStream".toLowerCase(), 
				extentMetadata.getExtentName());
	}

	@Test(expected=ExtentDoesNotExistException.class)
	public void testGetExtentMetaData_invalidExtentName() 
	throws SchemaMetadataException, MetadataException, 
	TypeMappingException, UnsupportedAttributeTypeException,
	SNEEConfigurationException, SourceMetadataException,
	TopologyReaderException, MalformedURLException,
	SNEEDataSourceException, CostParametersException, SNCBException 
	{
		props.setProperty(SNEEPropertyNames.INPUTS_LOGICAL_SCHEMA_FILE, "etc/logical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE, "etc/physical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_COST_PARAMETERS_FILE, "etc/cost-parameters.xml");
		SNEEProperties.initialise(props);

		MetadataManager schema = new MetadataManager(null); 
		
		schema.getExtentMetadata("Random");
	}

	@Test(expected=SourceDoesNotExistException.class)
	public void testGetSourceMetaData_noSourceForExtent() 
	throws SchemaMetadataException, MetadataException, 
	TypeMappingException, UnsupportedAttributeTypeException,
	SNEEConfigurationException, SourceMetadataException,
	TopologyReaderException, MalformedURLException,
	SNEEDataSourceException, CostParametersException, SNCBException 
	{
		props.setProperty(SNEEPropertyNames.INPUTS_LOGICAL_SCHEMA_FILE, "etc/logical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_COST_PARAMETERS_FILE, "etc/cost-parameters.xml");
		SNEEProperties.initialise(props);

		MetadataManager schema = new MetadataManager(null); 
		
		schema.getSource("PushStream");
	}

	public void testGetSourceMetaData() 
	throws SchemaMetadataException, MetadataException, 
	TypeMappingException, UnsupportedAttributeTypeException,
	SNEEConfigurationException, SourceMetadataException,
	TopologyReaderException, MalformedURLException,
	SNEEDataSourceException, CostParametersException, SNCBException,
	SourceDoesNotExistException
	{
		props.setProperty(SNEEPropertyNames.INPUTS_LOGICAL_SCHEMA_FILE, "etc/logical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE, "etc/physical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_COST_PARAMETERS_FILE, "etc/cost-parameters.xml");
		SNEEProperties.initialise(props);

		MetadataManager schema = new MetadataManager(null); 
		
		schema.getSource("PushStream");
	}
	
	@Test(expected=MalformedURLException.class)
	public void testAddWebServiceSource_invalidURL() 
	throws TypeMappingException, SchemaMetadataException, 
	MalformedURLException, SNEEDataSourceException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException, 
	TopologyReaderException, CostParametersException, SNCBException 
	{
		props.setProperty(SNEEPropertyNames.SNCB_PERFORM_METADATA_COLLECTION, "false");
		SNEEProperties.initialise(props);
		
		MetadataManager schema = new MetadataManager(null);//{
//			private SourceManager createSourceManager() {
//				return mockSourceManager;
//			}
//		};
		schema.addDataSource("bad url", "bad url", 
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
	@Test@Ignore
	public void testPullStreamServiceSource() 
	throws TypeMappingException, SchemaMetadataException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException, 
	TopologyReaderException, MalformedURLException,
	SNEEDataSourceException, CostParametersException, SNCBException {
		final PullSourceWrapper mockWrapper = 
			createMock(PullSourceWrapper.class);
		ExtentMetadata mockExtent = createMock(ExtentMetadata.class);
		List<String> mockResourceList = new ArrayList<String>();
		mockResourceList.add("resource1");
		mockResourceList.add("resource2");
		expect(mockWrapper.getResourceNames()).
			andReturn(mockResourceList);
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

		MetadataManager schema = new MetadataManager(null);// {
//			protected PullSourceWrapper createPullSource(String url)
//			throws MalformedURLException {
//				return mockWrapper;
//			}
//		};
		//Expect 1 source which provides 2 extents
//		assertEquals(1, schema.getSources().size());
		assertEquals(2, schema.getExtentNames().size());
	}

//	@Test@Ignore
//	public void testStoredDataServiceSource() 
//	throws TypeMappingException, SchemaMetadataException, 
//	SNEEConfigurationException, MetadataException, 
//	UnsupportedAttributeTypeException, SourceMetadataException, 
//	TopologyReaderException, MalformedURLException,
//	SNEEDataSourceException, CostParametersException {
//		final WSDAIRSourceWrapper mockWrapper = 
//			createMock(WSDAIRSourceWrapper.class);
//		ExtentMetadata mockExtent = createMock(ExtentMetadata.class);
//		List<String> mockResourceList = new ArrayList<String>();
//		mockResourceList.add("resource1");
//		mockResourceList.add("resource2");
//		expect(mockWrapper.getResourceNames()).
//			andReturn(mockResourceList);
//		List<ExtentMetadata> extents = new ArrayList<ExtentMetadata>();
//		extents.add(mockExtent);
//		expect(mockWrapper.getSchema("resource1")).andReturn(extents);
//		expect(mockExtent.getExtentName()).andReturn("extent1");
//		expect(mockExtent.getExtentType()).andReturn(ExtentType.TABLE);
//		expect(mockWrapper.getSchema("resource2")).andReturn(extents);
//		expect(mockExtent.getExtentName()).andReturn("extent2");
//		expect(mockExtent.getExtentType()).andReturn(ExtentType.TABLE);
//		replayAll();
//
//		props.setProperty(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE, 
//				"etc/physical-schema_wsdair-source.xml");
//		SNEEProperties.initialise(props);
//
//		MetadataManager schema = new MetadataManager() {
//			protected WSDAIRSourceWrapper createWSDAIRSource(String url)
//			throws MalformedURLException {
//				return mockWrapper;
//			}
//		};
//		//Expect 1 source which provides 2 extents
////		assertEquals(1, schema.getSources().size());
//		assertEquals(2, schema.getExtentNames().size());
//	}

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
		new MetadataManager(null);
	}
	
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
		new MetadataManager(null);
	}
	
	@Test(expected=MalformedURLException.class)
	public void testAddServiceSource_malformedURL() 
	throws SNEEDataSourceException, SchemaMetadataException, 
	TypeMappingException, MalformedURLException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException,
	TopologyReaderException, CostParametersException, SNCBException 
	{
		SNEEProperties.initialise(props);
		MetadataManager schema = new MetadataManager(null);
		schema.addDataSource(
				"", "notaurl", SourceType.PULL_STREAM_SERVICE);
	}
	
	@Test(expected=SourceMetadataException.class)
	public void testAddServiceSource_pushStreamSource() 
	throws SNEEDataSourceException, SchemaMetadataException, 
	TypeMappingException, MalformedURLException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException,
	TopologyReaderException, CostParametersException, SNCBException 
	{
		SNEEProperties.initialise(props);
		MetadataManager schema = new MetadataManager(null);
		schema.addDataSource(
				"PushStreamService", "http://example.net", 
				SourceType.PUSH_STREAM_SERVICE);
	}
	
	@Test(expected=SourceMetadataException.class)
	public void testAddServiceSource_querySource() 
	throws SNEEDataSourceException, SchemaMetadataException, 
	TypeMappingException, MalformedURLException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException,
	TopologyReaderException, CostParametersException, SNCBException 
	{
		SNEEProperties.initialise(props);
		MetadataManager schema = new MetadataManager(null);
		schema.addDataSource(
				"QueryService", "http://example.net", 
				SourceType.QUERY_SERVICE);
	}

	@Test@Ignore
	public void testAddServiceSource_pullStreamSource() 
	throws SNEEDataSourceException, SchemaMetadataException, 
	TypeMappingException, MalformedURLException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException,
	TopologyReaderException, CostParametersException, SNCBException 
	{
		SNEEProperties.initialise(props);
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
		List<ExtentMetadata> extents = new ArrayList<ExtentMetadata>();
		extents.add(mockExtent);
		expect(mockSourceWrapper.getSchema(resourceName)).andReturn(extents);
		expect(mockExtent.getExtentName()).andReturn(streamName);
		expect(mockExtent.getExtentType()).andReturn(ExtentType.PUSHED);
		//Test
		replayAll();
		MetadataManager schema = new MetadataManager(null) {
			protected PullSourceWrapper createPullSource(String url)
			throws MalformedURLException {
				return mockSourceWrapper;
			}
		};
		schema.addDataSource("CCO-WS",
				"http://webgis1.geodata.soton.ac.uk:8080/CCO/services/PullStream?wsdl", 
				SourceType.PULL_STREAM_SERVICE);
//		assertEquals(1, schema.getSources().size());
		assertEquals(1, schema.getExtentNames().size());
	}
	
}
