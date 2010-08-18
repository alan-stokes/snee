package uk.ac.manchester.cs.snee.compiler.metadata;

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
import org.junit.Test;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentDoesNotExistException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentType;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceType;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.data.webservice.PullSourceWrapper;

public class MetadataTest extends EasyMockSupport {

	String[] colTypes = {"integer", "float", "string", "boolean"};
	private Properties props;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				MetadataTest.class.getClassLoader().getResource(
						"etc/log4j.properties"));
	}

	@Before
	public void setUp() throws Exception {
		props = new Properties();
		props.setProperty(SNEEPropertyNames.INPUTS_TYPES_FILE, "etc/Types.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_UNITS_FILE, "etc/units.xml");
		props.setProperty(SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR, "output");
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
	SNEEDataSourceException {
		props.setProperty(SNEEPropertyNames.INPUTS_LOGICAL_SCHEMA_FILE, "etc/logical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE, "etc/physical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_COST_PARAMETERS_FILE, "etc/cost-parameters.xml");
		SNEEProperties.initialise(props);

		Metadata schema = new Metadata();
		assertEquals(2, schema.getPushedExtents().size());
	}

	@Test
	public void testPullStreamMetaData() 
	throws TypeMappingException, SchemaMetadataException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException, 
	TopologyReaderException, MalformedURLException,
	SNEEDataSourceException {
		props.setProperty(SNEEPropertyNames.INPUTS_LOGICAL_SCHEMA_FILE, "etc/logical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE, "etc/physical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_COST_PARAMETERS_FILE, "etc/cost-parameters.xml");
		SNEEProperties.initialise(props);

		Metadata schema = new Metadata();
		assertEquals(1, schema.getAcquireExtents().size());
	}

	@Test
	public void testTableStreamMetaData() 
	throws TypeMappingException, SchemaMetadataException, 
	MetadataException, UnsupportedAttributeTypeException, 
	SNEEConfigurationException, SourceMetadataException,
	TopologyReaderException, MalformedURLException, 
	SNEEDataSourceException {
		props.setProperty(SNEEPropertyNames.INPUTS_LOGICAL_SCHEMA_FILE, "etc/logical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE, "etc/physical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_COST_PARAMETERS_FILE, "etc/cost-parameters.xml");
		SNEEProperties.initialise(props);

		Metadata schema = new Metadata(); 
		assertEquals(1, schema.getStoredExtents().size());
	}

	@Test
	public void testGetSourceMetaData_validExtentName() 
	throws TypeMappingException, SchemaMetadataException, 
	MetadataException, UnsupportedAttributeTypeException, 
	SNEEConfigurationException, SourceMetadataException,
	TopologyReaderException, MalformedURLException,
	SNEEDataSourceException {
		props.setProperty(SNEEPropertyNames.INPUTS_LOGICAL_SCHEMA_FILE, "etc/logical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE, "etc/physical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_COST_PARAMETERS_FILE, "etc/cost-parameters.xml");
		SNEEProperties.initialise(props);

		Metadata schema = new Metadata(); 
		ExtentMetadata extentMetadata = 
			schema.getExtentMetadata("PushStream");
		assertEquals("PushStream".toLowerCase(), 
				extentMetadata.getExtentName());
	}

	@Test(expected=ExtentDoesNotExistException.class)
	public void testGetSourceMetaData_invalidExtentName() 
	throws SchemaMetadataException, MetadataException, 
	TypeMappingException, UnsupportedAttributeTypeException,
	SNEEConfigurationException, SourceMetadataException,
	TopologyReaderException, MalformedURLException,
	SNEEDataSourceException 
	{
		props.setProperty(SNEEPropertyNames.INPUTS_LOGICAL_SCHEMA_FILE, "etc/logical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE, "etc/physical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_COST_PARAMETERS_FILE, "etc/cost-parameters.xml");
		SNEEProperties.initialise(props);

		Metadata schema = new Metadata(); 
		
		schema.getExtentMetadata("Random");
	}

	@Test(expected=MalformedURLException.class)
	public void testAddWebServiceSource_invalidURL() 
	throws TypeMappingException, SchemaMetadataException, 
	MalformedURLException, SNEEDataSourceException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException, 
	TopologyReaderException 
	{
		Metadata schema = new Metadata();
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
	 */
	@Test
	public void testPullStreamServiceSource() 
	throws TypeMappingException, SchemaMetadataException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException, 
	TopologyReaderException, MalformedURLException,
	SNEEDataSourceException {
		final PullSourceWrapper mockWrapper = 
			createMock(PullSourceWrapper.class);
		ExtentMetadata mockExtent = createMock(ExtentMetadata.class);
		List<String> mockResourceList = new ArrayList<String>();
		mockResourceList.add("resource1");
		mockResourceList.add("resource2");
		expect(mockWrapper.getResourceNames()).
			andReturn(mockResourceList);
		expect(mockWrapper.getSchema("resource1")).andReturn(mockExtent);
		expect(mockExtent.getExtentName()).andReturn("extent1");
		expect(mockExtent.getExtentType()).andReturn(ExtentType.PUSHED);
		expect(mockWrapper.getSchema("resource2")).andReturn(mockExtent);
		expect(mockExtent.getExtentName()).andReturn("extent2");
		expect(mockExtent.getExtentType()).andReturn(ExtentType.PUSHED);
		replayAll();

		props.setProperty(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE, 
				"etc/physical-schema_pull-stream-source.xml");
		SNEEProperties.initialise(props);

		Metadata schema = new Metadata() {
			protected PullSourceWrapper createPullSource(String url)
			throws MalformedURLException {
				return mockWrapper;
			}
		};
		//Expect 1 source which provides 2 extents
		assertEquals(1, schema.getSources().size());
		assertEquals(2, schema.getExtentNames().size());
	}
	
	@Test(expected=SourceMetadataException.class)
	public void testPushStreamServiceSource() 
	throws TypeMappingException, SchemaMetadataException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException, 
	TopologyReaderException, MalformedURLException,
	SNEEDataSourceException {
		props.setProperty(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE, 
				"etc/physical-schema_push-stream-source.xml");
		SNEEProperties.initialise(props);
		new Metadata();
	}
	
	@Test(expected=SourceMetadataException.class)
	public void testQueryServiceSource() 
	throws TypeMappingException, SchemaMetadataException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException, 
	TopologyReaderException, MalformedURLException,
	SNEEDataSourceException {
		props.setProperty(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE, 
				"etc/physical-schema_query-source.xml");
		SNEEProperties.initialise(props);
		new Metadata();
	}
	
	@Test(expected=MalformedURLException.class)
	public void testAddServiceSource_malformedURL() 
	throws SNEEDataSourceException, SchemaMetadataException, 
	TypeMappingException, MalformedURLException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException,
	TopologyReaderException 
	{
		SNEEProperties.initialise(props);
		Metadata schema = new Metadata();
		schema.addServiceSource(
				"", "notaurl", SourceType.PULL_STREAM_SERVICE);
	}
	
	@Test(expected=SourceMetadataException.class)
	public void testAddServiceSource_pushStreamSource() 
	throws SNEEDataSourceException, SchemaMetadataException, 
	TypeMappingException, MalformedURLException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException,
	TopologyReaderException 
	{
		SNEEProperties.initialise(props);
		Metadata schema = new Metadata();
		schema.addServiceSource(
				"PushStreamService", "http://example.net", 
				SourceType.PUSH_STREAM_SERVICE);
	}
	
	@Test(expected=SourceMetadataException.class)
	public void testAddServiceSource_querySource() 
	throws SNEEDataSourceException, SchemaMetadataException, 
	TypeMappingException, MalformedURLException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException,
	TopologyReaderException 
	{
		SNEEProperties.initialise(props);
		Metadata schema = new Metadata();
		schema.addServiceSource(
				"QueryService", "http://example.net", 
				SourceType.QUERY_SERVICE);
	}

	@Test
	public void testAddServiceSource_pullStreamSource() 
	throws SNEEDataSourceException, SchemaMetadataException, 
	TypeMappingException, MalformedURLException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException,
	TopologyReaderException 
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
		expect(mockSourceWrapper.getSchema(resourceName)).andReturn(mockExtent);
		expect(mockExtent.getExtentName()).andReturn(streamName);
		expect(mockExtent.getExtentType()).andReturn(ExtentType.PUSHED);
		//Test
		replayAll();
		Metadata schema = new Metadata() {
			protected PullSourceWrapper createPullSource(String url)
			throws MalformedURLException {
				return mockSourceWrapper;
			}
		};
		schema.addServiceSource("CCO-WS",
				"http://webgis1.geodata.soton.ac.uk:8080/CCO/services/PullStream?wsdl", 
				SourceType.PULL_STREAM_SERVICE);
		assertEquals(1, schema.getSources().size());
		assertEquals(1, schema.getExtentNames().size());
	}
	
}
