package uk.ac.manchester.cs.snee.compiler.metadata;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

import java.net.MalformedURLException;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.easymock.classextension.EasyMockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

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
import uk.ac.manchester.cs.snee.data.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.data.webservice.PullSourceWrapper;

public class MetadataTest extends EasyMockSupport {

//	String[] colTypes = {"real", "integer", "double", "numeric",
//			"decimal", "smallint", "bit", "tinyint", "date",
//			"time", "timestamp", "varchar", "char", "longvarchar",
//			"boolean"};
	String[] colTypes = {"integer", "float", "string", "boolean"};
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				MetadataTest.class.getClassLoader().getResource(
						"etc/log4j.properties"));
	}

//	String logicalSchemaFilename = "testLogicalSchema";
//	String physicalSchemaFilename = "test/physical-schema.xml";

	@Before
	public void setUp() throws Exception {
		Properties props = new Properties();
		props.setProperty(SNEEPropertyNames.INPUTS_TYPES_FILE, "Types.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_UNITS_FILE, "units.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_LOGICAL_SCHEMA_FILE, "logical-schema.xml");
		props.setProperty(SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR, "output");
		SNEEProperties.initialise(props);
	}

//	private File createTestSchemaFile(String[][] extents) {
//		File file = createFileName();
//		try {
//			Transformer transformer = 
//				TransformerFactory.newInstance().newTransformer();
//			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
//			
//			Document doc = createDocument(extents);
//			
//			StreamResult result = new StreamResult(
//					new FileOutputStream(file));
//			DOMSource source = new DOMSource(doc);
//			transformer.transform(source, result);
//		} catch (Exception e) {
//			fail("Error writing test schema file.");
//		}
//		return file;
//	}
//
//	private File createFileName() {
//		File logicalSchemaFile;
//		do {
//			logicalSchemaFilename += "-1";
//			logicalSchemaFile = new File(logicalSchemaFilename + ".xml");
//		} while (logicalSchemaFile.exists());
//		logicalSchemaFilename += ".xml";
//		return logicalSchemaFile;
//	}
	
//	private Document createDocument(String[][] extents) 
//	throws ParserConfigurationException {
//		DocumentBuilderFactory factory = 
//			DocumentBuilderFactory.newInstance();
//		DocumentBuilder builder = factory.newDocumentBuilder();
//		DOMImplementation impl = builder.getDOMImplementation();
//		Document doc = impl.createDocument(null, null, null);
//		Element schemaElement = doc.createElement("schema");
//		doc.appendChild(schemaElement);
//
//		for (int i = 0; i < extents.length; i++) {
//			schemaElement.appendChild(createExtentElement(doc, 
//					extents[i][0], extents[i][1], extents[i][2]));
//		}
//		return doc;
//	}
	
//	private Element createExtentElement(Document doc, String name, 
//			String extentType, String streamType) {
//		
//		Element pushElement = doc.createElement(extentType);
//		pushElement.setAttribute("name", name);	
//		if (extentType.equals("stream"))
//			pushElement.setAttribute("type", streamType);
//		
//		for (int i = 0; i < colTypes.length; i++) {
//			Element col = createColumnElement(doc, colTypes[i]+"Col", 
//					colTypes[i]);
//			pushElement.appendChild(col);
//		}
//		
//		return pushElement;
//	}

//	private Element createColumnElement(Document doc, String name, 
//			String type) {
//		Element colElement = doc.createElement("column");
//		colElement.setAttribute("name", name);
//		Element typeElement = doc.createElement("type");
//		typeElement.setAttribute("class", type);
//		colElement.appendChild(typeElement);
//		return colElement;
//	}
	
	@After
	public void tearDown() throws Exception {
	}

//	@Test
//	public void testPushStreamMetaData() 
//	throws TypeMappingException, SchemaMetadataException, 
//	MetadataException, UnsupportedAttributeTypeException, 
//	SNEEConfigurationException, SourceMetadataException {
//		String[][] streams = {{"PushTest", "stream", "push"}, 
//				{"PushStream", "stream", "push"}};
//		File file = createTestSchemaFile(streams);
//		Metadata schema = new Metadata();
////			new Metadata(logicalSchemaFilename);
//		file.delete();
//		assertEquals(2, schema.getPushedExtents().size());
//		assertEquals(0, schema.getAcquireExtents().size());
//		assertEquals(0, schema.getStoredExtents().size());
//	}
//
//	@Test
//	public void testPullStreamMetaData() 
//	throws TypeMappingException, SchemaMetadataException, 
//	MetadataException, UnsupportedAttributeTypeException, 
//	SNEEConfigurationException, SourceMetadataException {
//		String[][] streams = {{"PullTest", "stream", "pull"},
//				{"stream", "stream", "pull"}};
//		File file = createTestSchemaFile(streams);
//		Metadata schema = new Metadata(); 
////			new Metadata(logicalSchemaFilename);
//		file.delete();
//		assertEquals(0, schema.getPushedExtents().size());
//		assertEquals(2, schema.getAcquireExtents().size());
//		assertEquals(0, schema.getStoredExtents().size());
//	}
//
//	@Test
//	public void testTableStreamMetaData() 
//	throws TypeMappingException, SchemaMetadataException, 
//	MetadataException, UnsupportedAttributeTypeException, 
//	SNEEConfigurationException, SourceMetadataException {
//		String[][] tables = {{"TableTest","table", ""}};
//		File file = createTestSchemaFile(tables);
//		Metadata schema = new Metadata(); 
////			new Metadata(logicalSchemaFilename);
//		file.delete();
//		assertEquals(0, schema.getPushedExtents().size());
//		assertEquals(0, schema.getAcquireExtents().size());
//		assertEquals(1, schema.getStoredExtents().size());
//	}
//	
//	@Test
//	public void testStreamMetaData_oldForm() 
//	throws TypeMappingException, SchemaMetadataException, 
//	MetadataException, UnsupportedAttributeTypeException, 
//	SNEEConfigurationException, SourceMetadataException {
//		String[][] streams = {{"PullTest", "stream", ""}};
//		File file = createTestSchemaFile(streams);
//		Metadata schema = new Metadata(); 
////			new Metadata(logicalSchemaFilename);
//		file.delete();
//		assertEquals(0, schema.getPushedExtents().size());
//		assertEquals(0, schema.getAcquireExtents().size());
//		assertEquals(0, schema.getStoredExtents().size());
//	}
//	
//	@Test
//	public void testGetSourceMetaData_validExtentName() 
//	throws TypeMappingException, SchemaMetadataException, 
//	MetadataException, UnsupportedAttributeTypeException, 
//	SNEEConfigurationException, SourceMetadataException {
//		String[][] streams = {{"PushTest", "stream", "push"}};
//		File file = createTestSchemaFile(streams);
//		Metadata schema = new Metadata(); 
////			new Metadata(logicalSchemaFilename);
//		file.delete();
//		assertEquals(1, schema.getPushedExtents().size());
//		assertEquals(0, schema.getAcquireExtents().size());
//		assertEquals(0, schema.getStoredExtents().size());
//		ExtentMetadata extentMetadata = schema.getExtentMetadata("PushTest");
//		assertEquals(true, extentMetadata.getExtentName().equalsIgnoreCase("PushTest"));
//	}

	@Test
	public void testGetSourceMetaData_testSchema() 
	throws TypeMappingException, SchemaMetadataException, 
	MetadataException, UnsupportedAttributeTypeException, 
	SNEEConfigurationException, SourceMetadataException {
		Metadata schema = new Metadata(); 
//			new Metadata("test/logical-schema.xml");
		assertEquals(2, schema.getPushedExtents().size());
		assertEquals(1, schema.getAcquireExtents().size());
		assertEquals(1, schema.getStoredExtents().size());
	}

	@Test(expected=ExtentDoesNotExistException.class)
	public void testGetSourceMetaData_invalidExtentName() 
	throws SchemaMetadataException, MetadataException, 
	TypeMappingException, UnsupportedAttributeTypeException,
	SNEEConfigurationException, SourceMetadataException 
	{
		Metadata schema = new Metadata(); 
//			new Metadata("test/logical-schema.xml");
		schema.getExtentMetadata("Random");
	}

	@Test(expected=MalformedURLException.class)
	public void testAddWebServiceSource_invalidURL() 
	throws TypeMappingException, SchemaMetadataException, 
	MalformedURLException, SNEEDataSourceException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException 
	{
		Metadata schema = new Metadata();
		schema.addWebServiceSource("bad url");
	}
	
	@Test(expected=ExtentDoesNotExistException.class)
	public void testAddWebServiceSource_unknownSource() 
	throws SNEEDataSourceException, SchemaMetadataException, 
	TypeMappingException, MalformedURLException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException 
	{
		//Instantiate mock
		final PullSourceWrapper mockSourceWrapper = createMock(PullSourceWrapper.class);
		final ExtentMetadata mockExtent = createMock(ExtentMetadata.class);
		String resourceName = "cco:pull:HerneBay_met";
		String streamName = "envdata_hernebay_met";
		//Record mocks
		expect(mockSourceWrapper.getSchema(resourceName)).andReturn(mockExtent);
		expect(mockExtent.getExtentName()).andReturn(streamName);
		expect(mockExtent.getExtentType()).andReturn(ExtentType.PUSHED);
		//Test
		replayAll();
		Metadata schema = new Metadata();
		schema.addWebServiceSource(
				"http://webgis1.geodata.soton.ac.uk:8080/CCO/services/PullStream?wsdl");
		schema.getExtentMetadata("someName");
		verifyAll();
	}
	
	@Ignore@Test //FIXME: Implement feature
	public void testAddWebServiceSource() 
	throws SNEEDataSourceException, SchemaMetadataException, 
	TypeMappingException, MalformedURLException, 
	SNEEConfigurationException, MetadataException,
	UnsupportedAttributeTypeException, SourceMetadataException 
	{
		//Instantiate mock
		final PullSourceWrapper mockSourceWrapper = createMock(PullSourceWrapper.class);
		final ExtentMetadata mockExtent = createMock(ExtentMetadata.class);
		String resourceName = "cco:pull:HerneBay_met";
		String streamName = "envdata_hernebay_met";
		//Record mocks
		expect(mockSourceWrapper.getSchema(resourceName)).andReturn(mockExtent);
		expect(mockExtent.getExtentName()).andReturn(streamName);
		expect(mockExtent.getExtentType()).andReturn(ExtentType.PUSHED);
		//Test
		replayAll();
		Metadata schema = new Metadata();
		schema.addWebServiceSource("http://webgis1.geodata.soton.ac.uk:8080/CCO/services/PullStream?wsdl");
		schema.getExtentMetadata(streamName);
		verifyAll();
	}
	
}
