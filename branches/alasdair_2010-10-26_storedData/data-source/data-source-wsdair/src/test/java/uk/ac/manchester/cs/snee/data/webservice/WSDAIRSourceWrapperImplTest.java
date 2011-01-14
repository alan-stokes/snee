package uk.ac.manchester.cs.snee.data.webservice;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.cxf.ws.addressing.ReferenceParametersType;
import org.apache.log4j.PropertyConfigurator;
import org.easymock.classextension.EasyMockSupport;
import org.ggf.namespaces._2005._12.ws_dai.DataResourceAddressType;
import org.ggf.namespaces._2005._12.ws_dai.DataResourceUnavailableFault;
import org.ggf.namespaces._2005._12.ws_dai.GetDataResourcePropertyDocumentRequest;
import org.ggf.namespaces._2005._12.ws_dai.GetResourceListRequest;
import org.ggf.namespaces._2005._12.ws_dai.GetResourceListResponse;
import org.ggf.namespaces._2005._12.ws_dai.InvalidResourceNameFault;
import org.ggf.namespaces._2005._12.ws_dai.NotAuthorizedFault;
import org.ggf.namespaces._2005._12.ws_dai.ServiceBusyFault;
import org.ggf.namespaces._2005._12.ws_dair.SQLPropertyDocumentType;
import org.ggf.namespaces._2005._12.ws_dair.SchemaDescription;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.datasource.webservice.WSDAIRAccessServiceClient;
import uk.ac.manchester.cs.snee.datasource.webservice.WSDAIRSourceWrapperImpl;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.Types;

public class WSDAIRSourceWrapperImplTest extends EasyMockSupport {
	
	private static final String WSDAI_OGSA_ACCESS_CCO_DATA_RESOURCE = 
		"wsdai:OGSAAccessCCODataResource";

	private WSDAIRSourceWrapperImpl wsdairSource;

	// Mock Objects
	final WSDAIRAccessServiceClient mockWsdairClient = 
		createMock(WSDAIRAccessServiceClient.class);
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				WSDAIRSourceWrapperImplTest.class.getClassLoader().getResource(
				"etc/log4j.properties"));
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		// Configure the types
		String typesFileLoc = 
			WSDAIRSourceWrapperImplTest.class.getClassLoader().
				getResource("etc/Types.xml").getFile();
		Types types = new Types(typesFileLoc);

		// Create a source wrapper to test
		wsdairSource = new WSDAIRSourceWrapperImpl("http://example.org", types) {
			protected WSDAIRAccessServiceClient createServiceClient(
					String url) 
		    throws MalformedURLException {
				return mockWsdairClient;
			}
		};
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetResourceNames() 
	throws SNEEDataSourceException, NotAuthorizedFault, ServiceBusyFault,
	SAXException, IOException, ParserConfigurationException {
		GetResourceListResponse mockResponse = 
			createMock(GetResourceListResponse.class);
		DataResourceAddressType mockDataResourceAddress =
			createMock(DataResourceAddressType.class);
		ReferenceParametersType mockRefParaType = 
			createMock(ReferenceParametersType.class);
		
		List<DataResourceAddressType> addressList = 
			new ArrayList<DataResourceAddressType>();
		addressList.add(mockDataResourceAddress);
		
		List<Object> nodeList = new ArrayList<Object>();
		String newNode =  
			"<ns4:DataResourceAbstractName " +
			"xmlns:ns4=\"http://www.ggf.org/namespaces/2005/12/WS-DAI/\">" +
			"wsdai:OGSAAccessCCODataResource" +
			"</ns4:DataResourceAbstractName>"; // Convert this to XML
		
		Document document = createXMLDocument(newNode);
		Node node = document.getFirstChild();
		nodeList.add(node);
		
		expect(mockWsdairClient.getResourceList(
				isA(GetResourceListRequest.class))).andReturn(mockResponse);
		expect(mockResponse.getDataResourceAddress()).andReturn(addressList);
		expect(mockDataResourceAddress.getReferenceParameters()).andReturn(
				mockRefParaType);
		expect(mockRefParaType.getAny()).andReturn(nodeList);
		
		replayAll();
		
		// Perform operation
		List<String> resourceNames = wsdairSource.getResourceNames();
		
		// Verify result
		assertEquals(false, resourceNames.isEmpty());
		assertEquals(1, resourceNames.size());
		assertTrue(resourceNames.get(0).equals(
				WSDAI_OGSA_ACCESS_CCO_DATA_RESOURCE));
		verifyAll();
	}

	private Document createXMLDocument(String nodeText) throws SAXException,
			IOException, ParserConfigurationException {
//		System.out.println(nodeText);
		Document document = DocumentBuilderFactory
			.newInstance()
			.newDocumentBuilder()
			.parse(new InputSource(new StringReader(nodeText)));
		return document;
	}

	@Test
	public void testGetSchema_sqlPropertyDocument() 
	throws SNEEDataSourceException, SchemaMetadataException, 
	TypeMappingException, InvalidResourceNameFault,
	DataResourceUnavailableFault, NotAuthorizedFault, ServiceBusyFault,
	SAXException, IOException, ParserConfigurationException 
	{
		SQLPropertyDocumentType mockSQLPropDoc = 
			createMock(SQLPropertyDocumentType.class);
		SchemaDescription mockSchemaDesc = createMock(SchemaDescription.class);
		
		List<Object> nodeList = new ArrayList<Object>();
		String newNode = 
			"<databaseSchema xmlns=" +
			"\"http://ogsadai.org.uk/namespaces/2005/10/properties\" " +
			"xmlns:ns22=" +
			"\"http://ogsadai.org.uk/namespaces/2005/10/properties\">" +
			"<logicalSchema>" +
			"<table catalog=\"cco\" name=\"site_locations\" schema=\"null\"> " +
            "<column default=\"null\" fullName=\"site_locations_id\" " +
            "length=\"10\" name=\"id\" nullable=\"false\" position=\"1\">" +
            "<sqlTypeName>INT</sqlTypeName><sqlJavaTypeID>4</sqlJavaTypeID>" +
            "</column><column default=\"null\" fullName=\"site_locations_easting\" " +
            "length=\"19\" name=\"easting\" nullable=\"true \" position=\"2\"> " +
            "<sqlTypeName>BIGINT</sqlTypeName><sqlJavaTypeID>-5</sqlJavaTypeID>" +
            "</column><primaryKey><columnName>id</columnName></primaryKey>" +
            "</table></logicalSchema></databaseSchema>";
		
		Document document = createXMLDocument(newNode);
		Node node = document.getFirstChild();
		nodeList.add(node);
		System.out.println(node.getTextContent());
				
		expect(mockWsdairClient.getSQLPropertyDocument(isA(
				GetDataResourcePropertyDocumentRequest.class)))
				.andReturn(mockSQLPropDoc);
		expect(mockSQLPropDoc.getDataResourceAbstractName())
			.andReturn(WSDAI_OGSA_ACCESS_CCO_DATA_RESOURCE)
			.times(0, 1);
		expect(mockSQLPropDoc.getSchemaDescription()).andReturn(mockSchemaDesc);
		expect(mockSchemaDesc.getAny()).andReturn(nodeList);
		
		replayAll();
		//Perform operation
		List<ExtentMetadata> extentList = 
			wsdairSource.getSchema(WSDAI_OGSA_ACCESS_CCO_DATA_RESOURCE);
		// Verify result
		assertEquals(1, extentList.size());
		verifyAll();
	}

	@Test@Ignore
	public void testExecuteQuery() {
		fail("Not yet implemented");
	}

}
