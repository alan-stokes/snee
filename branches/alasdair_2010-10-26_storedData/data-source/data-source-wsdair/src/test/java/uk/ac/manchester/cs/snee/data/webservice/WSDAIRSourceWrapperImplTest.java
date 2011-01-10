package uk.ac.manchester.cs.snee.data.webservice;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
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
import org.easymock.internal.matchers.InstanceOf;
import org.ggf.namespaces._2005._12.ws_dai.DataResourceAddressType;
import org.ggf.namespaces._2005._12.ws_dai.DataResourceUnavailableFault;
import org.ggf.namespaces._2005._12.ws_dai.GetDataResourcePropertyDocumentRequest;
import org.ggf.namespaces._2005._12.ws_dai.GetResourceListRequest;
import org.ggf.namespaces._2005._12.ws_dai.GetResourceListResponse;
import org.ggf.namespaces._2005._12.ws_dai.InvalidResourceNameFault;
import org.ggf.namespaces._2005._12.ws_dai.NotAuthorizedFault;
import org.ggf.namespaces._2005._12.ws_dai.ServiceBusyFault;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.datasource.webservice.WSDAIRAccessServiceClient;
import uk.ac.manchester.cs.snee.datasource.webservice.WSDAIRSourceWrapperImpl;
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
		
		Document document = DocumentBuilderFactory
			.newInstance()
			.newDocumentBuilder()
			.parse(new InputSource(new StringReader(newNode)));
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
				"wsdai:OGSAAccessCCODataResource"));
		verifyAll();
	}

	@Test@Ignore
	public void testGetSchema() 
	throws SNEEDataSourceException, SchemaMetadataException, 
	TypeMappingException, InvalidResourceNameFault,
	DataResourceUnavailableFault, NotAuthorizedFault, ServiceBusyFault 
	{
		String propDocFile = WSDAIRSourceWrapperImplTest.class.getClassLoader().
			getResource("etc/cco_sqlPropertyDoc.xml").getFile();
		GetDataResourcePropertyDocumentRequest request = 
			new GetDataResourcePropertyDocumentRequest();
		expect(mockWsdairClient.getPropertyDocument(request)).andReturn(null);
		assertNull(wsdairSource.getSchema(WSDAI_OGSA_ACCESS_CCO_DATA_RESOURCE));
	}

	@Test@Ignore
	public void testExecuteQuery() {
		fail("Not yet implemented");
	}

}
