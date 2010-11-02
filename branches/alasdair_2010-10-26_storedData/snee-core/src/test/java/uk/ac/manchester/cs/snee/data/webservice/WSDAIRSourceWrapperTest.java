package uk.ac.manchester.cs.snee.data.webservice;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.net.MalformedURLException;

import org.apache.log4j.PropertyConfigurator;
import org.easymock.classextension.EasyMockSupport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.common.UtilsException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.Types;
import eu.semsorgrid4env.service.wsdai.DataResourceUnavailableFault;
import eu.semsorgrid4env.service.wsdai.GetDataResourcePropertyDocumentRequest;
import eu.semsorgrid4env.service.wsdai.InvalidResourceNameFault;
import eu.semsorgrid4env.service.wsdai.NotAuthorizedFault;
import eu.semsorgrid4env.service.wsdai.ServiceBusyFault;

public class WSDAIRSourceWrapperTest extends EasyMockSupport {
	
	private static final String WSDAI_OGSA_ACCESS_CCO_DATA_RESOURCE = "wsdai:OGSAAccessCCODataResource";

	private WSDAIRSourceWrapper wsdairSource;

	// Mock Objects
	final WSDAIRAccessServiceClient mockWsdairClient = 
		createMock(WSDAIRAccessServiceClient.class);
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				WSDAIRSourceWrapperTest.class.getClassLoader().getResource(
				"etc/log4j.properties"));
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		// Configure the types
		String typesFileLoc = 
			Utils.validateFileLocation("etc/Types.xml");
		Types types = new Types(typesFileLoc);

		// Create a source wrapper to test
		wsdairSource = new WSDAIRSourceWrapper("http://example.org", types) {
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

	@Test@Ignore
	public void testGetResourceNames() {
		fail("Not yet implemented");
	}

	@Test@Ignore
	public void testGetSchema() 
	throws SNEEDataSourceException, SchemaMetadataException, 
	TypeMappingException, UtilsException, InvalidResourceNameFault,
	DataResourceUnavailableFault, NotAuthorizedFault, ServiceBusyFault 
	{
		String propDocFile = 
			Utils.validateFileLocation("etc/cco_sqlPropertyDoc.xml");
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
