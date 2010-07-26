package uk.ac.manchester.cs.snee.compiler.metadata.source;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.apache.log4j.PropertyConfigurator;
import org.easymock.classextension.EasyMockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentDoesNotExistException;
import uk.ac.manchester.cs.snee.data.webservice.PullSourceWrapper;

public class WebServiceSourceMetadataTest extends EasyMockSupport {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				WebServiceSourceMetadataTest.class.getClassLoader().
				getResource("etc/log4j.properties"));
	}

	private WebServiceSourceMetadata serviceMetadata;
	private List<String> mockList;
	private Map<String, String> mockMap;
	private PullSourceWrapper mockSource;

	@Before
	public void setUp() throws Exception {
		mockList = createMock(List.class);
		mockMap = createMock(Map.class);
		mockSource = createMock(PullSourceWrapper.class);
		serviceMetadata = new WebServiceSourceMetadata("testService", 
				mockList, "http://localhost", mockMap, mockSource);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test@Ignore
	public void testGetServiceUrl() {
		assertEquals("http://localhost", serviceMetadata.getServiceUrl());
	}

	@Ignore
	@Test(expected=ExtentDoesNotExistException.class)
	public void testGetResourceName_invalidStreamName() 
	throws ExtentDoesNotExistException {
		String streamName = "testStream";
		//Record
		expect(mockMap.containsKey(streamName )).andReturn(false);
		//Test
		replayAll();
		serviceMetadata.getResourceName(streamName);
		verifyAll();
	}

	@Test@Ignore
	public void testGetResourceName_validStreamName() 
	throws ExtentDoesNotExistException {
		String streamName = "testStream";
		String resourceName = "test:pull:resource";
		//Record
		expect(mockMap.containsKey(streamName)).andReturn(true);
		expect(mockMap.get(streamName)).andReturn(resourceName);
		//Test
		replayAll();
		assertEquals(resourceName, serviceMetadata.getResourceName(streamName));
		verifyAll();
	}

}
