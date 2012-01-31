package uk.ac.manchester.cs.snee.metadata.source;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.apache.log4j.PropertyConfigurator;
import org.easymock.classextension.EasyMockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.manchester.cs.snee.datasource.webservice.PullSourceWrapper;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentDoesNotExistException;
import uk.ac.manchester.cs.snee.metadata.source.WebServiceSourceMetadata;

public class WebServiceSourceMetadataTest extends EasyMockSupport {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				WebServiceSourceMetadataTest.class.getClassLoader().
				getResource("etc/log4j.properties"));
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetServiceUrl() {
		@SuppressWarnings("unchecked")
    List<String> mockList = createMock(List.class);
		@SuppressWarnings("unchecked")
    Map<String, String> mockMap = createMock(Map.class);
		PullSourceWrapper mockSource = createMock(PullSourceWrapper.class);

		//Record
		expect(mockSource.getSourceType()).andReturn(SourceType.PULL_STREAM_SERVICE);

		//Test
		replayAll();
		WebServiceSourceMetadata serviceMetadata = 
			new WebServiceSourceMetadata("testService", 
				mockList, "http://localhost", mockMap, mockSource);
		assertEquals("http://localhost", serviceMetadata.getServiceUrl());
		verifyAll();
	}

	@Test(expected=ExtentDoesNotExistException.class)
	public void testGetResourceName_invalidStreamName() 
	throws ExtentDoesNotExistException {
		String streamName = "testStream";
		String resourceName = "test:pull:resource";
		@SuppressWarnings("unchecked")
    List<String> mockList = createMock(List.class);
		@SuppressWarnings("unchecked")
    Map<String, String> mockMap = createMock(Map.class);
		PullSourceWrapper mockSource = createMock(PullSourceWrapper.class);

		//Record
		expect(mockSource.getSourceType()).andReturn(SourceType.PULL_STREAM_SERVICE);
		expect(mockMap.containsKey(streamName)).andReturn(false);
		expect(mockMap.get(streamName)).andReturn(resourceName);

		//Test
		replayAll();
		WebServiceSourceMetadata serviceMetadata = 
			new WebServiceSourceMetadata("testService", 
				mockList, "http://localhost", mockMap, mockSource);
		serviceMetadata.getResourceName(streamName);
		verifyAll();
	}

	@Test
	public void testGetResourceName_validStreamName() 
	throws ExtentDoesNotExistException {
		String streamName = "testStream";
		String resourceName = "test:pull:resource";
		@SuppressWarnings("unchecked")
    List<String> mockList = createMock(List.class);
		@SuppressWarnings("unchecked")
    Map<String, String> mockMap = createMock(Map.class);
		PullSourceWrapper mockSource = createMock(PullSourceWrapper.class);

		//Record
		expect(mockSource.getSourceType()).andReturn(SourceType.PULL_STREAM_SERVICE);
		expect(mockMap.containsKey(streamName)).andReturn(true);
		expect(mockMap.get(streamName)).andReturn(resourceName);

		//Test
		replayAll();
		WebServiceSourceMetadata serviceMetadata = 
			new WebServiceSourceMetadata("testService", 
				mockList, "http://localhost", mockMap, mockSource);
		assertEquals(resourceName, serviceMetadata.getResourceName(streamName));
		verifyAll();
	}

	@Test
	public void testGetSource() {
		@SuppressWarnings("unchecked")
    List<String> mockList = createMock(List.class);
		@SuppressWarnings("unchecked")
    Map<String, String> mockMap = createMock(Map.class);
		PullSourceWrapper mockSource = createMock(PullSourceWrapper.class);

		//Record
		expect(mockSource.getSourceType()).andReturn(SourceType.PULL_STREAM_SERVICE);

		//Test
		replayAll();
		WebServiceSourceMetadata serviceMetadata = 
			new WebServiceSourceMetadata("testService", 
				mockList, "http://localhost", mockMap, mockSource);
		assertEquals(mockSource, serviceMetadata.getSource());
		verifyAll();
	}
	
}
