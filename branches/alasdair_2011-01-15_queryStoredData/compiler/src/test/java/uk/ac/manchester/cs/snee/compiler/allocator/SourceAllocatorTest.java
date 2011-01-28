package uk.ac.manchester.cs.snee.compiler.allocator;

import static org.easymock.EasyMock.expect;

import java.util.Iterator;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;
import org.easymock.classextension.EasyMockSupport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.manchester.cs.snee.compiler.queryplan.LAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;
import uk.ac.manchester.cs.snee.operators.logical.AcquireOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.ReceiveOperator;
import uk.ac.manchester.cs.snee.operators.logical.ScanOperator;

public class SourceAllocatorTest extends EasyMockSupport {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				SourceAllocatorTest.class.getClassLoader().
				getResource("etc/log4j.properties"));
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	private LAF mockLaf;
	private Iterator<LogicalOperator> mockIterator;
	private AcquireOperator mockAcquireOperator;
	private ReceiveOperator mockReceiveOperator;
	private ScanOperator mockScanOperator;
	private List<SourceMetadataAbstract> mockList;
	private SourceMetadataAbstract mockSourceMetadata;

	@Before
	public void setUp() throws Exception {
		mockLaf = createMock(LAF.class);
		mockIterator = createMock(Iterator.class);
		mockAcquireOperator = createMock(AcquireOperator.class);
		mockReceiveOperator = createMock(ReceiveOperator.class);
		mockScanOperator = createMock(ScanOperator.class);
		mockList = createMock(List.class);
		mockSourceMetadata = createMock(SourceMetadataAbstract.class);
	}

	@After
	public void tearDown() throws Exception {
	}

	
	@Test(expected=SourceAllocatorException.class)
	public void testAllocateSources_noOperators() 
	throws SourceAllocatorException {
		expect(mockLaf.getID()).andReturn("noQuery").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("noQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).andReturn(false);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		allocator.allocateSources(mockLaf);
		verifyAll();
	}
	
	@Test
	public void testAllocateSources_sensorQuery() 
	throws SourceAllocatorException {
		expect(mockLaf.getID()).andReturn("sensorQuery").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("sensorQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).andReturn(true).andReturn(false);
		expect(mockIterator.next()).andReturn(mockAcquireOperator);
		expect(mockAcquireOperator.getSources()).andReturn(mockList);
		Object[] mockObjArray = {mockSourceMetadata};
		expect(mockList.toArray()).andReturn(mockObjArray);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		allocator.allocateSources(mockLaf);
		verifyAll();
	}
	
	@Test(expected=SourceAllocatorException.class)
	public void testAllocateSources_twoSensorQuery() 
	throws SourceAllocatorException {
		expect(mockLaf.getID()).andReturn("twoSensorQuery").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("twoSensorQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).
			andReturn(true).times(2).andReturn(false);
		expect(mockIterator.next()).
			andReturn(mockAcquireOperator).times(2);
		expect(mockAcquireOperator.getSources()).
			andReturn(mockList).times(2);
		Object[] mockObjArray = {mockSourceMetadata};
		expect(mockList.toArray()).andReturn(mockObjArray).times(2);
		expect(mockSourceMetadata.getSourceType()).
			andReturn(SourceType.SENSOR_NETWORK).times(2);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		allocator.allocateSources(mockLaf);
		verifyAll();
	}
	
	@Test
	public void testAllocateSources_pullStreamSources() 
	throws SourceAllocatorException {
		expect(mockLaf.getID()).andReturn("pullStream").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("pullStream-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).andReturn(true).andReturn(false);
		expect(mockIterator.next()).andReturn(mockAcquireOperator);
		expect(mockAcquireOperator.getSources()).andReturn(mockList);
		Object[] mockObjArray = {mockSourceMetadata};
		expect(mockList.toArray()).andReturn(mockObjArray);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		allocator.allocateSources(mockLaf);
		verifyAll();
	}
	
	@Test
	public void testAllocateSources_twoPullStream() 
	throws SourceAllocatorException {
		expect(mockLaf.getID()).andReturn("twoPullStreamQuery").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("twoPullStreamQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).
			andReturn(true).times(2).andReturn(false);
		expect(mockIterator.next()).
			andReturn(mockAcquireOperator).times(2);
		expect(mockAcquireOperator.getSources()).
			andReturn(mockList).times(2);
		Object[] mockObjArray = {mockSourceMetadata};
		expect(mockList.toArray()).andReturn(mockObjArray).times(2);
		expect(mockSourceMetadata.getSourceType()).
			andReturn(SourceType.PULL_STREAM_SERVICE).times(2);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		allocator.allocateSources(mockLaf);
		verifyAll();
	}
	
	@Test
	public void testAllocateSources_UDPSource() 
	throws SourceAllocatorException {
		expect(mockLaf.getID()).andReturn("UDPSource").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("UDPSource-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).andReturn(true).andReturn(false);
		expect(mockIterator.next()).andReturn(mockReceiveOperator);
		expect(mockReceiveOperator.getSources()).andReturn(mockList);
		Object[] mockObjArray = {mockSourceMetadata};
		expect(mockList.toArray()).andReturn(mockObjArray);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		allocator.allocateSources(mockLaf);
		verifyAll();
	}
	
	@Test
	public void testAllocateSources_twoUDPSources() 
	throws SourceAllocatorException {
		expect(mockLaf.getID()).andReturn("twoUDPSource").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("twoUDPSources-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).
			andReturn(true).times(2).andReturn(false);
		expect(mockIterator.next()).
			andReturn(mockReceiveOperator).times(2);
		expect(mockReceiveOperator.getSources()).
			andReturn(mockList).times(2);
		Object[] mockObjArray = {mockSourceMetadata};
		expect(mockList.toArray()).andReturn(mockObjArray).times(2);
		expect(mockSourceMetadata.getSourceType()).
			andReturn(SourceType.UDP_SOURCE).times(2);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		allocator.allocateSources(mockLaf);
		verifyAll();
	}
	
	@Test
	public void testAllocateSources_scanRelationalQuery() 
	throws SourceAllocatorException {
		expect(mockLaf.getID()).andReturn("scanQuery").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("scanQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).andReturn(true).andReturn(false);
		expect(mockIterator.next()).andReturn(mockScanOperator);
		expect(mockScanOperator.getSources()).andReturn(mockList);
		Object[] mockObjArray = {mockSourceMetadata};
		expect(mockList.toArray()).andReturn(mockObjArray);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		allocator.allocateSources(mockLaf);
		verifyAll();
	}
	
	@Test
	public void testAllocateSources_twoRelationalSources() 
	throws SourceAllocatorException {
		expect(mockLaf.getID()).andReturn("scanQuery").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("scanQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).andReturn(true).times(2).andReturn(false);
		expect(mockIterator.next()).andReturn(mockScanOperator).times(2);
		expect(mockScanOperator.getSources()).andReturn(mockList).times(2);
		Object[] mockObjArray = {mockSourceMetadata};
		expect(mockList.toArray()).andReturn(mockObjArray).times(2);
		expect(mockSourceMetadata.getSourceType()).
		andReturn(SourceType.RELATIONAL).times(2);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		allocator.allocateSources(mockLaf);
		verifyAll();
	}

	@Test
	public void testAllocateSources_scanWSDAIRQuery() 
	throws SourceAllocatorException {
		expect(mockLaf.getID()).andReturn("scanQuery").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("scanQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).andReturn(true).andReturn(false);
		expect(mockIterator.next()).andReturn(mockScanOperator);
		expect(mockScanOperator.getSources()).andReturn(mockList);
		Object[] mockObjArray = {mockSourceMetadata};
		expect(mockList.toArray()).andReturn(mockObjArray);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		allocator.allocateSources(mockLaf);
		verifyAll();
	}
	
	@Test
	public void testAllocateSources_twoWSDAIRSources() 
	throws SourceAllocatorException {
		expect(mockLaf.getID()).andReturn("scanQuery").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("scanQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).andReturn(true).times(2).andReturn(false);
		expect(mockIterator.next()).andReturn(mockScanOperator).times(2);
		expect(mockScanOperator.getSources()).andReturn(mockList).times(2);
		Object[] mockObjArray = {mockSourceMetadata};
		expect(mockList.toArray()).andReturn(mockObjArray).times(2);
		expect(mockSourceMetadata.getSourceType()).
		andReturn(SourceType.WSDAIR).times(2);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		allocator.allocateSources(mockLaf);
		verifyAll();
	}
	
	@Test
	public void testAllocateSources_mixedUDPWSDAIRQuery() 
	throws SourceAllocatorException {
		expect(mockLaf.getID()).andReturn("mixedUDPWSDAIRQuery").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("mixedUDPWSDAIRQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).
			andReturn(true).times(2).andReturn(false);
		expect(mockIterator.next()).
			andReturn(mockReceiveOperator).andReturn(mockScanOperator);
		expect(mockReceiveOperator.getSources()).andReturn(mockList);
		expect(mockScanOperator.getSources()).andReturn(mockList);
		Object[] mockObjArray = {mockSourceMetadata};
		expect(mockList.toArray()).andReturn(mockObjArray).times(2);
		expect(mockSourceMetadata.getSourceType()).
			andReturn(SourceType.WSDAIR).
			andReturn(SourceType.UDP_SOURCE);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		allocator.allocateSources(mockLaf);
		verifyAll();
	}
	
	@Test
	public void testAllocateSources_mixedPushWSDAIRQuery() 
	throws SourceAllocatorException {
		expect(mockLaf.getID()).andReturn("mixedPushWSDAIRQuery").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("mixedPushWSDAIRQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).
			andReturn(true).times(2).andReturn(false);
		expect(mockIterator.next()).andReturn(mockScanOperator).
			andReturn(mockReceiveOperator);
		expect(mockReceiveOperator.getSources()).andReturn(mockList);
		expect(mockScanOperator.getSources()).andReturn(mockList);
		Object[] mockObjArray = {mockSourceMetadata};
		expect(mockList.toArray()).andReturn(mockObjArray).times(2);
		expect(mockSourceMetadata.getSourceType()).
			andReturn(SourceType.WSDAIR).
			andReturn(SourceType.PUSH_STREAM_SERVICE);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		allocator.allocateSources(mockLaf);
		verifyAll();
	}
	
	@Test
	public void testAllocateSources_mixedPullWSDAIRQuery() 
	throws SourceAllocatorException {
		expect(mockLaf.getID()).andReturn("mixedPullWSDAIRQuery").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("mixedPullWSDAIRQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).
			andReturn(true).times(2).andReturn(false);
		expect(mockIterator.next()).andReturn(mockScanOperator).
			andReturn(mockAcquireOperator);
		expect(mockAcquireOperator.getSources()).andReturn(mockList);
		expect(mockScanOperator.getSources()).andReturn(mockList);
		Object[] mockObjArray = {mockSourceMetadata};
		expect(mockList.toArray()).andReturn(mockObjArray).times(2);
		expect(mockSourceMetadata.getSourceType()).
			andReturn(SourceType.WSDAIR).
			andReturn(SourceType.PULL_STREAM_SERVICE);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		allocator.allocateSources(mockLaf);
		verifyAll();
	}
	
	@Test(expected=SourceAllocatorException.class)
	public void testAllocateSources_mixedSensorWSDAIRQuery() 
	throws SourceAllocatorException {
		expect(mockLaf.getID()).andReturn("mixedSensorWSDAIRQuery").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("mixedSensorWSDAIRQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).
			andReturn(true).times(2).andReturn(false);
		expect(mockIterator.next()).
			andReturn(mockScanOperator).andReturn(mockAcquireOperator);
		expect(mockScanOperator.getSources()).andReturn(mockList);
		expect(mockAcquireOperator.getSources()).andReturn(mockList);
		Object[] mockObjArray = {mockSourceMetadata};
		expect(mockList.toArray()).andReturn(mockObjArray).times(2);
		expect(mockSourceMetadata.getSourceType()).
			andReturn(SourceType.WSDAIR).
			andReturn(SourceType.SENSOR_NETWORK);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		allocator.allocateSources(mockLaf);
		verifyAll();
	}
	
	@Test(expected=SourceAllocatorException.class)
	public void testAllocateSources_mixedPushSensorQuery() 
	throws SourceAllocatorException {
		expect(mockLaf.getID()).andReturn("mixedPushSensorQuery").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("mixedPushSensorQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).
			andReturn(true).times(2).andReturn(false);
		expect(mockIterator.next()).
			andReturn(mockAcquireOperator).andReturn(mockReceiveOperator);
		expect(mockAcquireOperator.getSources()).andReturn(mockList);
		expect(mockReceiveOperator.getSources()).andReturn(mockList);
		Object[] mockObjArray = {mockSourceMetadata};
		expect(mockList.toArray()).andReturn(mockObjArray).times(2);
		expect(mockSourceMetadata.getSourceType()).
			andReturn(SourceType.SENSOR_NETWORK).
			andReturn(SourceType.PUSH_STREAM_SERVICE);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		allocator.allocateSources(mockLaf);
		verifyAll();
	}

}
