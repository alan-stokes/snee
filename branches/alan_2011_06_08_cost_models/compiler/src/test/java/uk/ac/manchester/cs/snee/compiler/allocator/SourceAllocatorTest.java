package uk.ac.manchester.cs.snee.compiler.allocator;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

import java.util.Iterator;

import org.apache.log4j.PropertyConfigurator;
import org.easymock.classextension.EasyMockSupport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.manchester.cs.snee.compiler.queryplan.DLAF;
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
	private SourceMetadataAbstract mockSourceMetadata;

	@SuppressWarnings("unchecked")
  @Before
	public void setUp() throws Exception {
		mockLaf = createMock(LAF.class);
		mockIterator = createMock(Iterator.class);
		mockAcquireOperator = createMock(AcquireOperator.class);
		mockReceiveOperator = createMock(ReceiveOperator.class);
		mockScanOperator = createMock(ScanOperator.class);
		mockSourceMetadata = createMock(SourceMetadataAbstract.class);
	}

	@After
	public void tearDown() throws Exception {
	}

	final SourceMetadataAbstract mockSourceMetadata2 =
		createMock(SourceMetadataAbstract.class);
	
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
	public void testAllocateSources_sensorQuery_oneAcquire() 
	throws SourceAllocatorException {
		expect(mockLaf.getID()).andReturn("sensorQuery").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("sensorQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).andReturn(true).andReturn(false);
		expect(mockIterator.next()).andReturn(mockAcquireOperator);
		expect(mockAcquireOperator.getSource()).andReturn(mockSourceMetadata);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf);
		assertEquals(1, dlaf.getSources().size());
		verifyAll();
	}
	
	/**
	 * Tests two acquires on the same sensor network source
	 * @throws SourceAllocatorException
	 */
	@Test
	public void testAllocateSources_sensorQuery_twoAcquire() 
	throws SourceAllocatorException {
		expect(mockLaf.getID()).andReturn("sensorQuery").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("sensorQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).andReturn(true).andReturn(true).
			andReturn(false);
		expect(mockIterator.next()).andReturn(mockAcquireOperator).times(2);
		expect(mockAcquireOperator.getSource()).
			andReturn(mockSourceMetadata).times(2);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf);
		assertEquals(1, dlaf.getSources().size());
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
		expect(mockAcquireOperator.getSource()).
			andReturn(mockSourceMetadata);
		expect(mockSourceMetadata.getSourceType()).
			andReturn(SourceType.SENSOR_NETWORK);
		expect(mockAcquireOperator.getSource()).
			andReturn(mockSourceMetadata2);
		expect(mockSourceMetadata2.getSourceType()).
			andReturn(SourceType.SENSOR_NETWORK);
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
		expect(mockIterator.next()).andReturn(mockReceiveOperator);
		expect(mockReceiveOperator.getSource()).andReturn(mockSourceMetadata);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf);
		assertEquals(1, dlaf.getSources().size());
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
		expect(mockAcquireOperator.getSource()).
			andReturn(mockSourceMetadata).andReturn(mockSourceMetadata2);
		expect(mockSourceMetadata.getSourceType()).
			andReturn(SourceType.PULL_STREAM_SERVICE);
		expect(mockSourceMetadata2.getSourceType()).
			andReturn(SourceType.PULL_STREAM_SERVICE);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf);
		assertEquals(2, dlaf.getSources().size());
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
		expect(mockReceiveOperator.getSource()).andReturn(mockSourceMetadata);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf);
		assertEquals(1, dlaf.getSources().size());
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
		expect(mockReceiveOperator.getSource()).
			andReturn(mockSourceMetadata).andReturn(mockSourceMetadata2);
		expect(mockSourceMetadata.getSourceType()).
			andReturn(SourceType.UDP_SOURCE);
		expect(mockSourceMetadata2.getSourceType()).
			andReturn(SourceType.UDP_SOURCE);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf);
		assertEquals(2, dlaf.getSources().size());
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
		expect(mockScanOperator.getSource()).andReturn(mockSourceMetadata);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf);
		assertEquals(1, dlaf.getSources().size());
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
		expect(mockScanOperator.getSource()).
			andReturn(mockSourceMetadata).
			andReturn(mockSourceMetadata2);
		expect(mockSourceMetadata.getSourceType()).
			andReturn(SourceType.RELATIONAL);
		expect(mockSourceMetadata2.getSourceType()).
			andReturn(SourceType.RELATIONAL);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf);
		assertEquals(2, dlaf.getSources().size());
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
		expect(mockScanOperator.getSource()).andReturn(mockSourceMetadata);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf);
		assertEquals(1, dlaf.getSources().size());
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
		expect(mockScanOperator.getSource()).
			andReturn(mockSourceMetadata).andReturn(mockSourceMetadata2);
		expect(mockSourceMetadata.getSourceType()).
			andReturn(SourceType.WSDAIR);
		expect(mockSourceMetadata2.getSourceType()).
			andReturn(SourceType.WSDAIR);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf);
		assertEquals(2, dlaf.getSources().size());
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
		expect(mockReceiveOperator.getSource()).andReturn(mockSourceMetadata);
		expect(mockScanOperator.getSource()).andReturn(mockSourceMetadata2);
		expect(mockSourceMetadata.getSourceType()).
			andReturn(SourceType.WSDAIR);
		expect(mockSourceMetadata2.getSourceType()).
			andReturn(SourceType.UDP_SOURCE);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf);
		assertEquals(2, dlaf.getSources().size());
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
		expect(mockReceiveOperator.getSource()).andReturn(mockSourceMetadata);
		expect(mockScanOperator.getSource()).andReturn(mockSourceMetadata2);
		expect(mockSourceMetadata.getSourceType()).
			andReturn(SourceType.WSDAIR);
		expect(mockSourceMetadata2.getSourceType()).
			andReturn(SourceType.PUSH_STREAM_SERVICE);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf);
		assertEquals(2, dlaf.getSources().size());
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
		expect(mockAcquireOperator.getSource()).andReturn(mockSourceMetadata);
		expect(mockScanOperator.getSource()).andReturn(mockSourceMetadata2);
		expect(mockSourceMetadata.getSourceType()).
			andReturn(SourceType.WSDAIR);
		expect(mockSourceMetadata2.getSourceType()).
			andReturn(SourceType.PULL_STREAM_SERVICE);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf);
		assertEquals(2, dlaf.getSources().size());
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
		expect(mockScanOperator.getSource()).andReturn(mockSourceMetadata);
		expect(mockAcquireOperator.getSource()).andReturn(mockSourceMetadata2);
		expect(mockSourceMetadata.getSourceType()).
			andReturn(SourceType.WSDAIR);
		expect(mockSourceMetadata2.getSourceType()).
			andReturn(SourceType.SENSOR_NETWORK);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf);
		assertEquals(2, dlaf.getSources().size());
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
		expect(mockAcquireOperator.getSource()).andReturn(mockSourceMetadata);
		expect(mockReceiveOperator.getSource()).andReturn(mockSourceMetadata2);
		expect(mockSourceMetadata.getSourceType()).
			andReturn(SourceType.SENSOR_NETWORK);
		expect(mockSourceMetadata2.getSourceType()).
			andReturn(SourceType.PUSH_STREAM_SERVICE);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf);
		assertEquals(2, dlaf.getSources().size());
		verifyAll();
	}

}
