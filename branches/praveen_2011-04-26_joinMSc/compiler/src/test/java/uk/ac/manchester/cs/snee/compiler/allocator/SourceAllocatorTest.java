package uk.ac.manchester.cs.snee.compiler.allocator;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;

import java.util.Iterator;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.easymock.classextension.EasyMockSupport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.params.QueryParameters;
import uk.ac.manchester.cs.snee.compiler.params.qos.QoSExpectations;
import uk.ac.manchester.cs.snee.compiler.queryplan.DLAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.LAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;
import uk.ac.manchester.cs.snee.metadata.source.StreamingSourceMetadataAbstract;
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

	private LAF mockLaf = createMock(LAF.class);
	private Iterator<LogicalOperator> mockIterator = createMock(Iterator.class);
	private AcquireOperator mockAcquireOperator = 
		createMock(AcquireOperator.class);
	private ReceiveOperator mockReceiveOperator = 
		createMock(ReceiveOperator.class);
	private ScanOperator mockScanOperator = createMock(ScanOperator.class);
	private SourceMetadataAbstract mockSourceMetadata = 
		createMock(SourceMetadataAbstract.class);
	private SourceMetadataAbstract mockSourceMetadata2 = 
		createMock(SourceMetadataAbstract.class);
	private StreamingSourceMetadataAbstract mockStreamSourceMetadata =
		createMock(StreamingSourceMetadataAbstract.class);
	final StreamingSourceMetadataAbstract mockStreamSourceMetadata2 =
		createMock(StreamingSourceMetadataAbstract.class);
	private QoSExpectations qos;

	@Before
	public void setUp() throws Exception {
		Properties props = new Properties();
		props.setProperty(SNEEPropertyNames.INPUTS_TYPES_FILE, "etc/Types.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_UNITS_FILE, "etc/units.xml");
		props.setProperty(SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR, "output");
		props.setProperty(SNEEPropertyNames.SNCB_PERFORM_METADATA_COLLECTION, "false");
		props.setProperty(SNEEPropertyNames.SNCB_GENERATE_COMBINED_IMAGE, "false");
		SNEEProperties.initialise(props);

		QueryParameters parameters = 
			new QueryParameters(1, "etc/query-parameters.xml");
		if (parameters != null) {
			qos = parameters.getQoS();
		}
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test(expected=SourceAllocatorException.class)
	public void testAllocateSources_noOperators() 
	throws SourceAllocatorException, SourceMetadataException {
		expect(mockLaf.getID()).andReturn("noQuery").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("noQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).andReturn(false);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		allocator.allocateSources(mockLaf, qos);
		verifyAll();
	}
	
	@Test
	public void testAllocateSources_sensorQuery_oneAcquire() 
	throws SourceAllocatorException, SourceMetadataException {
		expect(mockLaf.getID()).andReturn("sensorQuery").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("sensorQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockLaf.operatorIterator(TraversalOrder.POST_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).andReturn(true).andReturn(false)
			.andReturn(true).andReturn(false);
		expect(mockIterator.next()).andReturn(mockAcquireOperator).times(2);
		expect(mockAcquireOperator.getSource()).andReturn(mockSourceMetadata);
		mockAcquireOperator.setSourceRate(10000.0);
		expectLastCall().times(1);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf, qos);
		assertEquals(1, dlaf.getSources().size());
		verifyAll();
	}
	
	/**
	 * Tests two acquires on the same sensor network source
	 * @throws SourceAllocatorException
	 * @throws SourceMetadataException 
	 */
	@Test
	public void testAllocateSources_sensorQuery_twoAcquire() 
	throws SourceAllocatorException, SourceMetadataException {
		expect(mockLaf.getID()).andReturn("sensorQuery").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("sensorQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockLaf.operatorIterator(TraversalOrder.POST_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).andReturn(true).andReturn(true).
			andReturn(false).andReturn(true).andReturn(true).andReturn(false);
		expect(mockIterator.next()).andReturn(mockAcquireOperator).times(4);
		expect(mockAcquireOperator.getSource()).
			andReturn(mockSourceMetadata).times(2);
		mockAcquireOperator.setSourceRate(10000.0);
		expectLastCall().times(2);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf, qos);
		assertEquals(1, dlaf.getSources().size());
		verifyAll();
	}
	
	@Test(expected=SourceAllocatorException.class)
	public void testAllocateSources_twoSensorQuery() 
	throws SourceAllocatorException, SourceMetadataException {
		expect(mockLaf.getID()).andReturn("twoSensorQuery").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("twoSensorQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).
			andReturn(true).times(2).andReturn(false);
		expect(mockIterator.next()).
			andReturn(mockAcquireOperator).times(2);
		expect(mockAcquireOperator.getSource()).
			andReturn(mockStreamSourceMetadata);
		expect(mockStreamSourceMetadata.getSourceType()).
			andReturn(SourceType.SENSOR_NETWORK);
		expect(mockAcquireOperator.getSource()).
			andReturn(mockStreamSourceMetadata2);
		expect(mockStreamSourceMetadata2.getSourceType()).
			andReturn(SourceType.SENSOR_NETWORK);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		allocator.allocateSources(mockLaf, qos);
		verifyAll();
	}
	
	@Test
	public void testAllocateSources_pullStreamSources() 
	throws SourceAllocatorException, SourceMetadataException {
		expect(mockLaf.getID()).andReturn("pullStream").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("pullStream-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockLaf.operatorIterator(TraversalOrder.POST_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext())
			.andReturn(true).andReturn(false)
			.andReturn(true).andReturn(false);
		expect(mockIterator.next()).andReturn(mockAcquireOperator).times(2);
		expect(mockAcquireOperator.getSource()).
			andReturn(mockStreamSourceMetadata);
		mockAcquireOperator.setSourceRate(10000.0);
		expectLastCall().times(1);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf, qos);
		assertEquals(1, dlaf.getSources().size());
		verifyAll();
	}
	
	@Test
	public void testAllocateSources_twoPullStream() 
	throws SourceAllocatorException, SourceMetadataException {
		expect(mockLaf.getID()).andReturn("twoPullStreamQuery").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("twoPullStreamQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockLaf.operatorIterator(TraversalOrder.POST_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).
			andReturn(true).times(2).andReturn(false).
			andReturn(true).times(2).andReturn(false);
		expect(mockIterator.next()).andReturn(mockAcquireOperator).times(4);
		expect(mockAcquireOperator.getSource()).
			andReturn(mockStreamSourceMetadata).
			andReturn(mockStreamSourceMetadata2);
		expect(mockStreamSourceMetadata.getSourceType()).
			andReturn(SourceType.PULL_STREAM_SERVICE);
		expect(mockStreamSourceMetadata2.getSourceType()).
			andReturn(SourceType.PULL_STREAM_SERVICE);
		mockAcquireOperator.setSourceRate(10000.0);
		expectLastCall().times(2);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf, qos);
		assertEquals(2, dlaf.getSources().size());
		verifyAll();
	}
	
	@Test
	public void testAllocateSources_UDPSource() 
	throws SourceAllocatorException, SourceMetadataException {
		expect(mockLaf.getID()).andReturn("UDPSource").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("UDPSource-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockLaf.operatorIterator(TraversalOrder.POST_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).
			andReturn(true).andReturn(false).
			andReturn(true).andReturn(false);
		expect(mockIterator.next()).andReturn(mockReceiveOperator).times(2);
		expect(mockReceiveOperator.getSource()).
			andReturn(mockStreamSourceMetadata).times(2);
		expect(mockReceiveOperator.getExtentName()).andReturn("name");
		expect(mockStreamSourceMetadata.getRate("name")).andReturn(2.0);
		mockReceiveOperator.setSourceRate(2.0);
		expectLastCall().times(1);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf, qos);
		assertEquals(1, dlaf.getSources().size());
		verifyAll();
	}
	
	@Test
	public void testAllocateSources_twoUDPSources() 
	throws SourceAllocatorException, SourceMetadataException {
		expect(mockLaf.getID()).andReturn("twoUDPSource").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("twoUDPSources-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockLaf.operatorIterator(TraversalOrder.POST_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).
			andReturn(true).times(2).andReturn(false).
			andReturn(true).times(2).andReturn(false);
		expect(mockIterator.next()).
			andReturn(mockReceiveOperator).times(4);
		expect(mockReceiveOperator.getSource()).
			andReturn(mockStreamSourceMetadata).
			andReturn(mockStreamSourceMetadata2).
			andReturn(mockStreamSourceMetadata).
			andReturn(mockStreamSourceMetadata2);
		expect(mockStreamSourceMetadata.getSourceType()).
			andReturn(SourceType.UDP_SOURCE);
		expect(mockStreamSourceMetadata2.getSourceType()).
			andReturn(SourceType.UDP_SOURCE);
		expect(mockReceiveOperator.getExtentName()).andReturn("name").times(2);
		expect(mockStreamSourceMetadata.getRate("name")).andReturn(2.0);
		expect(mockStreamSourceMetadata2.getRate("name")).andReturn(2.0);
		mockReceiveOperator.setSourceRate(2.0);
		expectLastCall().times(2);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf, qos);
		assertEquals(2, dlaf.getSources().size());
		verifyAll();
	}
	
	@Test
	public void testAllocateSources_scanRelationalQuery() 
	throws SourceAllocatorException, SourceMetadataException {
		expect(mockLaf.getID()).andReturn("scanQuery").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("scanQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockLaf.operatorIterator(TraversalOrder.POST_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext())
			.andReturn(true).andReturn(false)
			.andReturn(true).andReturn(false);
		expect(mockIterator.next()).andReturn(mockScanOperator).times(2);
		expect(mockScanOperator.getSource())
			.andReturn(mockSourceMetadata).times(1);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf, qos);
		assertEquals(1, dlaf.getSources().size());
		verifyAll();
	}
	
	@Test
	public void testAllocateSources_twoRelationalSources() 
	throws SourceAllocatorException, SourceMetadataException {
		expect(mockLaf.getID()).andReturn("scanQuery").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("scanQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockLaf.operatorIterator(TraversalOrder.POST_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext())
			.andReturn(true).times(2).andReturn(false)
			.andReturn(true).times(2).andReturn(false);
		expect(mockIterator.next()).andReturn(mockScanOperator).times(4);
		expect(mockScanOperator.getSource()).
			andReturn(mockSourceMetadata).
			andReturn(mockSourceMetadata2);
		expect(mockSourceMetadata.getSourceType())
			.andReturn(SourceType.RELATIONAL);
		expect(mockSourceMetadata2.getSourceType())
			.andReturn(SourceType.RELATIONAL);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf, qos);
		assertEquals(2, dlaf.getSources().size());
		verifyAll();
	}

	@Test
	public void testAllocateSources_scanWSDAIRQuery() 
	throws SourceAllocatorException, SourceMetadataException {
		expect(mockLaf.getID()).andReturn("scanQuery").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("scanQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockLaf.operatorIterator(TraversalOrder.POST_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext())
			.andReturn(true).andReturn(false)
			.andReturn(true).andReturn(false);
		expect(mockIterator.next()).andReturn(mockScanOperator).times(2);
		expect(mockScanOperator.getSource()).andReturn(mockSourceMetadata);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf, qos);
		assertEquals(1, dlaf.getSources().size());
		verifyAll();
	}
	
	@Test
	public void testAllocateSources_twoWSDAIRSources() 
	throws SourceAllocatorException, SourceMetadataException {
		expect(mockLaf.getID()).andReturn("scanQuery").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("scanQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockLaf.operatorIterator(TraversalOrder.POST_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext())
			.andReturn(true).times(2).andReturn(false)
			.andReturn(true).times(2).andReturn(false);
		expect(mockIterator.next()).andReturn(mockScanOperator).times(4);
		expect(mockScanOperator.getSource()).
			andReturn(mockSourceMetadata).andReturn(mockSourceMetadata2);
		expect(mockSourceMetadata.getSourceType()).
			andReturn(SourceType.WSDAIR);
		expect(mockSourceMetadata2.getSourceType()).
			andReturn(SourceType.WSDAIR);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf, qos);
		assertEquals(2, dlaf.getSources().size());
		verifyAll();
	}
	
	@Test
	public void testAllocateSources_mixedUDPWSDAIRQuery() 
	throws SourceAllocatorException, SourceMetadataException {
		expect(mockLaf.getID()).andReturn("mixedUDPWSDAIRQuery").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("mixedUDPWSDAIRQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockLaf.operatorIterator(TraversalOrder.POST_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).
			andReturn(true).times(2).andReturn(false)
			.andReturn(true).times(2).andReturn(false);
		expect(mockIterator.next()).
			andReturn(mockReceiveOperator).andReturn(mockScanOperator)
			.andReturn(mockReceiveOperator).andReturn(mockScanOperator);
		expect(mockReceiveOperator.getSource())
			.andReturn(mockStreamSourceMetadata).times(2);
		expect(mockScanOperator.getSource()).andReturn(mockSourceMetadata);
		expect(mockSourceMetadata.getSourceType()).
			andReturn(SourceType.WSDAIR);
		expect(mockStreamSourceMetadata.getSourceType()).
			andReturn(SourceType.UDP_SOURCE);
		expect(mockReceiveOperator.getExtentName()).andReturn("name");
		expect(mockStreamSourceMetadata.getRate("name")).andReturn(1.0);
		mockReceiveOperator.setSourceRate(1.0);
		expectLastCall().times(1);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf, qos);
		assertEquals(2, dlaf.getSources().size());
		verifyAll();
	}
	
	@Test
	public void testAllocateSources_mixedPushWSDAIRQuery() 
	throws SourceAllocatorException, SourceMetadataException {
		expect(mockLaf.getID()).andReturn("mixedPushWSDAIRQuery").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("mixedPushWSDAIRQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockLaf.operatorIterator(TraversalOrder.POST_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).
			andReturn(true).times(2).andReturn(false)
			.andReturn(true).times(2).andReturn(false);
		expect(mockIterator.next())
			.andReturn(mockScanOperator).andReturn(mockReceiveOperator)
			.andReturn(mockScanOperator).andReturn(mockReceiveOperator);
		expect(mockReceiveOperator.getSource())
			.andReturn(mockStreamSourceMetadata).times(2);
		expect(mockScanOperator.getSource()).andReturn(mockSourceMetadata);
		expect(mockSourceMetadata.getSourceType()).
			andReturn(SourceType.WSDAIR);
		expect(mockStreamSourceMetadata.getSourceType()).
			andReturn(SourceType.PUSH_STREAM_SERVICE);
		expect(mockReceiveOperator.getExtentName()).andReturn("name");
		expect(mockStreamSourceMetadata.getRate("name")).andReturn(1.0);
		mockReceiveOperator.setSourceRate(1.0);
		expectLastCall().times(1);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf, qos);
		assertEquals(2, dlaf.getSources().size());
		verifyAll();
	}
	
	@Test
	public void testAllocateSources_mixedPullWSDAIRQuery() 
	throws SourceAllocatorException, SourceMetadataException {
		expect(mockLaf.getID()).andReturn("mixedPullWSDAIRQuery").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("mixedPullWSDAIRQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockLaf.operatorIterator(TraversalOrder.POST_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).
			andReturn(true).times(2).andReturn(false)
			.andReturn(true).times(2).andReturn(false);
		expect(mockIterator.next())
			.andReturn(mockScanOperator).andReturn(mockAcquireOperator)
			.andReturn(mockScanOperator).andReturn(mockAcquireOperator);
		expect(mockAcquireOperator.getSource())
			.andReturn(mockStreamSourceMetadata);
		expect(mockScanOperator.getSource()).andReturn(mockSourceMetadata);
		expect(mockSourceMetadata.getSourceType()).
			andReturn(SourceType.WSDAIR);
		expect(mockStreamSourceMetadata.getSourceType()).
			andReturn(SourceType.PULL_STREAM_SERVICE);
		mockAcquireOperator.setSourceRate(10000.0);
		expectLastCall().times(1);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf, qos);
		assertEquals(2, dlaf.getSources().size());
		verifyAll();
	}
	
	@Test(expected=SourceAllocatorException.class)
	public void testAllocateSources_mixedSensorWSDAIRQuery() 
	throws SourceAllocatorException, SourceMetadataException {
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
		DLAF dlaf = allocator.allocateSources(mockLaf, qos);
		assertEquals(2, dlaf.getSources().size());
		verifyAll();
	}
	
	@Test(expected=SourceAllocatorException.class)
	public void testAllocateSources_mixedPushSensorQuery() 
	throws SourceAllocatorException, SourceMetadataException {
		expect(mockLaf.getID()).andReturn("mixedPushSensorQuery").times(0, 2);
		expect(mockLaf.getQueryName()).andReturn("mixedPushSensorQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).
			andReturn(true).times(2).andReturn(false);
		expect(mockIterator.next()).
			andReturn(mockAcquireOperator).andReturn(mockReceiveOperator);
		expect(mockAcquireOperator.getSource()).andReturn(mockStreamSourceMetadata);
		expect(mockReceiveOperator.getSource()).andReturn(mockStreamSourceMetadata2);
		expect(mockStreamSourceMetadata.getSourceType()).
			andReturn(SourceType.SENSOR_NETWORK);
		expect(mockStreamSourceMetadata2.getSourceType()).
			andReturn(SourceType.PUSH_STREAM_SERVICE);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		DLAF dlaf = allocator.allocateSources(mockLaf, qos);
		assertEquals(2, dlaf.getSources().size());
		verifyAll();
	}

}
