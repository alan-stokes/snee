package uk.ac.manchester.cs.snee.compiler.allocator;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;
import org.easymock.classextension.EasyMockSupport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import uk.ac.manchester.cs.snee.ResultStoreImplTest;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceMetadata;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceType;
import uk.ac.manchester.cs.snee.compiler.queryplan.LAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.evaluator.types.TaggedTuple;
import uk.ac.manchester.cs.snee.operators.logical.AcquireOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.ReceiveOperator;
import uk.ac.manchester.cs.snee.operators.logical.ScanOperator;

public class SourceAllocatorTest extends EasyMockSupport {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				ResultStoreImplTest.class.getClassLoader().
				getResource("etc/log4j.properties"));
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	final LAF mockLaf = createMock(LAF.class);
	final Iterator<LogicalOperator> mockIterator = 
		createMock(Iterator.class);
	final AcquireOperator mockAcquireOperator = 
		createMock(AcquireOperator.class);
	final ReceiveOperator mockReceiveOperator = 
		createMock(ReceiveOperator.class);
	final ScanOperator mockScanOperator = 
		createMock(ScanOperator.class);
	final List<SourceMetadata> mockList =
		createMock(List.class);
	final SourceMetadata mockSourceMetadata =
		createMock(SourceMetadata.class);
	
	@Test(expected=SourceAllocatorException.class)
	public void testAllocateSources_noOperators() 
	throws SourceAllocatorException {
		expect(mockLaf.getName()).andReturn("noQuery");
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
		expect(mockLaf.getName()).andReturn("sensorQuery");
		expect(mockLaf.getQueryName()).andReturn("sensorQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).andReturn(true).andReturn(false);
		expect(mockIterator.next()).andReturn(mockAcquireOperator);
		expect(mockAcquireOperator.getSources()).andReturn(mockList);
		Object[] mockObjArray = {mockSourceMetadata};
		expect(mockList.toArray()).andReturn(mockObjArray);
		expect(mockSourceMetadata.getSourceType()).
			andReturn(SourceType.SENSOR_NETWORK);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		allocator.allocateSources(mockLaf);
		verifyAll();
	}
	
	@Test(expected=SourceAllocatorException.class)
	public void testAllocateSources_twoSensorQuery() 
	throws SourceAllocatorException {
		expect(mockLaf.getName()).andReturn("twoSensorQuery");
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
		expect(mockLaf.getName()).andReturn("pullStream");
		expect(mockLaf.getQueryName()).andReturn("pullStream-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).andReturn(true).andReturn(false);
		expect(mockIterator.next()).andReturn(mockReceiveOperator);
		expect(mockReceiveOperator.getSources()).andReturn(mockList);
		Object[] mockObjArray = {mockSourceMetadata};
		expect(mockList.toArray()).andReturn(mockObjArray);
		expect(mockSourceMetadata.getSourceType()).
			andReturn(SourceType.PULL_STREAM_SERVICE);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		allocator.allocateSources(mockLaf);
		verifyAll();
	}
	
	@Test(expected=SourceAllocatorException.class)
	public void testAllocateSources_twoPullStream() 
	throws SourceAllocatorException {
		expect(mockLaf.getName()).andReturn("twoPullStreamQuery");
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
	
	@Test(expected=SourceAllocatorException.class)
	public void testAllocateSources_scanQuery() 
	throws SourceAllocatorException {
		expect(mockLaf.getName()).andReturn("scanQuery");
		expect(mockLaf.getQueryName()).andReturn("scanQuery-LAF");
		expect(mockLaf.operatorIterator(TraversalOrder.PRE_ORDER)).
			andReturn(mockIterator);
		expect(mockIterator.hasNext()).andReturn(true);
		expect(mockIterator.next()).andReturn(mockScanOperator);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		allocator.allocateSources(mockLaf);
		verifyAll();
	}
	
	@Test(expected=SourceAllocatorException.class)
	public void testAllocateSources_mixedQuery() 
	throws SourceAllocatorException {
		expect(mockLaf.getName()).andReturn("mixedQuery");
		expect(mockLaf.getQueryName()).andReturn("mixedQuery-LAF");
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
			andReturn(SourceType.SENSOR_NETWORK).
			andReturn(SourceType.PUSH_STREAM_SERVICE);
		replayAll();
		SourceAllocator allocator = new SourceAllocator();
		allocator.allocateSources(mockLaf);
		verifyAll();
	}

}
