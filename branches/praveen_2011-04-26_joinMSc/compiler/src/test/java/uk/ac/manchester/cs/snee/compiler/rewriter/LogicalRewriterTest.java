package uk.ac.manchester.cs.snee.compiler.rewriter;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertTrue;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.easymock.classextension.EasyMockSupport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.common.UtilsException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.LAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.LAFUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.DataAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.IntLiteral;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiType;
import uk.ac.manchester.cs.snee.compiler.translator.TranslatorTest;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.Types;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;
import uk.ac.manchester.cs.snee.metadata.source.StreamingSourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.operators.logical.AcquireOperator;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.JoinOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.ReceiveOperator;
import uk.ac.manchester.cs.snee.operators.logical.SelectOperator;
import uk.ac.manchester.cs.snee.operators.logical.WindowOperator;
import uk.ac.manchester.cs.snee.sncb.SNCBException;

public class LogicalRewriterTest extends EasyMockSupport {

	Logger logger = Logger.getLogger(this.getClass().getName());

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				TranslatorTest.class.getClassLoader().getResource(
						"etc/log4j.properties"));
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	private LogicalRewriter rewriter;
	private AttributeType boolType;
	private LAF laf;
	private Types types;

	@Before
	public void setUp() throws Exception {
		Properties props = new Properties();
		props.setProperty(SNEEPropertyNames.INPUTS_TYPES_FILE, "etc/Types.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_UNITS_FILE, "etc/units.xml");
		props.setProperty(SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR, "output");
		props.setProperty(SNEEPropertyNames.GENERAL_DELETE_OLD_FILES, "true");
		props.setProperty(SNEEPropertyNames.GENERATE_QEP_IMAGES, "true");
		props.setProperty(SNEEPropertyNames.CONVERT_QEP_IMAGES, "false");
		SNEEProperties.initialise(props);
		MetadataManager metadata = new MetadataManager(null);
		String typesFileLoc = 
			Utils.validateFileLocation("etc/Types.xml");
		types = new Types(typesFileLoc);
		boolType = types.getType("boolean");
		rewriter = new LogicalRewriter(metadata);
	}

	@After
	public void tearDown() throws Exception {
		if (SNEEProperties.getBoolSetting(SNEEPropertyNames.GENERATE_QEP_IMAGES)) {
			Utils.checkDirectory("output/" + laf.getQueryName() + "/query-plan", true);
			if (logger.isTraceEnabled()) {
				logger.trace("Generating graph image " + laf.getID());
			}
			new LAFUtils(laf).generateGraphImage();
		}
	}
	
	private LogicalOperator testOperator(Iterator<LogicalOperator> iterator, 
			String exOpName) {
		LogicalOperator op = iterator.next();
		String opName = op.getOperatorName();
		assertTrue("Expected " + exOpName + " but instead " + opName, 
				exOpName.equals(opName));
		return op;
	}

	/**
	 * Test on the simplest form of query
	 * Receive -> deliver
	 * No affect
	 * @throws SourceMetadataException 
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws OptimizationException 
	 * @throws SNEEConfigurationException 
	 * @throws AssertionError 
	 */
	@Test
	public void testPushSelectionDown_selectStarQuery() 
	throws SourceMetadataException, SchemaMetadataException, 
	TypeMappingException, OptimizationException, AssertionError,
	SNEEConfigurationException {
		ExtentMetadata mockExtent = createMock(ExtentMetadata.class);
		StreamingSourceMetadataAbstract mockSource = 
			createMock(StreamingSourceMetadataAbstract.class);
		List<Attribute> attrList = new ArrayList<Attribute>();
		
		expect(mockExtent.getExtentName()).andReturn("streamName").anyTimes();
		expect(mockExtent.getAttributes()).andReturn(attrList);
		expect(mockExtent.getCardinality()).andReturn(1);
				
		expect(mockSource.getSourceName()).andReturn("sourceName").anyTimes();
		expect(mockSource.getSourceType()).andReturn(SourceType.PULL_STREAM_SERVICE);
		expect(mockSource.getRate("streamName")).andReturn(2.0);
				
		replayAll();

		LogicalOperator receiveOp = 
			new ReceiveOperator(mockExtent, mockSource, boolType);
		LogicalOperator deliverOp = 
			new DeliverOperator(receiveOp, boolType);

		laf = new LAF(deliverOp, "select-star");
		rewriter.pushSelectionDown(laf);
		Iterator<LogicalOperator> opIt = 
			laf.operatorIterator(TraversalOrder.POST_ORDER);
		testOperator(opIt, "RECEIVE");
		testOperator(opIt, "DELIVER");
		verifyAll();
	}

	/**
	 * Test on the simplest form of select query
	 * Receive -> select -> deliver
	 * No affect
	 * @throws TypeMappingException 
	 * @throws AssertionError 
	 * @throws SchemaMetadataException 
	 * @throws SourceMetadataException 
	 * @throws OptimizationException 
	 * @throws SNEEConfigurationException 
	 */
	@Test
	public void testPushSelectionDown_selectQuery() 
	throws SchemaMetadataException, AssertionError, TypeMappingException,
	SourceMetadataException, OptimizationException, SNEEConfigurationException {
		ExtentMetadata mockExtent = createMock(ExtentMetadata.class);
		StreamingSourceMetadataAbstract mockSource = 
			createMock(StreamingSourceMetadataAbstract.class);
		Expression mockPredicate = createMock(Expression.class);
		List<Attribute> attrList = new ArrayList<Attribute>();
		
		expect(mockExtent.getExtentName()).andReturn("streamName").anyTimes();
		expect(mockExtent.getAttributes()).andReturn(attrList);
		expect(mockExtent.getCardinality()).andReturn(1);
				
		expect(mockSource.getSourceName()).andReturn("sourceName").anyTimes();
		expect(mockSource.getSourceType()).andReturn(SourceType.PULL_STREAM_SERVICE);
		expect(mockSource.getRate("streamName")).andReturn(2.0);
		
		expect(mockPredicate.getType()).andReturn(boolType).times(2);
		
		replayAll();

		LogicalOperator receiveOp = 
			new ReceiveOperator(mockExtent, mockSource, boolType);
		LogicalOperator selectOp = 
			new SelectOperator(mockPredicate, receiveOp, boolType);
		LogicalOperator deliverOp = 
			new DeliverOperator(selectOp, boolType);

		laf = new LAF(deliverOp, "select-query");
		rewriter.pushSelectionDown(laf);
		Iterator<LogicalOperator> opIt = 
			laf.operatorIterator(TraversalOrder.POST_ORDER);
		testOperator(opIt, "RECEIVE");
		testOperator(opIt, "SELECT");
		testOperator(opIt, "DELIVER");
		verifyAll();
	}

	/**
	 * Test on select query with time window
	 * Receive -> window -> select -> deliver
	 * select pushed below time window
	 * @throws SourceMetadataException 
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws OptimizationException 
	 * @throws SNEEConfigurationException 
	 * @throws AssertionError 
	 */
	@Test
	public void testPushSelectionDown_receiveTimeWindowSelectQuery()
	throws SchemaMetadataException, TypeMappingException, 
	SourceMetadataException, OptimizationException, AssertionError,
	SNEEConfigurationException {
		ExtentMetadata mockExtent = createMock(ExtentMetadata.class);
		StreamingSourceMetadataAbstract mockSource = 
			createMock(StreamingSourceMetadataAbstract.class);
		Attribute mockAttribute = createMock(Attribute.class);
		AttributeType mockType = createMock(AttributeType.class);
		List<Attribute> attrList = new ArrayList<Attribute>();
		
		expect(mockExtent.getExtentName()).andReturn("streamName");
		expect(mockExtent.getAttributes()).andReturn(attrList);
		expect(mockExtent.getCardinality()).andReturn(1);
				
		expect(mockSource.getSourceName()).andReturn("sourceName").anyTimes();
		expect(mockSource.getSourceType())
			.andReturn(SourceType.PULL_STREAM_SERVICE);
		expect(mockSource.getRate("streamName")).andReturn(2.0);
		
		expect(mockAttribute.getAttributeSchemaName())
			.andReturn("integerColumn").anyTimes();
		expect(mockAttribute.getAttributeDisplayName())
			.andReturn("integerColumn").anyTimes();
		expect(mockAttribute.getExtentName())
			.andReturn("streamName").anyTimes();
		expect(mockAttribute.getType()).andReturn(mockType).anyTimes();
		expect(mockType.getName()).andReturn("integer").anyTimes();

		replayAll();

		LogicalOperator receiveOp = 
			new ReceiveOperator(mockExtent, mockSource, boolType);
		LogicalOperator windowOp = 
			new WindowOperator(0, 0, true, 0, 0, receiveOp, boolType);
		Expression[] expressions = new Expression[2];
		expressions[0] = new DataAttribute(mockAttribute);
		expressions[1] = new IntLiteral(42, types.getType("integer"));
		Expression selectPredicate = new MultiExpression(expressions, MultiType.LESSTHANEQUALS, boolType);
		LogicalOperator selectOp = 
			new SelectOperator(selectPredicate, windowOp, boolType);
		LogicalOperator deliverOp = 
			new DeliverOperator(selectOp, boolType);

		laf = new LAF(deliverOp, "receive-time-select");
		rewriter.pushSelectionDown(laf);
		Iterator<LogicalOperator> opIt = 
			laf.operatorIterator(TraversalOrder.POST_ORDER);
		testOperator(opIt, "RECEIVE");
		testOperator(opIt, "SELECT");
		testOperator(opIt, "WINDOW");
		testOperator(opIt, "DELIVER");
		verifyAll();
	}

	/**
	 * Test on select query with row window
	 * Receive -> window -> select -> deliver
	 * select cannot be pushed below row window
	 * @throws SourceMetadataException 
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws OptimizationException 
	 * @throws SNEEConfigurationException 
	 * @throws AssertionError 
	 */
	@Test
	public void testPushSelectionDown_receiveRowWindowSelectQuery()
	throws SchemaMetadataException, TypeMappingException, 
	SourceMetadataException, OptimizationException, AssertionError, 
	SNEEConfigurationException {
		ExtentMetadata mockExtent = createMock(ExtentMetadata.class);
		StreamingSourceMetadataAbstract mockSource = 
			createMock(StreamingSourceMetadataAbstract.class);
		Attribute mockAttribute = createMock(Attribute.class);
		AttributeType mockType = createMock(AttributeType.class);
		List<Attribute> attrList = new ArrayList<Attribute>();
		
		expect(mockExtent.getExtentName()).andReturn("streamName");
		expect(mockExtent.getAttributes()).andReturn(attrList);
		expect(mockExtent.getCardinality()).andReturn(1);
				
		expect(mockSource.getSourceName()).andReturn("sourceName").anyTimes();
		expect(mockSource.getSourceType())
			.andReturn(SourceType.PULL_STREAM_SERVICE);
		expect(mockSource.getRate("streamName")).andReturn(2.0);
		
		expect(mockAttribute.getAttributeSchemaName())
			.andReturn("integerColumn").anyTimes();
		expect(mockAttribute.getAttributeDisplayName())
			.andReturn("integerColumn").anyTimes();
		expect(mockAttribute.getExtentName())
			.andReturn("streamName").anyTimes();
		expect(mockAttribute.getType()).andReturn(mockType).anyTimes();
		expect(mockType.getName()).andReturn("integer").anyTimes();

		replayAll();

		LogicalOperator receiveOp = 
			new ReceiveOperator(mockExtent, mockSource, boolType);
		LogicalOperator windowOp = 
			new WindowOperator(0, 0, false, 0, 0, receiveOp, boolType);
		Expression[] expressions = new Expression[2];
		expressions[0] = new DataAttribute(mockAttribute);
		expressions[1] = new IntLiteral(42, types.getType("integer"));
		Expression selectPredicate = new MultiExpression(expressions, MultiType.LESSTHANEQUALS, boolType);
		LogicalOperator selectOp = 
			new SelectOperator(selectPredicate, windowOp, boolType);
		LogicalOperator deliverOp = 
			new DeliverOperator(selectOp, boolType);

		laf = new LAF(deliverOp, "receive-row-select");
		rewriter.pushSelectionDown(laf);
		Iterator<LogicalOperator> opIt = 
			laf.operatorIterator(TraversalOrder.POST_ORDER);
		testOperator(opIt, "RECEIVE");
		testOperator(opIt, "WINDOW");
		testOperator(opIt, "SELECT");
		testOperator(opIt, "DELIVER");
		verifyAll();
	}

	/**
	 * Test on select time window query over an acquire source, combine=true
	 * Acquire -> window -> select -> deliver
	 * select moves into the acquire operator
	 * Acquire -> window -> deliver
	 * @throws SourceMetadataException 
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws OptimizationException 
	 * @throws SNEEConfigurationException 
	 * @throws SNCBException 
	 * @throws CostParametersException 
	 * @throws SNEEDataSourceException 
	 * @throws TopologyReaderException 
	 * @throws UnsupportedAttributeTypeException 
	 * @throws MetadataException 
	 * @throws MalformedURLException 
	 * @throws UtilsException 
	 */
	@Test
	public void testPushSelectionDown_acquireTimeWindowSelectQueryCombine()
	throws SchemaMetadataException, TypeMappingException, 
	SourceMetadataException, OptimizationException, SNEEConfigurationException, MalformedURLException, MetadataException, UnsupportedAttributeTypeException, TopologyReaderException, SNEEDataSourceException, CostParametersException, SNCBException, UtilsException {
		Properties props = new Properties();
		props.setProperty(SNEEPropertyNames.INPUTS_TYPES_FILE, "etc/Types.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_UNITS_FILE, "etc/units.xml");
		props.setProperty(SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR, "output");
		props.setProperty(SNEEPropertyNames.GENERAL_DELETE_OLD_FILES, "true");
		props.setProperty(SNEEPropertyNames.LOGICAL_REWRITER_COMBINE_ACQUIRE_SELECT, "true");
		props.setProperty(SNEEPropertyNames.GENERATE_QEP_IMAGES, "true");
		props.setProperty(SNEEPropertyNames.CONVERT_QEP_IMAGES, "false");
		SNEEProperties.initialise(props);
		MetadataManager metadata = new MetadataManager(null);
		String typesFileLoc = 
			Utils.validateFileLocation("etc/Types.xml");
		Types types = new Types(typesFileLoc);
		AttributeType boolType = types.getType("boolean");
		LogicalRewriter rewriter = new LogicalRewriter(metadata);

		ExtentMetadata mockExtent = createMock(ExtentMetadata.class);
		StreamingSourceMetadataAbstract mockSource = 
			createMock(StreamingSourceMetadataAbstract.class);
		Attribute mockAttribute = createMock(Attribute.class);
		AttributeType mockType = createMock(AttributeType.class);
		List<Attribute> attrList = new ArrayList<Attribute>();
		
		expect(mockExtent.getExtentName()).andReturn("streamName");
		expect(mockExtent.getAttributes()).andReturn(attrList).times(2);
		expect(mockExtent.getCardinality()).andReturn(1);
				
		expect(mockSource.getSourceName()).andReturn("sourceName").anyTimes();
		expect(mockSource.getSourceType()).andReturn(SourceType.SENSOR_NETWORK);
		
		expect(mockAttribute.getAttributeSchemaName())
			.andReturn("integerColumn");
		expect(mockAttribute.getAttributeDisplayName())
			.andReturn("integerColumn");
		expect(mockAttribute.getExtentName()).andReturn("sensorStream");
		expect(mockAttribute.getType()).andReturn(mockType).anyTimes();
		expect(mockType.getName()).andReturn("integer").anyTimes();

		replayAll();
		
		LogicalOperator acquireOp = 
			new AcquireOperator(mockExtent, types, mockSource, boolType);
		LogicalOperator windowOp = 
			new WindowOperator(0, 0, true, 0, 0, acquireOp, boolType);
		Expression[] expressions = new Expression[2];
		expressions[0] = new DataAttribute(mockAttribute);
		expressions[1] = new IntLiteral(42, types.getType("integer"));
		Expression selectPredicate = new MultiExpression(expressions, MultiType.LESSTHANEQUALS, boolType);
		LogicalOperator selectOp = 
			new SelectOperator(selectPredicate, windowOp, boolType);
		LogicalOperator deliverOp = 
			new DeliverOperator(selectOp, boolType);

		laf = new LAF(deliverOp, "acquire-time-select");
		rewriter.pushSelectionDown(laf);
		rewriter.removeUnrequiredOperators(laf);
		Iterator<LogicalOperator> opIt = 
			laf.operatorIterator(TraversalOrder.POST_ORDER);
		testOperator(opIt, "ACQUIRE");
		testOperator(opIt, "WINDOW");
		testOperator(opIt, "DELIVER");
		verifyAll();
	}

	/**
	 * Test on select time window query over an acquire source, combine=false
	 * Acquire -> window -> select -> deliver
	 * one select moves below window, but remains separate from acquire
	 * Acquire -> select -> window -> deliver
	 * @throws SourceMetadataException 
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws OptimizationException 
	 * @throws SNEEConfigurationException 
	 * @throws SNCBException 
	 * @throws CostParametersException 
	 * @throws SNEEDataSourceException 
	 * @throws TopologyReaderException 
	 * @throws UnsupportedAttributeTypeException 
	 * @throws MetadataException 
	 * @throws MalformedURLException 
	 * @throws UtilsException 
	 */
	@Test
	public void testPushSelectionDown_acquireTimeWindowSelectQueryDontCombine()
	throws SchemaMetadataException, TypeMappingException, 
	SourceMetadataException, OptimizationException, SNEEConfigurationException, MalformedURLException, MetadataException, UnsupportedAttributeTypeException, TopologyReaderException, SNEEDataSourceException, CostParametersException, SNCBException, UtilsException {
		Properties props = new Properties();
		props.setProperty(SNEEPropertyNames.INPUTS_TYPES_FILE, "etc/Types.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_UNITS_FILE, "etc/units.xml");
		props.setProperty(SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR, "output");
		props.setProperty(SNEEPropertyNames.GENERAL_DELETE_OLD_FILES, "true");
		props.setProperty(SNEEPropertyNames.LOGICAL_REWRITER_COMBINE_ACQUIRE_SELECT, "false");
		props.setProperty(SNEEPropertyNames.GENERATE_QEP_IMAGES, "false");
		props.setProperty(SNEEPropertyNames.CONVERT_QEP_IMAGES, "false");
		SNEEProperties.initialise(props);
		MetadataManager metadata = new MetadataManager(null);
		String typesFileLoc = 
			Utils.validateFileLocation("etc/Types.xml");
		Types types = new Types(typesFileLoc);
		AttributeType boolType = types.getType("boolean");
		LogicalRewriter rewriter = new LogicalRewriter(metadata);

		ExtentMetadata mockExtent = createMock(ExtentMetadata.class);
		StreamingSourceMetadataAbstract mockSource = 
			createMock(StreamingSourceMetadataAbstract.class);
		Attribute mockAttribute = createMock(Attribute.class);
		AttributeType mockType = createMock(AttributeType.class);
		List<Attribute> attrList = new ArrayList<Attribute>();
		
		expect(mockExtent.getExtentName()).andReturn("streamName");
		expect(mockExtent.getAttributes()).andReturn(attrList).times(2);
		expect(mockExtent.getCardinality()).andReturn(1);
				
		expect(mockSource.getSourceName()).andReturn("sourceName").anyTimes();
		expect(mockSource.getSourceType()).andReturn(SourceType.SENSOR_NETWORK);
		
		expect(mockAttribute.getAttributeSchemaName())
			.andReturn("integerColumn");
		expect(mockAttribute.getAttributeDisplayName())
			.andReturn("integerColumn");
		expect(mockAttribute.getExtentName()).andReturn("sensorStream");
		expect(mockAttribute.getType()).andReturn(mockType).anyTimes();
		expect(mockType.getName()).andReturn("integer").anyTimes();

		replayAll();
		
		LogicalOperator acquireOp = 
			new AcquireOperator(mockExtent, types, mockSource, boolType);
		LogicalOperator windowOp = 
			new WindowOperator(0, 0, true, 0, 0, acquireOp, boolType);
		Expression[] expressions = new Expression[2];
		expressions[0] = new DataAttribute(mockAttribute);
		expressions[1] = new IntLiteral(42, types.getType("integer"));
		Expression selectPredicate = new MultiExpression(expressions, MultiType.LESSTHANEQUALS, boolType);
		LogicalOperator selectOp = 
			new SelectOperator(selectPredicate, windowOp, boolType);
		LogicalOperator deliverOp = 
			new DeliverOperator(selectOp, boolType);

		laf = new LAF(deliverOp, "acquire-time-select");
		rewriter.pushSelectionDown(laf);
		rewriter.removeUnrequiredOperators(laf);
		Iterator<LogicalOperator> opIt = 
			laf.operatorIterator(TraversalOrder.POST_ORDER);
		testOperator(opIt, "ACQUIRE");
		testOperator(opIt, "SELECT");
		testOperator(opIt, "WINDOW");
		testOperator(opIt, "DELIVER");
		verifyAll();
	}

	/**
	 * Test on select row window query over an acquire source, combine=true
	 * Acquire -> window -> select -> deliver
	 * select cannot move below window
	 * Acquire -> window -> select -> deliver
	 * @throws SourceMetadataException 
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws OptimizationException 
	 * @throws SNEEConfigurationException 
	 * @throws SNCBException 
	 * @throws CostParametersException 
	 * @throws SNEEDataSourceException 
	 * @throws TopologyReaderException 
	 * @throws UnsupportedAttributeTypeException 
	 * @throws MetadataException 
	 * @throws MalformedURLException 
	 * @throws UtilsException 
	 */
	@Test
	public void testPushSelectionDown_acquireRowWindowSelectQueryCombine()
	throws SchemaMetadataException, TypeMappingException, 
	SourceMetadataException, OptimizationException, SNEEConfigurationException, MalformedURLException, MetadataException, UnsupportedAttributeTypeException, TopologyReaderException, SNEEDataSourceException, CostParametersException, SNCBException, UtilsException {
		Properties props = new Properties();
		props.setProperty(SNEEPropertyNames.INPUTS_TYPES_FILE, "etc/Types.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_UNITS_FILE, "etc/units.xml");
		props.setProperty(SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR, "output");
		props.setProperty(SNEEPropertyNames.GENERAL_DELETE_OLD_FILES, "true");
		props.setProperty(SNEEPropertyNames.LOGICAL_REWRITER_COMBINE_ACQUIRE_SELECT, "true");
		props.setProperty(SNEEPropertyNames.GENERATE_QEP_IMAGES, "true");
		props.setProperty(SNEEPropertyNames.CONVERT_QEP_IMAGES, "false");
		SNEEProperties.initialise(props);
		MetadataManager metadata = new MetadataManager(null);
		String typesFileLoc = 
			Utils.validateFileLocation("etc/Types.xml");
		Types types = new Types(typesFileLoc);
		AttributeType boolType = types.getType("boolean");
		LogicalRewriter rewriter = new LogicalRewriter(metadata);

		ExtentMetadata mockExtent = createMock(ExtentMetadata.class);
		StreamingSourceMetadataAbstract mockSource = 
			createMock(StreamingSourceMetadataAbstract.class);
		Attribute mockAttribute = createMock(Attribute.class);
		AttributeType mockType = createMock(AttributeType.class);
		List<Attribute> attrList = new ArrayList<Attribute>();
		
		expect(mockExtent.getExtentName()).andReturn("streamName");
		expect(mockExtent.getAttributes()).andReturn(attrList).times(2);
		expect(mockExtent.getCardinality()).andReturn(1);
				
		expect(mockSource.getSourceName()).andReturn("sourceName").anyTimes();
		expect(mockSource.getSourceType()).andReturn(SourceType.SENSOR_NETWORK);
		
		expect(mockAttribute.getAttributeSchemaName())
			.andReturn("integerColumn");
		expect(mockAttribute.getAttributeDisplayName())
			.andReturn("integerColumn");
		expect(mockAttribute.getExtentName()).andReturn("sensorStream");
		expect(mockAttribute.getType()).andReturn(mockType).anyTimes();
		expect(mockType.getName()).andReturn("integer").anyTimes();

		replayAll();
		
		LogicalOperator acquireOp = 
			new AcquireOperator(mockExtent, types, mockSource, boolType);
		LogicalOperator windowOp = 
			new WindowOperator(0, 0, false, 0, 0, acquireOp, boolType);
		Expression[] expressions = new Expression[2];
		expressions[0] = new DataAttribute(mockAttribute);
		expressions[1] = new IntLiteral(42, types.getType("integer"));
		Expression selectPredicate = new MultiExpression(expressions, MultiType.LESSTHANEQUALS, boolType);
		LogicalOperator selectOp = 
			new SelectOperator(selectPredicate, windowOp, boolType);
		LogicalOperator deliverOp = 
			new DeliverOperator(selectOp, boolType);

		laf = new LAF(deliverOp, "acquire-row-select");
		rewriter.pushSelectionDown(laf);
		rewriter.removeUnrequiredOperators(laf);
		Iterator<LogicalOperator> opIt = 
			laf.operatorIterator(TraversalOrder.POST_ORDER);
		testOperator(opIt, "ACQUIRE");
		testOperator(opIt, "WINDOW");
		testOperator(opIt, "SELECT");
		testOperator(opIt, "DELIVER");
		verifyAll();
	}

	/**
	 * Test on select row window query over an acquire source, combine=false
	 * Acquire -> window -> select -> deliver
	 * select cannot move below window
	 * Acquire -> window -> select -> deliver
	 * @throws SourceMetadataException 
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws OptimizationException 
	 * @throws SNEEConfigurationException 
	 * @throws SNCBException 
	 * @throws CostParametersException 
	 * @throws SNEEDataSourceException 
	 * @throws TopologyReaderException 
	 * @throws UnsupportedAttributeTypeException 
	 * @throws MetadataException 
	 * @throws MalformedURLException 
	 * @throws UtilsException 
	 */
	@Test
	public void testPushSelectionDown_acquireRowWindowSelectQueryDontCombine()
	throws SchemaMetadataException, TypeMappingException, 
	SourceMetadataException, OptimizationException, SNEEConfigurationException, MalformedURLException, MetadataException, UnsupportedAttributeTypeException, TopologyReaderException, SNEEDataSourceException, CostParametersException, SNCBException, UtilsException {
		Properties props = new Properties();
		props.setProperty(SNEEPropertyNames.INPUTS_TYPES_FILE, "etc/Types.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_UNITS_FILE, "etc/units.xml");
		props.setProperty(SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR, "output");
		props.setProperty(SNEEPropertyNames.GENERAL_DELETE_OLD_FILES, "true");
		props.setProperty(SNEEPropertyNames.LOGICAL_REWRITER_COMBINE_ACQUIRE_SELECT, "false");
		props.setProperty(SNEEPropertyNames.GENERATE_QEP_IMAGES, "false");
		props.setProperty(SNEEPropertyNames.CONVERT_QEP_IMAGES, "false");
		SNEEProperties.initialise(props);
		MetadataManager metadata = new MetadataManager(null);
		String typesFileLoc = 
			Utils.validateFileLocation("etc/Types.xml");
		Types types = new Types(typesFileLoc);
		AttributeType boolType = types.getType("boolean");
		LogicalRewriter rewriter = new LogicalRewriter(metadata);

		ExtentMetadata mockExtent = createMock(ExtentMetadata.class);
		StreamingSourceMetadataAbstract mockSource = 
			createMock(StreamingSourceMetadataAbstract.class);
		Attribute mockAttribute = createMock(Attribute.class);
		AttributeType mockType = createMock(AttributeType.class);
		List<Attribute> attrList = new ArrayList<Attribute>();
		
		expect(mockExtent.getExtentName()).andReturn("streamName");
		expect(mockExtent.getAttributes()).andReturn(attrList).times(2);
		expect(mockExtent.getCardinality()).andReturn(1);
				
		expect(mockSource.getSourceName()).andReturn("sourceName").anyTimes();
		expect(mockSource.getSourceType()).andReturn(SourceType.SENSOR_NETWORK);
		
		expect(mockAttribute.getAttributeSchemaName())
			.andReturn("integerColumn");
		expect(mockAttribute.getAttributeDisplayName())
			.andReturn("integerColumn");
		expect(mockAttribute.getExtentName()).andReturn("sensorStream");
		expect(mockAttribute.getType()).andReturn(mockType).anyTimes();
		expect(mockType.getName()).andReturn("integer").anyTimes();

		replayAll();
		
		LogicalOperator acquireOp = 
			new AcquireOperator(mockExtent, types, mockSource, boolType);
		LogicalOperator windowOp = 
			new WindowOperator(0, 0, false, 0, 0, acquireOp, boolType);
		Expression[] expressions = new Expression[2];
		expressions[0] = new DataAttribute(mockAttribute);
		expressions[1] = new IntLiteral(42, types.getType("integer"));
		Expression selectPredicate = new MultiExpression(expressions, MultiType.LESSTHANEQUALS, boolType);
		LogicalOperator selectOp = 
			new SelectOperator(selectPredicate, windowOp, boolType);
		LogicalOperator deliverOp = 
			new DeliverOperator(selectOp, boolType);

		laf = new LAF(deliverOp, "acquire-row-select");
		rewriter.pushSelectionDown(laf);
		rewriter.removeUnrequiredOperators(laf);
		Iterator<LogicalOperator> opIt = 
			laf.operatorIterator(TraversalOrder.POST_ORDER);
		testOperator(opIt, "ACQUIRE");
		testOperator(opIt, "WINDOW");
		testOperator(opIt, "SELECT");
		testOperator(opIt, "DELIVER");
		verifyAll();
	}

	/**
	 * Test on the simplest form of select query
	 * Receive -> select -> select -> deliver
	 * Selects should be combined
	 * @throws SourceMetadataException 
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws OptimizationException 
	 * @throws SNEEConfigurationException 
	 * @throws AssertionError 
	 */
	@Test
	public void testPushSelectionDown_multiSelectQuery()
	throws SchemaMetadataException, TypeMappingException, 
	SourceMetadataException, OptimizationException, AssertionError, 
	SNEEConfigurationException {
		ExtentMetadata mockExtent = createMock(ExtentMetadata.class);
		StreamingSourceMetadataAbstract mockSource = 
			createMock(StreamingSourceMetadataAbstract.class);
		MultiExpression mockPredicate = createMock(MultiExpression.class);
		List<Attribute> attrList = new ArrayList<Attribute>();
		
		expect(mockExtent.getExtentName()).andReturn("streamName").anyTimes();
		expect(mockExtent.getAttributes()).andReturn(attrList);
		expect(mockExtent.getCardinality()).andReturn(1);
				
		expect(mockSource.getSourceName()).andReturn("sourceName").anyTimes();
		expect(mockSource.getSourceType()).andReturn(SourceType.PULL_STREAM_SERVICE);
		expect(mockSource.getRate("streamName")).andReturn(2.0);
		
		expect(mockPredicate.getType()).andReturn(boolType).times(4);
		expect(mockPredicate.combinePredicates(mockPredicate, mockPredicate))
			.andReturn(mockPredicate).anyTimes();
		expect(mockPredicate.getMultiType())
			.andReturn(MultiType.GREATERTHAN).anyTimes();
		expect(mockPredicate.isJoinCondition()).andReturn(false).anyTimes();
		replayAll();

		LogicalOperator receiveOp = 
			new ReceiveOperator(mockExtent, mockSource, boolType);
		LogicalOperator selectOp1 = 
			new SelectOperator(mockPredicate, receiveOp, boolType);
		LogicalOperator selectOp2 = 
			new SelectOperator(mockPredicate, selectOp1, boolType);
		LogicalOperator deliverOp = 
			new DeliverOperator(selectOp2, boolType);

		laf = new LAF(deliverOp, "multi-select");
		rewriter.pushSelectionDown(laf);
		Iterator<LogicalOperator> opIt = 
			laf.operatorIterator(TraversalOrder.POST_ORDER);
		testOperator(opIt, "RECEIVE");
		testOperator(opIt, "SELECT");
		testOperator(opIt, "DELIVER");
		verifyAll();
	}

	/**
	 * Test on the simplest form of select query
	 * Receive -> window -> 
	 * Receive -> window ->
	 * join -> select -> deliver
	 * one select moves below join to left child
	 * @throws SourceMetadataException 
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws OptimizationException 
	 * @throws SNEEConfigurationException 
	 * @throws AssertionError 
	 */
	@Test
	public void testPushSelectionDown_timeWindowJoinQuery()
	throws SchemaMetadataException, TypeMappingException, 
	SourceMetadataException, OptimizationException, AssertionError,
	SNEEConfigurationException {
		ExtentMetadata mockExtent = createMock(ExtentMetadata.class);
		StreamingSourceMetadataAbstract mockSource = 
			createMock(StreamingSourceMetadataAbstract.class);
		Attribute mockAttribute = createMock(Attribute.class);
		Attribute mockAttribute2 = createMock(Attribute.class);
		AttributeType mockType = createMock(AttributeType.class);
		List<Attribute> attrList = new ArrayList<Attribute>();
		
		expect(mockExtent.getExtentName()).andReturn("streamName").times(2);
		expect(mockExtent.getAttributes()).andReturn(attrList).times(2);
		expect(mockExtent.getCardinality()).andReturn(1).times(2);
				
		expect(mockSource.getSourceName()).andReturn("sourceName").anyTimes();
		expect(mockSource.getSourceType())
			.andReturn(SourceType.PULL_STREAM_SERVICE).times(2);
		expect(mockSource.getRate("streamName")).andReturn(2.0).andReturn(1.0);
		
		expect(mockAttribute.getAttributeSchemaName())
			.andReturn("integerColumn").anyTimes();
		expect(mockAttribute.getAttributeDisplayName())
			.andReturn("integerColumn").anyTimes();
		expect(mockAttribute.getExtentName())
			.andReturn("streamLeft").anyTimes();
		expect(mockAttribute.getType()).andReturn(mockType).anyTimes();
		
		expect(mockAttribute2.getAttributeSchemaName()).andReturn("timestamp");
		expect(mockAttribute2.getAttributeDisplayName()).andReturn("timestamp");
		expect(mockAttribute2.getExtentName()).andReturn("streamRight");
		expect(mockAttribute2.getType()).andReturn(mockType).anyTimes();

		expect(mockType.getName()).andReturn("integer").anyTimes();

		replayAll();

		LogicalOperator receiveOpLeft = 
			new ReceiveOperator(mockExtent, mockSource, boolType);
		LogicalOperator windowOpLeft = 
			new WindowOperator(0, 0, true, 0, 0, receiveOpLeft, boolType);
		LogicalOperator receiveOpRight = 
			new ReceiveOperator(mockExtent, mockSource, boolType);
		LogicalOperator windowOpRight = 
			new WindowOperator(0, 0, true, 0, 0, receiveOpRight, boolType);
		LogicalOperator joinOp = 
			new JoinOperator(windowOpLeft, windowOpRight, boolType);		
		Expression[] attributes = new Expression[2];
		attributes[0] = new DataAttribute(mockAttribute);
		attributes[1] = new DataAttribute(mockAttribute2);
		Expression joinPredicate = new MultiExpression(attributes, MultiType.EQUALS, boolType);
		joinPredicate.setIsJoinCondition(true);
		LogicalOperator selectOp1 = 
			new SelectOperator(joinPredicate, joinOp, boolType);
		LogicalOperator deliverOp = 
			new DeliverOperator(selectOp1, boolType);

		laf = new LAF(deliverOp, "join-time");
		rewriter.pushSelectionDown(laf);
		Iterator<LogicalOperator> opIt = 
			laf.operatorIterator(TraversalOrder.POST_ORDER);
		testOperator(opIt, "RECEIVE");
		testOperator(opIt, "WINDOW");
		testOperator(opIt, "RECEIVE");
		testOperator(opIt, "WINDOW");
		testOperator(opIt, "JOIN");
		testOperator(opIt, "SELECT");
		testOperator(opIt, "DELIVER");
		verifyAll();
	}

	/**
	 * Test query
	 * Receive -> window -> 
	 * Receive -> window ->
	 * join -> select -> select -> deliver
	 * one select moves below join to left child
	 * @throws SourceMetadataException 
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws OptimizationException 
	 * @throws SNEEConfigurationException 
	 * @throws AssertionError 
	 */
	@Test
	public void testPushSelectionDown_timeWindowJoinLeftSelectQuery()
	throws SchemaMetadataException, TypeMappingException, 
	SourceMetadataException, OptimizationException, AssertionError, 
	SNEEConfigurationException {
		ExtentMetadata mockExtentLeft = createMock(ExtentMetadata.class);
		ExtentMetadata mockExtentRight = createMock(ExtentMetadata.class);
		StreamingSourceMetadataAbstract mockSource = 
			createMock(StreamingSourceMetadataAbstract.class);
		Attribute mockAttribute = createMock(Attribute.class);
		Attribute mockAttribute1 = createMock(Attribute.class); 
		AttributeType mockType = createMock(AttributeType.class);
		
		expect(mockAttribute.getAttributeSchemaName())
			.andReturn("integerColumn").anyTimes();
		expect(mockAttribute.getAttributeDisplayName())
			.andReturn("integerColumn").anyTimes();
		expect(mockAttribute.getExtentName())
			.andReturn("streamLeft").anyTimes();
		expect(mockAttribute.getType()).andReturn(mockType).anyTimes();
		
		expect(mockAttribute1.getAttributeSchemaName())
			.andReturn("timestamp").anyTimes();
		expect(mockAttribute1.getAttributeDisplayName())
			.andReturn("timestamp").anyTimes();
		expect(mockAttribute1.getExtentName())
			.andReturn("streamRight").anyTimes();
		expect(mockAttribute1.getType()).andReturn(mockType).anyTimes();
		List<Attribute> leftAttrList = new ArrayList<Attribute>();
		leftAttrList.add(mockAttribute);
		List<Attribute> rightAttrList = new ArrayList<Attribute>();
		rightAttrList.add(mockAttribute1);
		
		expect(mockExtentLeft.getExtentName()).andReturn("streamLeft").anyTimes();
		expect(mockExtentLeft.getAttributes()).andReturn(leftAttrList).anyTimes();
		expect(mockExtentLeft.getCardinality()).andReturn(1).anyTimes();

		expect(mockExtentRight.getExtentName()).andReturn("streamRight").anyTimes();
		expect(mockExtentRight.getAttributes()).andReturn(rightAttrList).anyTimes();
		expect(mockExtentRight.getCardinality()).andReturn(1).anyTimes();
				
		expect(mockSource.getSourceName()).andReturn("sourceName").anyTimes();
		expect(mockSource.getSourceType())
			.andReturn(SourceType.PULL_STREAM_SERVICE).times(2);
		expect(mockSource.getRate("streamLeft")).andReturn(2.0);
		expect(mockSource.getRate("streamRight")).andReturn(1.0);

		expect(mockType.getName()).andReturn("integer").anyTimes();

		replayAll();

		LogicalOperator receiveOpLeft = 
			new ReceiveOperator(mockExtentLeft, mockSource, boolType);
		LogicalOperator windowOpLeft = 
			new WindowOperator(0, 0, true, 0, 0, receiveOpLeft, boolType);
		LogicalOperator receiveOpRight = 
			new ReceiveOperator(mockExtentRight, mockSource, boolType);
		LogicalOperator windowOpRight = 
			new WindowOperator(0, 0, true, 0, 0, receiveOpRight, boolType);
		LogicalOperator joinOp = 
			new JoinOperator(windowOpLeft, windowOpRight, boolType);		
		Expression[] attributes = new Expression[2];
		attributes[0] = new DataAttribute(mockAttribute);
		attributes[1] = new DataAttribute(mockAttribute1);
		Expression joinPredicate = new MultiExpression(attributes, MultiType.EQUALS, boolType);
		LogicalOperator selectOp1 = 
			new SelectOperator(joinPredicate, joinOp, boolType);
		Expression[] expressions = new Expression[2];
		expressions[0] = new DataAttribute(mockAttribute);
		expressions[1] = new IntLiteral(42, types.getType("integer"));
		Expression selectPredicate = new MultiExpression(expressions, MultiType.LESSTHANEQUALS, boolType);
		LogicalOperator selectOp2 = 
			new SelectOperator(selectPredicate, selectOp1, boolType);
		LogicalOperator deliverOp = 
			new DeliverOperator(selectOp2, boolType);

		laf = new LAF(deliverOp, "join-time-select");
		rewriter.pushSelectionDown(laf);
		Iterator<LogicalOperator> opIt = 
			laf.operatorIterator(TraversalOrder.POST_ORDER);
		testOperator(opIt, "RECEIVE");
		testOperator(opIt, "SELECT");
		testOperator(opIt, "WINDOW");
		testOperator(opIt, "RECEIVE");
		testOperator(opIt, "WINDOW");
		testOperator(opIt, "JOIN");
		testOperator(opIt, "SELECT");
		testOperator(opIt, "DELIVER");
		verifyAll();
	}

	/**
	 * Test query
	 * Receive -> window -> 
	 * Receive -> window ->
	 * join -> select -> select -> deliver
	 * one select moves below join to right child
	 * @throws SourceMetadataException 
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws OptimizationException 
	 * @throws SNEEConfigurationException 
	 * @throws AssertionError 
	 */
	@Test
	public void testPushSelectionDown_timeWindowJoinRightSelectQuery()
	throws SchemaMetadataException, TypeMappingException, 
	SourceMetadataException, OptimizationException, AssertionError, 
	SNEEConfigurationException {
		ExtentMetadata mockExtent1 = createMock(ExtentMetadata.class);
		ExtentMetadata mockExtent2 = createMock(ExtentMetadata.class);
		StreamingSourceMetadataAbstract mockSource = 
			createMock(StreamingSourceMetadataAbstract.class);
		Attribute mockAttribute = createMock(Attribute.class);
		Attribute mockAttribute1 = createMock(Attribute.class); 
		AttributeType mockType = createMock(AttributeType.class);
		
		expect(mockAttribute.getAttributeSchemaName())
			.andReturn("integerColumn").anyTimes();
		expect(mockAttribute.getAttributeDisplayName())
			.andReturn("integerColumn").anyTimes();
		expect(mockAttribute.getExtentName())
			.andReturn("streamLeft").anyTimes();
		expect(mockAttribute.getType()).andReturn(mockType).anyTimes();
		
		expect(mockAttribute1.getAttributeSchemaName())
			.andReturn("timestamp").anyTimes();
		expect(mockAttribute1.getAttributeDisplayName())
			.andReturn("timestamp").anyTimes();
		expect(mockAttribute1.getExtentName())
			.andReturn("streamRight").anyTimes();
		expect(mockAttribute1.getType()).andReturn(mockType).anyTimes();
		List<Attribute> attrList1 = new ArrayList<Attribute>();
		attrList1.add(mockAttribute);
		List<Attribute> attrList2 = new ArrayList<Attribute>();
		attrList2.add(mockAttribute1);
		
		expect(mockExtent1.getExtentName()).andReturn("stream2").anyTimes();
		expect(mockExtent1.getAttributes()).andReturn(attrList1).anyTimes();
		expect(mockExtent1.getCardinality()).andReturn(1).anyTimes();

		expect(mockExtent2.getExtentName()).andReturn("stream1").anyTimes();
		expect(mockExtent2.getAttributes()).andReturn(attrList2).anyTimes();
		expect(mockExtent2.getCardinality()).andReturn(1).anyTimes();
				
		expect(mockSource.getSourceName()).andReturn("sourceName").anyTimes();
		expect(mockSource.getSourceType())
			.andReturn(SourceType.PULL_STREAM_SERVICE).times(2);
		expect(mockSource.getRate("stream1")).andReturn(2.0);
		expect(mockSource.getRate("stream2")).andReturn(1.0);

		expect(mockType.getName()).andReturn("integer").anyTimes();

		replayAll();

		LogicalOperator receiveOpLeft = 
			new ReceiveOperator(mockExtent1, mockSource, boolType);
		LogicalOperator windowOp1 = 
			new WindowOperator(0, 0, true, 0, 0, receiveOpLeft, boolType);
		LogicalOperator receiveOpRight = 
			new ReceiveOperator(mockExtent2, mockSource, boolType);
		LogicalOperator windowOp2 = 
			new WindowOperator(0, 0, true, 0, 0, receiveOpRight, boolType);
		LogicalOperator joinOp = 
			new JoinOperator(windowOp2, windowOp1, boolType);		
		Expression[] attributes = new Expression[2];
		attributes[0] = new DataAttribute(mockAttribute);
		attributes[1] = new DataAttribute(mockAttribute1);
		Expression joinPredicate = new MultiExpression(attributes, MultiType.EQUALS, boolType);
		LogicalOperator selectOp1 = 
			new SelectOperator(joinPredicate, joinOp, boolType);
		Expression[] expressions = new Expression[2];
		expressions[0] = new DataAttribute(mockAttribute);
		expressions[1] = new IntLiteral(42, types.getType("integer"));
		Expression selectPredicate = new MultiExpression(expressions, MultiType.LESSTHANEQUALS, boolType);
		LogicalOperator selectOp2 = 
			new SelectOperator(selectPredicate, selectOp1, boolType);
		LogicalOperator deliverOp = 
			new DeliverOperator(selectOp2, boolType);

		laf = new LAF(deliverOp, "join-time-select");
		rewriter.pushSelectionDown(laf);
		Iterator<LogicalOperator> opIt = 
			laf.operatorIterator(TraversalOrder.POST_ORDER);
		testOperator(opIt, "RECEIVE");
		testOperator(opIt, "WINDOW");
		testOperator(opIt, "RECEIVE");
		testOperator(opIt, "SELECT");
		testOperator(opIt, "WINDOW");
		testOperator(opIt, "JOIN");
		testOperator(opIt, "SELECT");
		testOperator(opIt, "DELIVER");
		verifyAll();
	}

	/**
	 * Test on select time window query over an acquire source, combine=true
	 * Acquire -> window -> 
	 * Acquire -> window ->
	 * join -> select -> select -> deliver
	 * one select moves below join to right child and is combined with acquire
	 * Acquire -> window -> 
	 * Acquire -> window ->
	 * join -> select -> deliver
	 * @throws SourceMetadataException 
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws OptimizationException 
	 * @throws SNEEConfigurationException 
	 * @throws SNCBException 
	 * @throws CostParametersException 
	 * @throws SNEEDataSourceException 
	 * @throws TopologyReaderException 
	 * @throws UnsupportedAttributeTypeException 
	 * @throws MetadataException 
	 * @throws MalformedURLException 
	 * @throws UtilsException 
	 */
	@Test
	public void testPushSelectionDown_acquireTimeWindowJoinSelectQueryCombine()
	throws SchemaMetadataException, TypeMappingException, 
	SourceMetadataException, OptimizationException, SNEEConfigurationException, 
	MalformedURLException, MetadataException, UnsupportedAttributeTypeException,
	TopologyReaderException, SNEEDataSourceException, CostParametersException,
	SNCBException, UtilsException {
		Properties props = new Properties();
		props.setProperty(SNEEPropertyNames.INPUTS_TYPES_FILE, "etc/Types.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_UNITS_FILE, "etc/units.xml");
		props.setProperty(SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR, "output");
		props.setProperty(SNEEPropertyNames.GENERAL_DELETE_OLD_FILES, "true");
		props.setProperty(SNEEPropertyNames.LOGICAL_REWRITER_COMBINE_ACQUIRE_SELECT, "true");
		props.setProperty(SNEEPropertyNames.GENERATE_QEP_IMAGES, "true");
		props.setProperty(SNEEPropertyNames.CONVERT_QEP_IMAGES, "false");
		SNEEProperties.initialise(props);
		MetadataManager metadata = new MetadataManager(null);
		String typesFileLoc = 
			Utils.validateFileLocation("etc/Types.xml");
		Types types = new Types(typesFileLoc);
		AttributeType boolType = types.getType("boolean");
		LogicalRewriter rewriter = new LogicalRewriter(metadata);
		
		ExtentMetadata mockExtentLeft = createMock(ExtentMetadata.class);
		ExtentMetadata mockExtentRight = createMock(ExtentMetadata.class);
		StreamingSourceMetadataAbstract mockSource = 
			createMock(StreamingSourceMetadataAbstract.class);
		Attribute mockAttribute = createMock(Attribute.class);
		Attribute mockAttribute1 = createMock(Attribute.class); 
		AttributeType mockType = createMock(AttributeType.class);
		
		expect(mockAttribute.getAttributeSchemaName())
			.andReturn("integerColumn").anyTimes();
		expect(mockAttribute.getAttributeDisplayName())
			.andReturn("integerColumn").anyTimes();
		expect(mockAttribute.getExtentName())
			.andReturn("streamLeft").anyTimes();
		expect(mockAttribute.getType()).andReturn(mockType).anyTimes();
		
		expect(mockAttribute1.getAttributeSchemaName())
			.andReturn("timestamp").anyTimes();
		expect(mockAttribute1.getAttributeDisplayName())
			.andReturn("timestamp").anyTimes();
		expect(mockAttribute1.getExtentName())
			.andReturn("streamRight").anyTimes();
		expect(mockAttribute1.getType()).andReturn(mockType).anyTimes();
		List<Attribute> leftAttrList = new ArrayList<Attribute>();
		leftAttrList.add(mockAttribute);
		List<Attribute> rightAttrList = new ArrayList<Attribute>();
		rightAttrList.add(mockAttribute1);
		
		expect(mockAttribute.getRequiredAttributes())
			.andReturn((ArrayList<Attribute>) leftAttrList);
		expect(mockAttribute1.getRequiredAttributes())
			.andReturn((ArrayList<Attribute>) rightAttrList);
		
		expect(mockExtentLeft.getExtentName()).andReturn("streamLeft").anyTimes();
		expect(mockExtentLeft.getAttributes()).andReturn(leftAttrList).anyTimes();
		expect(mockExtentLeft.getCardinality()).andReturn(1).anyTimes();

		expect(mockExtentRight.getExtentName()).andReturn("streamRight").anyTimes();
		expect(mockExtentRight.getAttributes()).andReturn(rightAttrList).anyTimes();
		expect(mockExtentRight.getCardinality()).andReturn(1).anyTimes();
				
		expect(mockSource.getSourceName()).andReturn("sourceName").anyTimes();
		expect(mockSource.getSourceType())
			.andReturn(SourceType.SENSOR_NETWORK).times(2);

		expect(mockType.getName()).andReturn("integer").anyTimes();

		replayAll();

		LogicalOperator receiveOpLeft = 
			new AcquireOperator(mockExtentLeft, types, mockSource, boolType);
		LogicalOperator windowOpLeft = 
			new WindowOperator(0, 0, true, 0, 0, receiveOpLeft, boolType);
		LogicalOperator receiveOpRight = 
			new AcquireOperator(mockExtentRight, types, mockSource, boolType);
		LogicalOperator windowOpRight = 
			new WindowOperator(0, 0, true, 0, 0, receiveOpRight, boolType);
		LogicalOperator joinOp = 
			new JoinOperator(windowOpLeft, windowOpRight, boolType);		
		Expression[] attributes = new Expression[2];
		attributes[0] = new DataAttribute(mockAttribute);
		attributes[1] = new DataAttribute(mockAttribute1);
		Expression joinPredicate = new MultiExpression(attributes, 
				MultiType.EQUALS, boolType);
		joinPredicate.setIsJoinCondition(true);
		LogicalOperator selectOp1 = 
			new SelectOperator(joinPredicate, joinOp, boolType);
		Expression[] expressions = new Expression[2];
		expressions[0] = new DataAttribute(mockAttribute);
		expressions[1] = new IntLiteral(42, types.getType("integer"));
		Expression selectPredicate = new MultiExpression(expressions, 
				MultiType.LESSTHANEQUALS, boolType);
		selectPredicate.setIsJoinCondition(false);
		LogicalOperator selectOp2 = 
			new SelectOperator(selectPredicate, selectOp1, boolType);
		LogicalOperator deliverOp = 
			new DeliverOperator(selectOp2, boolType);

		laf = new LAF(deliverOp, "acquire-time-join-select");
		rewriter.pushSelectionDown(laf);
		rewriter.removeUnrequiredOperators(laf);
		Iterator<LogicalOperator> opIt = 
			laf.operatorIterator(TraversalOrder.POST_ORDER);
		testOperator(opIt, "ACQUIRE");
		testOperator(opIt, "WINDOW");
		testOperator(opIt, "ACQUIRE");
		testOperator(opIt, "WINDOW");
		testOperator(opIt, "JOIN");
		testOperator(opIt, "SELECT");
		testOperator(opIt, "DELIVER");
		verifyAll();
	}

	/**
	 * Test query
	 * Receive -> window -> 
	 * Receive -> window ->
	 * join1 -> 
	 * Receive -> window ->
	 * join2 -> selectJoin1 -> selectJoin2 -> deliver
	 * one select moves below join to left child
	 * @throws SourceMetadataException 
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws OptimizationException 
	 * @throws SNEEConfigurationException 
	 * @throws AssertionError 
	 */
	@Test
	public void testPushSelectionDown_multiJoinSelectQuery1()
	throws SchemaMetadataException, TypeMappingException, 
	SourceMetadataException, OptimizationException, AssertionError, 
	SNEEConfigurationException {
		
		ExtentMetadata mockExtent1 = createMock(ExtentMetadata.class);
		ExtentMetadata mockExtent2 = createMock(ExtentMetadata.class);
		ExtentMetadata mockExtent3 = createMock(ExtentMetadata.class);
		StreamingSourceMetadataAbstract mockSource = 
			createMock(StreamingSourceMetadataAbstract.class);
		Attribute mockAttribute = createMock(Attribute.class);
		Attribute mockAttribute1 = createMock(Attribute.class); 
		Attribute mockAttribute2 = createMock(Attribute.class); 
		AttributeType mockType = createMock(AttributeType.class);
		
		expect(mockAttribute.getAttributeSchemaName())
			.andReturn("integerColumn").anyTimes();
		expect(mockAttribute.getAttributeDisplayName())
			.andReturn("integerColumn").anyTimes();
		expect(mockAttribute.getExtentName())
			.andReturn("stream1").anyTimes();
		expect(mockAttribute.isConstant()).andReturn(false).anyTimes();
		expect(mockAttribute.getType()).andReturn(mockType).anyTimes();
		
		expect(mockAttribute1.getAttributeSchemaName())
			.andReturn("timestamp").anyTimes();
		expect(mockAttribute1.getAttributeDisplayName())
			.andReturn("timestamp").anyTimes();
		expect(mockAttribute1.getExtentName())
			.andReturn("stream2").anyTimes();
		expect(mockAttribute1.isConstant()).andReturn(false).anyTimes();
		expect(mockAttribute1.getType()).andReturn(mockType).anyTimes();

		expect(mockAttribute2.getAttributeSchemaName())
			.andReturn("integerColumn").anyTimes();
		expect(mockAttribute2.getAttributeDisplayName())
			.andReturn("integerColumn").anyTimes();
		expect(mockAttribute2.getExtentName())
			.andReturn("stream3").anyTimes();
		expect(mockAttribute2.isConstant()).andReturn(false).anyTimes();
		expect(mockAttribute2.getType()).andReturn(mockType).anyTimes();
		
		List<Attribute> attrList = new ArrayList<Attribute>();
		attrList.add(mockAttribute);
		List<Attribute> attrList1 = new ArrayList<Attribute>();
		attrList1.add(mockAttribute1);
		List<Attribute> attrList2 = new ArrayList<Attribute>();
		attrList2.add(mockAttribute2);
		
		expect(mockExtent1.getExtentName()).andReturn("stream1").anyTimes();
		expect(mockExtent1.getAttributes()).andReturn(attrList).anyTimes();
		expect(mockExtent1.getCardinality()).andReturn(1).anyTimes();

		expect(mockExtent2.getExtentName()).andReturn("stream2").anyTimes();
		expect(mockExtent2.getAttributes()).andReturn(attrList1).anyTimes();
		expect(mockExtent2.getCardinality()).andReturn(1).anyTimes();

		expect(mockExtent3.getExtentName()).andReturn("stream3").anyTimes();
		expect(mockExtent3.getAttributes()).andReturn(attrList1).anyTimes();
		expect(mockExtent3.getCardinality()).andReturn(1).anyTimes();

		expect(mockSource.getSourceName()).andReturn("sourceName").anyTimes();
		expect(mockSource.getSourceType())
			.andReturn(SourceType.PULL_STREAM_SERVICE).times(3);
		expect(mockSource.getRate("stream1")).andReturn(2.0);
		expect(mockSource.getRate("stream2")).andReturn(1.0);
		expect(mockSource.getRate("stream3")).andReturn(0.1);

		expect(mockType.getName()).andReturn("integer").anyTimes();

		replayAll();

		LogicalOperator receiveOp1 = 
			new ReceiveOperator(mockExtent1, mockSource, boolType);
		LogicalOperator windowOp1 = 
			new WindowOperator(0, 0, true, 0, 0, receiveOp1, boolType);
		LogicalOperator receiveOp2 = 
			new ReceiveOperator(mockExtent2, mockSource, boolType);
		LogicalOperator windowOp2 = 
			new WindowOperator(0, 0, true, 0, 0, receiveOp2, boolType);
		LogicalOperator joinOp1 = 
			new JoinOperator(windowOp1, windowOp2, boolType);		
		LogicalOperator receiveOp3 = 
			new ReceiveOperator(mockExtent3, mockSource, boolType);
		LogicalOperator windowOp3 = 
			new WindowOperator(0, 0, true, 0, 0, receiveOp3, boolType);
		LogicalOperator joinOp2 = 
			new JoinOperator(joinOp1, windowOp3, boolType);		
		Expression[] attributes = new Expression[2];
		attributes[0] = new DataAttribute(mockAttribute);
		attributes[1] = new DataAttribute(mockAttribute1);
		Expression joinPredicate1 = 
			new MultiExpression(attributes, MultiType.EQUALS, boolType);
		LogicalOperator selectOp1 = 
			new SelectOperator(joinPredicate1, joinOp2, boolType);

		Expression[] attributes2 = new Expression[2];
		attributes2[0] = new DataAttribute(mockAttribute);
		attributes2[1] = new DataAttribute(mockAttribute2);
		Expression joinPredicate2 = 
			new MultiExpression(attributes2, MultiType.EQUALS, boolType);
		LogicalOperator selectOp3 = 
			new SelectOperator(joinPredicate2, selectOp1, boolType);

		Expression[] expressions = new Expression[2];
		expressions[0] = new DataAttribute(mockAttribute);
		expressions[1] = new IntLiteral(42, types.getType("integer"));
		Expression selectPredicate = new MultiExpression(expressions, MultiType.LESSTHANEQUALS, boolType);
		LogicalOperator selectOp2 = 
			new SelectOperator(selectPredicate, selectOp3, boolType);
		LogicalOperator deliverOp = 
			new DeliverOperator(selectOp2, boolType);

		laf = new LAF(deliverOp, "multi-join-time-select1");
		rewriter.pushSelectionDown(laf);
		Iterator<LogicalOperator> opIt = 
			laf.operatorIterator(TraversalOrder.POST_ORDER);
		testOperator(opIt, "RECEIVE");
		testOperator(opIt, "SELECT");
		testOperator(opIt, "WINDOW");
		testOperator(opIt, "RECEIVE");
		testOperator(opIt, "WINDOW");
		testOperator(opIt, "JOIN");
		testOperator(opIt, "SELECT");
		testOperator(opIt, "RECEIVE");
		testOperator(opIt, "WINDOW");
		testOperator(opIt, "JOIN");
		testOperator(opIt, "SELECT");
		testOperator(opIt, "DELIVER");
		verifyAll();
	}

	/**
	 * Test query
	 * Test query
	 * Receive -> window -> 
	 * Receive -> window ->
	 * join1 -> 
	 * Receive -> window ->
	 * join2 -> selectJoin2 -> selectJoin1 -> deliver
	 * one select moves below join to left child
	 * @throws SourceMetadataException 
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws OptimizationException 
	 * @throws SNEEConfigurationException 
	 * @throws AssertionError 
	 */
	@Test
	public void testPushSelectionDown_multiJoinSelectQuery2()
	throws SchemaMetadataException, TypeMappingException, 
	SourceMetadataException, OptimizationException, AssertionError, 
	SNEEConfigurationException {
		
		ExtentMetadata mockExtent1 = createMock(ExtentMetadata.class);
		ExtentMetadata mockExtent2 = createMock(ExtentMetadata.class);
		ExtentMetadata mockExtent3 = createMock(ExtentMetadata.class);
		StreamingSourceMetadataAbstract mockSource = 
			createMock(StreamingSourceMetadataAbstract.class);
		Attribute mockAttribute = createMock(Attribute.class);
		Attribute mockAttribute1 = createMock(Attribute.class); 
		Attribute mockAttribute2 = createMock(Attribute.class); 
		AttributeType mockType = createMock(AttributeType.class);
		
		expect(mockAttribute.getAttributeSchemaName())
			.andReturn("integerColumn").anyTimes();
		expect(mockAttribute.getAttributeDisplayName())
			.andReturn("integerColumn").anyTimes();
		expect(mockAttribute.getExtentName())
			.andReturn("stream1").anyTimes();
		expect(mockAttribute.isConstant()).andReturn(false).anyTimes();
		expect(mockAttribute.getType()).andReturn(mockType).anyTimes();
		
		expect(mockAttribute1.getAttributeSchemaName())
			.andReturn("timestamp").anyTimes();
		expect(mockAttribute1.getAttributeDisplayName())
			.andReturn("timestamp").anyTimes();
		expect(mockAttribute1.getExtentName())
			.andReturn("stream2").anyTimes();
		expect(mockAttribute1.isConstant()).andReturn(false).anyTimes();
		expect(mockAttribute1.getType()).andReturn(mockType).anyTimes();

		expect(mockAttribute2.getAttributeSchemaName())
			.andReturn("integerColumn").anyTimes();
		expect(mockAttribute2.getAttributeDisplayName())
			.andReturn("integerColumn").anyTimes();
		expect(mockAttribute2.getExtentName())
			.andReturn("stream3").anyTimes();
		expect(mockAttribute2.isConstant()).andReturn(false).anyTimes();
		expect(mockAttribute2.getType()).andReturn(mockType).anyTimes();
		
		List<Attribute> attrList = new ArrayList<Attribute>();
		attrList.add(mockAttribute);
		List<Attribute> attrList1 = new ArrayList<Attribute>();
		attrList1.add(mockAttribute1);
		List<Attribute> attrList2 = new ArrayList<Attribute>();
		attrList2.add(mockAttribute2);
		
		expect(mockExtent1.getExtentName()).andReturn("stream1").anyTimes();
		expect(mockExtent1.getAttributes()).andReturn(attrList).anyTimes();
		expect(mockExtent1.getCardinality()).andReturn(1).anyTimes();

		expect(mockExtent2.getExtentName()).andReturn("stream2").anyTimes();
		expect(mockExtent2.getAttributes()).andReturn(attrList1).anyTimes();
		expect(mockExtent2.getCardinality()).andReturn(1).anyTimes();

		expect(mockExtent3.getExtentName()).andReturn("stream3").anyTimes();
		expect(mockExtent3.getAttributes()).andReturn(attrList1).anyTimes();
		expect(mockExtent3.getCardinality()).andReturn(1).anyTimes();

		expect(mockSource.getSourceName()).andReturn("sourceName").anyTimes();
		expect(mockSource.getSourceType())
			.andReturn(SourceType.PULL_STREAM_SERVICE).times(3);
		expect(mockSource.getRate("stream1")).andReturn(2.0);
		expect(mockSource.getRate("stream2")).andReturn(1.0);
		expect(mockSource.getRate("stream3")).andReturn(0.1);

		expect(mockType.getName()).andReturn("integer").anyTimes();

		replayAll();

		LogicalOperator receiveOp1 = 
			new ReceiveOperator(mockExtent1, mockSource, boolType);
		LogicalOperator windowOp1 = 
			new WindowOperator(0, 0, true, 0, 0, receiveOp1, boolType);
		LogicalOperator receiveOp2 = 
			new ReceiveOperator(mockExtent2, mockSource, boolType);
		LogicalOperator windowOp2 = 
			new WindowOperator(0, 0, true, 0, 0, receiveOp2, boolType);
		LogicalOperator joinOp1 = 
			new JoinOperator(windowOp1, windowOp2, boolType);		
		LogicalOperator receiveOp3 = 
			new ReceiveOperator(mockExtent3, mockSource, boolType);
		LogicalOperator windowOp3 = 
			new WindowOperator(0, 0, true, 0, 0, receiveOp3, boolType);
		LogicalOperator joinOp2 = 
			new JoinOperator(joinOp1, windowOp3, boolType);		

		Expression[] attributes2 = new Expression[2];
		attributes2[0] = new DataAttribute(mockAttribute);
		attributes2[1] = new DataAttribute(mockAttribute2);
		Expression joinPredicate2 = 
			new MultiExpression(attributes2, MultiType.EQUALS, boolType);
		LogicalOperator selectOp3 = 
			new SelectOperator(joinPredicate2, joinOp2, boolType);
		
		Expression[] attributes = new Expression[2];
		attributes[0] = new DataAttribute(mockAttribute);
		attributes[1] = new DataAttribute(mockAttribute1);
		Expression joinPredicate1 = 
			new MultiExpression(attributes, MultiType.EQUALS, boolType);
		LogicalOperator selectOp1 = 
			new SelectOperator(joinPredicate1, selectOp3, boolType);

		Expression[] expressions = new Expression[2];
		expressions[0] = new DataAttribute(mockAttribute);
		expressions[1] = new IntLiteral(42, types.getType("integer"));
		Expression selectPredicate = new MultiExpression(expressions, MultiType.LESSTHANEQUALS, boolType);
		LogicalOperator selectOp2 = 
			new SelectOperator(selectPredicate, selectOp1, boolType);
		LogicalOperator deliverOp = 
			new DeliverOperator(selectOp2, boolType);

		laf = new LAF(deliverOp, "multi-join-time-select2");
		rewriter.pushSelectionDown(laf);
		Iterator<LogicalOperator> opIt = 
			laf.operatorIterator(TraversalOrder.POST_ORDER);
		testOperator(opIt, "RECEIVE");
		testOperator(opIt, "SELECT");
		testOperator(opIt, "WINDOW");
		testOperator(opIt, "RECEIVE");
		testOperator(opIt, "WINDOW");
		testOperator(opIt, "JOIN");
		testOperator(opIt, "SELECT");
		testOperator(opIt, "RECEIVE");
		testOperator(opIt, "WINDOW");
		testOperator(opIt, "JOIN");
		testOperator(opIt, "SELECT");
		testOperator(opIt, "DELIVER");
		verifyAll();
	}

}
