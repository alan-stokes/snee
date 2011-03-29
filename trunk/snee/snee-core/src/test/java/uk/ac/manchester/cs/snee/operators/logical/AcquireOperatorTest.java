package uk.ac.manchester.cs.snee.operators.logical;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
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
import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.DataAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.Types;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;

public class AcquireOperatorTest extends EasyMockSupport {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				AcquireOperatorTest.class.getClassLoader().getResource(
						"etc/log4j.properties"));
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	private Types types;
	private ExtentMetadata mockExtent;
	private Attribute mockAttribute;
	private SourceMetadataAbstract mockSource;

	@Before
	public void setUp() throws Exception {
		Properties props = new Properties();
		props.setProperty(SNEEPropertyNames.INPUTS_TYPES_FILE, "etc/Types.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_UNITS_FILE, "etc/units.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_LOGICAL_SCHEMA_FILE, "etc/logical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE, "etc/physical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_COST_PARAMETERS_FILE, "etc/cost-parameters.xml");
		props.setProperty(SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR, "output");
		SNEEProperties.initialise(props);
		String typesFileLoc = 
			Utils.validateFileLocation("etc/Types.xml");
		types = new Types(typesFileLoc);
		mockExtent = createMock(ExtentMetadata.class);
		mockAttribute = createMock(DataAttribute.class);
		mockSource = createMock(SensorNetworkSourceMetadata.class);
		expect(mockSource.getSourceName()).andReturn("sourceName").anyTimes();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetOperatorName() 
	throws TypeMappingException, SchemaMetadataException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		expect(mockExtent.getAttributes())
			.andReturn(new ArrayList<Attribute>()).times(2);
		expect(mockExtent.getCardinality()).andReturn(1);
		replayAll();
		AcquireOperator op = new AcquireOperator(mockExtent, types, 
				mockSource, 
				types.getType("boolean"));
		assertTrue(op.getOperatorName().equals("ACQUIRE"));
		verifyAll();
	}

	@Test
	public void testGetOperatorDataType() 
	throws TypeMappingException, SchemaMetadataException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		expect(mockExtent.getAttributes())
			.andReturn(new ArrayList<Attribute>()).times(2);
		expect(mockExtent.getCardinality()).andReturn(1);
		replayAll();
		AcquireOperator op = new AcquireOperator(mockExtent, types,
				mockSource, 
				types.getType("boolean"));
		assertEquals(OperatorDataType.STREAM, op.getOperatorDataType());
		verifyAll();
	}

	@Test
	public void testGetCardinality() 
	throws TypeMappingException, SchemaMetadataException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		List<Attribute> attributes = new ArrayList<Attribute>();
		attributes.add(mockAttribute);
		attributes.add(mockAttribute);
		expect(mockExtent.getAttributes())
			.andReturn(attributes).times(2);
		expect(mockAttribute.getRequiredAttributes()).
			andReturn(new ArrayList<Attribute>()).times(2);
		expect(mockExtent.getCardinality()).andReturn(1);
		replayAll();
		AcquireOperator op = new AcquireOperator(mockExtent, types,
				mockSource, 
				types.getType("boolean"));
		int cardinarlity = op.getCardinality(CardinalityType.MAX);
		assertEquals(1, cardinarlity);
		verifyAll();
	}

	@Test
	public void testAcceptsPredicates() 
	throws TypeMappingException, SchemaMetadataException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		expect(mockExtent.getAttributes())
			.andReturn(new ArrayList<Attribute>()).times(2);
		expect(mockExtent.getCardinality()).andReturn(1);
		replayAll();
		AcquireOperator op = new AcquireOperator(mockExtent, types, 
				mockSource, 
				types.getType("boolean"));
		assertTrue(op.acceptsPredicates());
		verifyAll();
	}

	@Test
	public void testPushProjectionDown_EmptyAttributes() 
	throws TypeMappingException, SchemaMetadataException,
	OptimizationException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		expect(mockExtent.getAttributes())
			.andReturn(new ArrayList<Attribute>()).times(2);
		expect(mockExtent.getCardinality()).andReturn(1);
		replayAll();
		AcquireOperator op = new AcquireOperator(mockExtent, types,
				mockSource, 
				types.getType("boolean"));
		assertFalse(op.pushProjectionDown(null, new ArrayList<Attribute>()));
		verifyAll();
	}

	@Test
	public void testPushSelectDown() 
	throws TypeMappingException, SchemaMetadataException {
		Expression mockExpression = createMock(Expression.class);
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		expect(mockExtent.getAttributes())
			.andReturn(new ArrayList<Attribute>()).times(2);
		expect(mockExtent.getCardinality()).andReturn(1);
		expect(mockExpression.getType()).andReturn(types.getType("boolean")).times(2);
		replayAll();
		AcquireOperator op = new AcquireOperator(mockExtent, types,
				mockSource, 
				types.getType("boolean"));
		assertTrue(op.pushSelectDown(mockExpression));
		verifyAll();
	}

	@Test
	public void testGetAcquiredAttributes() 
	throws SchemaMetadataException, TypeMappingException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		List<Attribute> attributes = new ArrayList<Attribute>();
		attributes.add(mockAttribute);
		attributes.add(mockAttribute);
		expect(mockExtent.getAttributes())
			.andReturn(attributes).times(2);
		expect(mockAttribute.getRequiredAttributes()).
			andReturn(new ArrayList<Attribute>()).times(2);
		expect(mockExtent.getCardinality()).andReturn(1);
		replayAll();
		AcquireOperator op = new AcquireOperator(mockExtent, types,
				mockSource, 
				types.getType("boolean"));
		List<Attribute> acAttr = op.getAcquiredAttributes();
		assertEquals(attributes.size(), acAttr.size());
		verifyAll();
	}
	
	@Test
	public void testToString()
	throws TypeMappingException, SchemaMetadataException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		expect(mockExtent.getAttributes())
		.andReturn(new ArrayList<Attribute>()).times(2);
		expect(mockExtent.getCardinality()).andReturn(1);
		replayAll();
		AcquireOperator op = new AcquireOperator(mockExtent, types,
				mockSource, 
				types.getType("boolean"));
		String extentMetadata = op.toString();
//		System.out.println(extentMetadata);
		assertTrue(extentMetadata.equals(
				"TYPE: stream   OPERATOR: ACQUIRE " +
		"(Name (cardinality=1 source=sourceName ) - cardinality: 1"));
		verifyAll();
	}
	
// Tests below are based on abstract InputOperator behaviour		
	
	@Test
	public void testGetExtentName() 
	throws TypeMappingException, SchemaMetadataException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		expect(mockExtent.getAttributes())
			.andReturn(new ArrayList<Attribute>()).times(2);
		expect(mockExtent.getCardinality()).andReturn(1);
		replayAll();
		AcquireOperator op = new AcquireOperator(mockExtent, types,
				mockSource, 
				types.getType("boolean"));
		assertTrue(op.getExtentName().equals("Name"));
		verifyAll();
	}

	@Test
	public void testGetmockSource() 
	throws TypeMappingException, SchemaMetadataException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		expect(mockExtent.getAttributes())
			.andReturn(new ArrayList<Attribute>()).times(2);
		expect(mockExtent.getCardinality()).andReturn(1);
		replayAll();
		AcquireOperator op = new AcquireOperator(mockExtent, types,
				mockSource, 
				types.getType("boolean"));
		assertEquals(mockSource, op.getSource());
		verifyAll();
	}

	@Test
	public void testGetAttributes() 
	throws TypeMappingException, SchemaMetadataException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		List<Attribute> attributes = new ArrayList<Attribute>();
		attributes.add(mockAttribute);
		attributes.add(mockAttribute);
		expect(mockExtent.getAttributes())
			.andReturn(attributes).times(2);
		expect(mockAttribute.getRequiredAttributes()).
			andReturn(new ArrayList<Attribute>()).times(2);
		expect(mockExtent.getCardinality()).andReturn(1);
		replayAll();
		AcquireOperator op = new AcquireOperator(mockExtent, types,
				mockSource, 
				types.getType("boolean"));
		List<Attribute> attrs = op.getAttributes();
		assertEquals(attributes.size(), attrs.size());
		verifyAll();
	}

	@Test
	public void testGetExpressions() 
	throws TypeMappingException, SchemaMetadataException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		List<Attribute> attributes = new ArrayList<Attribute>();
		attributes.add(mockAttribute);
		attributes.add(mockAttribute);
		expect(mockExtent.getAttributes())
			.andReturn(attributes).times(2);
		expect(mockAttribute.getRequiredAttributes()).
			andReturn(new ArrayList<Attribute>()).times(2);
		expect(mockExtent.getCardinality()).andReturn(1);
		replayAll();
		AcquireOperator op = new AcquireOperator(mockExtent, types,
				mockSource, 
				types.getType("boolean"));
		List<Expression> exprs = op.getExpressions();
		assertEquals(attributes.size(), exprs.size());
		verifyAll();
	}

	@Test
	public void testGetInputAttributes() 
	throws TypeMappingException, SchemaMetadataException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		List<Attribute> attributes = new ArrayList<Attribute>();
		attributes.add(mockAttribute);
		attributes.add(mockAttribute);
		expect(mockExtent.getAttributes())
			.andReturn(attributes).times(2);
		expect(mockAttribute.getRequiredAttributes()).
			andReturn(new ArrayList<Attribute>()).times(2);
		expect(mockExtent.getCardinality()).andReturn(1);
		replayAll();
		AcquireOperator op = new AcquireOperator(mockExtent, types,
				mockSource, 
				types.getType("boolean"));
		List<Attribute> attrs = op.getInputAttributes();
		assertEquals(attributes.size(), attrs.size());
		verifyAll();
	}

	@Test
	public void testGetNumberInputAttributes() 
	throws TypeMappingException, SchemaMetadataException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		List<Attribute> attributes = new ArrayList<Attribute>();
		attributes.add(mockAttribute);
		attributes.add(mockAttribute);
		expect(mockExtent.getAttributes())
			.andReturn(attributes).times(2);
		expect(mockAttribute.getRequiredAttributes()).
			andReturn(new ArrayList<Attribute>()).times(2);
		expect(mockExtent.getCardinality()).andReturn(1);
		replayAll();
		AcquireOperator op = new AcquireOperator(mockExtent, types,
				mockSource, 
				types.getType("boolean"));
		int size = op.getNumberInputAttributes();
		assertEquals(attributes.size(), size);
		verifyAll();
	}

	@Test
	public void testGetInputAttributeNumber() 
	throws TypeMappingException, SchemaMetadataException, OptimizationException 
	{
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		List<Attribute> attributes = new ArrayList<Attribute>();
		attributes.add(mockAttribute);
		Attribute mockAttr = createMock(DataAttribute.class);
		attributes.add(mockAttr);
		expect(mockExtent.getAttributes())
			.andReturn(attributes).times(2);
		expect(mockAttribute.getRequiredAttributes()).
			andReturn(new ArrayList<Attribute>());
		expect(mockAttr.getRequiredAttributes()).
			andReturn(new ArrayList<Attribute>());
		expect(mockExtent.getCardinality()).andReturn(1);
		replayAll();
		AcquireOperator op = new AcquireOperator(mockExtent, types,
				mockSource, 
				types.getType("boolean"));
		int attrNumber = op.getInputAttributeNumber(mockAttribute);
		assertEquals(0, attrNumber);
		attrNumber = op.getInputAttributeNumber(mockAttr);
		assertEquals(1, attrNumber);
		verifyAll();
	}

	@Test
	public void testIsAttributeSensitive() 
	throws TypeMappingException, SchemaMetadataException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		expect(mockExtent.getAttributes())
			.andReturn(new ArrayList<Attribute>()).times(2);
		expect(mockExtent.getCardinality()).andReturn(1);
		replayAll();
		AcquireOperator op = new AcquireOperator(mockExtent, types,
				mockSource, 
				types.getType("boolean"));
		assertFalse(op.isAttributeSensitive());
		verifyAll();
	}

	@Test
	public void testIsLocationSensitive() 
	throws TypeMappingException, SchemaMetadataException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		expect(mockExtent.getAttributes())
			.andReturn(new ArrayList<Attribute>()).times(2);
		expect(mockExtent.getCardinality()).andReturn(1);
		replayAll();
		AcquireOperator op = new AcquireOperator(mockExtent, types,
				mockSource, 
				types.getType("boolean"));
		assertTrue(op.isLocationSensitive());
		verifyAll();
	}

	@Test
	public void testIsRecursive() 
	throws TypeMappingException, SchemaMetadataException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		expect(mockExtent.getAttributes())
			.andReturn(new ArrayList<Attribute>()).times(2);
		expect(mockExtent.getCardinality()).andReturn(1);
		replayAll();
		AcquireOperator op = new AcquireOperator(mockExtent, types,
				mockSource, 
				types.getType("boolean"));
		assertFalse(op.isRecursive());
		verifyAll();
	}

	@Test
	public void testIsRemoveable() 
	throws TypeMappingException, SchemaMetadataException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		expect(mockExtent.getAttributes())
			.andReturn(new ArrayList<Attribute>()).times(2);
		expect(mockExtent.getCardinality()).andReturn(1);
		replayAll();
		AcquireOperator op = new AcquireOperator(mockExtent, types,
				mockSource, 
				types.getType("boolean"));
		assertFalse(op.isRemoveable());
		verifyAll();
	}

	//XXX: Removed by AG as metadata now handled in metadata object
//	@Test(expected=AssertionError.class)
//	public void testPushLocalNameDown() 
//	throws TypeMappingException, SchemaMetadataException {
//		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
//		expect(mockExtent.getAttributes())
//			.andReturn(new ArrayList<Attribute>()).times(2);
//		expect(mockExtent.getCardinality()).andReturn(1);
//		replayAll();
//		AcquireOperator op = new AcquireOperator(mockExtent, types,
//				mockSource, 
//				types.getType("boolean"));
//		op.pushLocalNameDown("newLocalName");
//		verifyAll();
//	}

}
