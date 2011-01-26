package uk.ac.manchester.cs.snee.operators.logical;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.*;

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
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.Types;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;

public class ScanOperatorTest extends EasyMockSupport {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				ScanOperatorTest.class.getClassLoader().getResource(
						"etc/log4j.properties"));
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	private Types types;
	private ExtentMetadata mockExtent;
	private Attribute mockAttribute;

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
		mockAttribute = createMock(Attribute.class);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetOperatorName() 
	throws TypeMappingException, SchemaMetadataException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		expect(mockExtent.getAttributes())
			.andReturn(new ArrayList<Attribute>());
		replayAll();
		List<SourceMetadataAbstract> sources =
			new ArrayList<SourceMetadataAbstract>();
		ScanOperator op = new ScanOperator(mockExtent, 
				sources, 
				types.getType("boolean"));
		assertTrue(op.getOperatorName().equals("SCAN"));
		verifyAll();
	}

	@Test
	public void testGetOperatorDataType() 
	throws TypeMappingException, SchemaMetadataException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		expect(mockExtent.getAttributes())
			.andReturn(new ArrayList<Attribute>());
		replayAll();
		List<SourceMetadataAbstract> sources =
			new ArrayList<SourceMetadataAbstract>();
		ScanOperator op = new ScanOperator(mockExtent, 
				sources, 
				types.getType("boolean"));
		assertEquals(OperatorDataType.RELATION, op.getOperatorDataType());
		verifyAll();
	}

	@Test
	public void testGetExtentName() 
	throws TypeMappingException, SchemaMetadataException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		expect(mockExtent.getAttributes())
			.andReturn(new ArrayList<Attribute>());
		replayAll();
		List<SourceMetadataAbstract> sources =
			new ArrayList<SourceMetadataAbstract>();
		ScanOperator op = new ScanOperator(mockExtent, 
				sources, 
				types.getType("boolean"));
		assertTrue(op.getExtentName().equals("Name"));
		verifyAll();
	}

	@Test
	public void testGetSources() 
	throws TypeMappingException, SchemaMetadataException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		expect(mockExtent.getAttributes())
			.andReturn(new ArrayList<Attribute>());
		replayAll();
		List<SourceMetadataAbstract> sources =
			new ArrayList<SourceMetadataAbstract>();
		ScanOperator op = new ScanOperator(mockExtent, 
				sources, 
				types.getType("boolean"));
		assertEquals(sources, op.getSources());
		verifyAll();
	}
	
	@Test
	public void testToString()
	throws TypeMappingException, SchemaMetadataException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		expect(mockExtent.getAttributes())
			.andReturn(new ArrayList<Attribute>());
		replayAll();
		List<SourceMetadataAbstract> sources =
			new ArrayList<SourceMetadataAbstract>();
		ScanOperator op = new ScanOperator(mockExtent, 
				sources, 
				types.getType("boolean"));
		String extentMetadata = op.toString();
//		System.out.println(extentMetadata);
		assertTrue(extentMetadata.equals(
				"TYPE: relation   OPERATOR: SCAN - cardinality: 0"));
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
			.andReturn(attributes);
		replayAll();
		List<SourceMetadataAbstract> sources =
			new ArrayList<SourceMetadataAbstract>();
		ScanOperator op = new ScanOperator(mockExtent, 
				sources, 
				types.getType("boolean"));
		int cardinarlity = op.getCardinality(CardinalityType.MAX);
		assertEquals(attributes.size(), cardinarlity);
		verifyAll();
	}

	@Test
	public void testIsAttributeSensitive() throws TypeMappingException, SchemaMetadataException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		expect(mockExtent.getAttributes())
			.andReturn(new ArrayList<Attribute>());
		replayAll();
		List<SourceMetadataAbstract> sources =
			new ArrayList<SourceMetadataAbstract>();
		ScanOperator op = new ScanOperator(mockExtent, 
				sources, 
				types.getType("boolean"));
		assertFalse(op.isAttributeSensitive());
		verifyAll();
	}

	@Test
	public void testIsLocationSensitive() 
	throws TypeMappingException, SchemaMetadataException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		expect(mockExtent.getAttributes())
			.andReturn(new ArrayList<Attribute>());
		replayAll();
		List<SourceMetadataAbstract> sources =
			new ArrayList<SourceMetadataAbstract>();
		ScanOperator op = new ScanOperator(mockExtent, 
				sources, 
				types.getType("boolean"));
		assertTrue(op.isLocationSensitive());
		verifyAll();
	}

	@Test
	public void testIsRecursive() 
	throws TypeMappingException, SchemaMetadataException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		expect(mockExtent.getAttributes())
			.andReturn(new ArrayList<Attribute>());
		replayAll();
		List<SourceMetadataAbstract> sources =
			new ArrayList<SourceMetadataAbstract>();
		ScanOperator op = new ScanOperator(mockExtent, 
				sources, 
				types.getType("boolean"));
		assertFalse(op.isRecursive());
		verifyAll();
	}

	@Test
	public void testAcceptsPredicates() 
	throws TypeMappingException, SchemaMetadataException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		expect(mockExtent.getAttributes())
			.andReturn(new ArrayList<Attribute>());
		replayAll();
		List<SourceMetadataAbstract> sources =
			new ArrayList<SourceMetadataAbstract>();
		ScanOperator op = new ScanOperator(mockExtent, 
				sources, 
				types.getType("boolean"));
		assertFalse(op.acceptsPredicates());
		verifyAll();
	}

	@Test
	public void testIsRemoveable() 
	throws TypeMappingException, SchemaMetadataException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		expect(mockExtent.getAttributes())
			.andReturn(new ArrayList<Attribute>());
		replayAll();
		List<SourceMetadataAbstract> sources =
			new ArrayList<SourceMetadataAbstract>();
		ScanOperator op = new ScanOperator(mockExtent, 
				sources, 
				types.getType("boolean"));
		assertFalse(op.isRemoveable());
		verifyAll();
	}

	@Test
	public void testPushProjectionDown() 
	throws TypeMappingException, SchemaMetadataException,
	OptimizationException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		expect(mockExtent.getAttributes())
			.andReturn(new ArrayList<Attribute>());
		replayAll();
		List<SourceMetadataAbstract> sources =
			new ArrayList<SourceMetadataAbstract>();
		ScanOperator op = new ScanOperator(mockExtent, 
				sources, 
				types.getType("boolean"));
		assertFalse(op.pushProjectionDown(null, null));
		verifyAll();
	}

	@Test
	public void testPushSelectDown() 
	throws TypeMappingException, SchemaMetadataException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		expect(mockExtent.getAttributes())
			.andReturn(new ArrayList<Attribute>());
		replayAll();
		List<SourceMetadataAbstract> sources =
			new ArrayList<SourceMetadataAbstract>();
		ScanOperator op = new ScanOperator(mockExtent, 
				sources, 
				types.getType("boolean"));
		assertFalse(op.pushSelectDown(null));
		verifyAll();
	}

	@Test(expected=AssertionError.class)
	public void testPushLocalNameDown() 
	throws TypeMappingException, SchemaMetadataException {
		expect(mockExtent.getExtentName()).andReturn("Name").anyTimes();
		expect(mockExtent.getAttributes())
			.andReturn(new ArrayList<Attribute>());
		replayAll();
		List<SourceMetadataAbstract> sources =
			new ArrayList<SourceMetadataAbstract>();
		ScanOperator op = new ScanOperator(mockExtent, 
				sources, 
				types.getType("boolean"));
		op.pushLocalNameDown("newLocalName");
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
			.andReturn(attributes);
		replayAll();
		List<SourceMetadataAbstract> sources =
			new ArrayList<SourceMetadataAbstract>();
		ScanOperator op = new ScanOperator(mockExtent, 
				sources, 
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
			.andReturn(attributes);
		replayAll();
		List<SourceMetadataAbstract> sources =
			new ArrayList<SourceMetadataAbstract>();
		ScanOperator op = new ScanOperator(mockExtent, 
				sources, 
				types.getType("boolean"));
		List<Expression> exprs = op.getExpressions();
		assertEquals(attributes.size(), exprs.size());
		verifyAll();
	}

}
