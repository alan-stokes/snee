package uk.ac.manchester.cs.snee.compiler.translator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.net.MalformedURLException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.common.UtilsException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.compiler.metadata.Metadata;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentDoesNotExistException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.Types;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceDoesNotExistException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.compiler.parser.ParserException;
import uk.ac.manchester.cs.snee.compiler.parser.SNEEqlLexer;
import uk.ac.manchester.cs.snee.compiler.parser.SNEEqlParser;
import uk.ac.manchester.cs.snee.sncb.SNCBException;

import uk.ac.manchester.cs.snee.compiler.queryplan.LAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.DataAttribute;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.UnionOperator;
import antlr.CommonAST;
import antlr.RecognitionException;
import antlr.TokenStreamException;
import antlr.collections.AST;

public class TranslatorTest {

	Logger logger = Logger.getLogger(this.getClass().getName());
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				TranslatorTest.class.getClassLoader().getResource(
						"etc/log4j.properties"));
	}

	private Translator translator;
	private Types types;

	@Before
	public void setUp() 
	throws TypeMappingException, SchemaMetadataException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException, 
	TopologyReaderException, MalformedURLException,
	SNEEDataSourceException, CostParametersException, UtilsException, SNCBException {
		Properties props = new Properties();
		props.setProperty(SNEEPropertyNames.INPUTS_TYPES_FILE, "etc/Types.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_UNITS_FILE, "etc/units.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_LOGICAL_SCHEMA_FILE, "etc/logical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE, "etc/physical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_COST_PARAMETERS_FILE, "etc/cost-parameters.xml");
		props.setProperty(SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR, "output");
		props.setProperty(SNEEPropertyNames.SNCB_PERFORM_METADATA_COLLECTION, "false");
		props.setProperty(SNEEPropertyNames.SNCB_TINYOS_ROOT, "~/work/tinyos-2.x");
		props.setProperty(SNEEPropertyNames.SNCB_GENERATE_COMBINED_IMAGE, "false");
		SNEEProperties.initialise(props);
		Metadata schemaMetadata = new Metadata();
		translator = new Translator(schemaMetadata);
		String typesFileLoc = 
			Utils.validateFileLocation("etc/Types.xml");
		types = new Types(typesFileLoc);
	}

	@After
	public void tearDown() throws Exception {
	}	
	
	private LAF testQuery(String query) 
	throws ParserException, SchemaMetadataException,
	ParserValidationException, AssertionError, OptimizationException,
	SourceDoesNotExistException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException 
	{
		SNEEqlLexer lexer = new SNEEqlLexer(new StringReader(query));
		SNEEqlParser parser = new SNEEqlParser(lexer);
		try {
			parser.parse();
		} catch (RecognitionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		} catch (TokenStreamException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		}
		CommonAST parseTree = (CommonAST)parser.getAST();
		if (logger.isInfoEnabled()) {
			logger.info("Query: " + query);
			logger.info("Parse tree: " + parseTree.toStringList());
			StringBuffer buffer = new StringBuffer("Parse tree:");
			buffer.append(displayNode(parseTree, 0));
			logger.info(buffer.toString());
//			System.out.println(query + "\n" + buffer.toString());
		}
		return translator.translate(parseTree, 1);
	}
	
	private String displayNode(AST ast, int level) {
		StringBuffer buffer = new StringBuffer();
		if (ast != null) {
			buffer.append("\n");
			for (int i = 1; i <= level; i++) {
				buffer.append("  ");
			}
			buffer.append(ast.getText());
			buffer.append(displayNode(ast.getFirstChild(), level + 1));
			AST nextSibling = ast.getNextSibling();
			buffer.append(displayNode(nextSibling, level));
		}
		return buffer.toString();
	}

	//TODO: Write tests for queries
	/*
	 * Queries that have worked in the past, should have tests written for them
	 * SELECT * FROM InFlow;
	 * SELECT temperature FROM InFlow;
	 * SELECT temperature as temp FROM InFlow;
	 * 
	 * SELECT * FROM InFlow[FROM NOW - 2 seconds TO NOW];
	 * SELECT RSTREAM * FROM InFlow [FROM NOW - 2 seconds TO NOW];
	 * SELECT RSTREAM temperature FROM InFlow [NOW];
	 * 
	 * SELECT RSTREAM AVG(temperature) as temp FROM InFlow [FROM NOW - 2 seconds TO NOW];
	 * SELECT RSTREAM AVG(i.temperature) as temp FROM InFlow [FROM NOW - 2 seconds TO NOW] i;
	 * 
	 * SELECT RSTREAM i.temperature as temp FROM InFlow [FROM NOW - 2 seconds TO NOW] i, OutFlow [FROM NOW - 2 seconds TO NOW] o WHERE i.temperature = o.temperature;
	 * 
	 * SELECT temperature, pressure, temperature * pressure FROM InFlow;
	 * SELECT temperature, pressure, (temperature * 9 / 5) +32 FROM InFlow;
	 * SELECT temperature, pressure, (temperature * 9 / 5) + 32 as Fahrenheit FROM InFlow;
	 */
	
	@Test(expected=ParserException.class)
	public void testRubbish() 
	throws ParserException, SourceDoesNotExistException, 
	SchemaMetadataException, ParserValidationException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("Some rubbish;");
	}
	
	@Test(expected=ParserException.class)
	public void testGibberish() 
	throws ParserException, SourceDoesNotExistException, 
	SchemaMetadataException, ParserValidationException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("Some gibberish that is not a query;");
	}
	
	@Test
	public void testSimpleQuery() 
	throws ParserException, SourceDoesNotExistException, 
	SchemaMetadataException, ParserValidationException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT * FROM TestStream;");
	}
	
	@Test
	public void testSimpleQuery_paren() 
	throws ParserException, SourceDoesNotExistException, 
	SchemaMetadataException, ParserValidationException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("(SELECT * FROM TestStream);");
	}
	
	@Test
	public void testSimpleProject() 
	throws ParserException, SourceDoesNotExistException, 
	SchemaMetadataException, ParserValidationException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT timestamp FROM TestStream;");
	}
		
	@Test
	public void testSimpleProject_paren() 
	throws ParserException, SourceDoesNotExistException, 
	SchemaMetadataException, ParserValidationException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("(SELECT timestamp FROM TestStream);");
	}
	
	@Test
	public void testRename() 
	throws SourceDoesNotExistException,
	ExtentDoesNotExistException, RecognitionException, 
	ParserException, SchemaMetadataException, 
	ParserValidationException, AssertionError, 
	OptimizationException, TypeMappingException 
	{
		LAF laf = testQuery("SELECT timestamp AS time " +
				"FROM TestStream;");
		LogicalOperator rootOperator = laf.getRootOperator();
		List<Attribute> attributes = 
			rootOperator.getAttributes();
		assertEquals(1, attributes.size());
		Attribute attribute = attributes.get(0);
		assertTrue(attribute.getAttributeDisplayName().
				equalsIgnoreCase("time"));
		assertTrue(attribute.getAttributeSchemaName().
				equalsIgnoreCase("timestamp"));
	}
	
	@Test
	public void testSimpleSelect() 
	throws ParserException, SourceDoesNotExistException, 
	SchemaMetadataException, ParserValidationException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT Timestamp " +
				"FROM TestStream " +
				"WHERE Timestamp < 42;");
	}
	
	@Test
	public void testSimpleSelect_attrComparison() 
	throws ParserException, SourceDoesNotExistException, 
	SchemaMetadataException, ParserValidationException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT Timestamp " +
				"FROM TestStream " +
				"WHERE Timestamp < integetColumn;");
	}
	
	@Test
	public void testRowWindow() throws ParserException, 
	SourceDoesNotExistException, SchemaMetadataException, 
	ParserValidationException, AssertionError, OptimizationException, 
	TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT * " +
				"FROM TestStream[FROM NOW-10 ROWS TO NOW - 0 SLIDE 5 ROWS];");
	}
	
	@Test
	public void testRowWindow_toNow() throws ParserException, 
	SourceDoesNotExistException, SchemaMetadataException, 
	ParserValidationException, AssertionError, OptimizationException, 
	TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT * " +
				"FROM TestStream[FROM NOW-10 ROWS TO NOW SLIDE 5 ROWS];");
	}
	
	@Test
	public void testNowWindow() throws ParserException, 
	SourceDoesNotExistException, SchemaMetadataException, 
	ParserValidationException, AssertionError, OptimizationException, 
	TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT * " +
				"FROM TestStream[NOW];");
	}
	
	@Test
	public void testNowSlideWindow() throws ParserException, 
	SourceDoesNotExistException, SchemaMetadataException, 
	ParserValidationException, AssertionError, OptimizationException, 
	TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT * " +
				"FROM TestStream[NOW SLIDE 5 ROWS];");
	}
	
	@Ignore
	@Test
	public void testNowToNowWindow() throws ParserException, 
	SourceDoesNotExistException, SchemaMetadataException, 
	ParserValidationException, AssertionError, OptimizationException, 
	TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		//FIXME: Correct parser/translator for FROM NOW TO NOW
		testQuery("SELECT * " +
				"FROM TestStream[FROM NOW TO NOW];");
	}
	
	@Test
	public void testTimeWindow() throws ParserException, 
	SourceDoesNotExistException, SchemaMetadataException, 
	ParserValidationException, AssertionError, OptimizationException, 
	TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT * " +
				"FROM TestStream[FROM NOW-10 MINUTES TO NOW SLIDE 30 SECONDS];");
	}
	
	@Test@Ignore
	public void testTimeWindow_noSlide() throws ParserException, 
	SourceDoesNotExistException, SchemaMetadataException, 
	ParserValidationException, AssertionError, OptimizationException, 
	TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		//FIXME: Correct translator
		testQuery("SELECT * " +
				"FROM TestStream[FROM NOW-10 MINUTES TO NOW];");
	}
	
	@Test
	public void testNowEquiJoin() throws ParserException, 
	SourceDoesNotExistException, SchemaMetadataException, 
	ParserValidationException, AssertionError, OptimizationException, 
	TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT * " +
				"FROM TestStream[NOW] t, PullStream[NOW] p " +
				"WHERE t.timestamp = p.timestamp;");
	}
	
	@Test
	public void testTimeEquiJoin() throws ParserException, 
	SourceDoesNotExistException, SchemaMetadataException, 
	ParserValidationException, AssertionError, OptimizationException, 
	TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT * " +
				"FROM TestStream[FROM NOW - 5 SECONDS TO NOW SLIDE 1 SECOND] t, PullStream[FROM NOW - 5 SECONDS TO NOW SLIDE 1 SECOND] p " +
				"WHERE t.timestamp = p.timestamp;");
	}
	
	@Test
	public void testJoinQueryRename() 
	throws SourceDoesNotExistException, ExtentDoesNotExistException, 
	RecognitionException, ParserException, SchemaMetadataException, 
	ParserValidationException, AssertionError, OptimizationException, 
	TypeMappingException
	{
		// Record result
		Attribute testTimestampAttr = 
			new DataAttribute("teststream", "timestamp", 
					"t.timestamp", types.getType("integer"));
		Attribute pullTimestampAttr =
			new DataAttribute("pullstream", "timestamp", 
					"p.timestamp", types.getType("integer"));
		
		// Run test
		LAF laf = testQuery("SELECT t.timestamp, p.timestamp " +
				"FROM TestStream[FROM NOW - 10 SECONDS TO " +
				"NOW SLIDE 10 SECOND] t, " +
				"PullStream[FROM NOW - 10 SECONDS TO " +
				"NOW SLIDE 10 SECOND] p;");
		
		// Verify result
		Iterator<LogicalOperator> it = 
			laf.operatorIterator(TraversalOrder.PRE_ORDER);
		LogicalOperator rootOperator = laf.getRootOperator();
		List<Attribute> attributes = rootOperator.getAttributes();
		assertEquals(2, attributes.size());
		assertEquals(testTimestampAttr, attributes.get(0));
		assertEquals(pullTimestampAttr, attributes.get(1));
	}
	
	@Test
	public void testSimpleRstreamSelectNOW() 
	throws SourceDoesNotExistException, 
	ExtentDoesNotExistException, RecognitionException, 
	ParserException, SchemaMetadataException, 
	ParserValidationException, AssertionError,
	OptimizationException, TypeMappingException
	{
		testQuery("RSTREAM SELECT * FROM TestStream[NOW];");
	}
	
	@Test
	public void testSimpleSelectRstreamNOW() 
	throws SourceDoesNotExistException, 
	ExtentDoesNotExistException, RecognitionException, 
	ParserException, SchemaMetadataException, 
	ParserValidationException, AssertionError,
	OptimizationException, TypeMappingException
	{
		testQuery("SELECT RSTREAM * FROM TestStream[NOW];");
	}
	
	private void verifyUnionQuery(LAF laf, int numUnions, 
			int numLeaves) {
		Iterator<LogicalOperator> it = 
			laf.operatorIterator(TraversalOrder.PRE_ORDER);
		int leafCount = 0;
		int unionCount = 0;
		while (it.hasNext()) {
			LogicalOperator op = it.next();
//			System.out.println(op.getOperatorName());
			if (op.isLeaf()) {
				leafCount++;
			}
			if (op instanceof UnionOperator) {
				unionCount++;
			}
		}
		assertEquals("Check number of union operators", 
				numUnions, unionCount);
		assertEquals("Check number of leaf operators",
				numLeaves, leafCount);
	}

	@Test(expected=ParserException.class)
	public void testUnionQuery_withoutParen() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ParserValidationException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		/* Union queries must contain parentheses around subqueries */
		testQuery("SELECT timestamp FROM TestStream UNION " +
				"SELECT timestamp FROM PullStream;");
	}
	
	@Test
	public void testUnionQuery_paren() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ParserValidationException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		LAF laf = testQuery("(SELECT timestamp FROM TestStream) " +
				"UNION " +
				"(SELECT timestamp FROM PullStream);");
		verifyUnionQuery(laf, 1, 2);
	}

	@Test
	public void testUnionQuery_Union3Query() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ParserValidationException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		LAF laf = testQuery("(SELECT timestamp FROM TestStream) UNION " +
				"(SELECT timestamp FROM PullStream) UNION" +
				"(SELECT timestamp FROM PushStream);");
		verifyUnionQuery(laf, 2, 3);
	}
	
	@Test
	public void testUnionQuery_Union4Query() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ParserValidationException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		LAF laf = testQuery("(SELECT timestamp FROM TestStream) UNION " +
				"(SELECT timestamp FROM PullStream) UNION" +
				"(SELECT timestamp FROM PushStream) UNION " +
				"(SELECT timestamp FROM SensorStream);");
		verifyUnionQuery(laf, 3, 4);
	}
	
	@Test(expected=ParserException.class)
	public void testUnionQuery_windows() 
	throws SourceDoesNotExistException, ExtentDoesNotExistException, 
	RecognitionException, ParserException, SchemaMetadataException,
	ParserValidationException, AssertionError, OptimizationException, 
	TypeMappingException
	{
		testQuery("(SELECT timestamp " +
				"FROM TestStream[FROM NOW - 10 SECONDS TO NOW SLIDE 1 SECOND]) " +
				"UNION " +
				"(SELECT timestamp " +
				"FROM PullStream[FROM NOW - 10 SECONDS TO NOW SLIDE 1 SECOND]);");
	}
	
	@Test(expected=ParserException.class)
	public void testUnionQuery_streamWindow() 
	throws SourceDoesNotExistException, ExtentDoesNotExistException, 
	RecognitionException, ParserException, SchemaMetadataException,
	ParserValidationException, AssertionError, OptimizationException, 
	TypeMappingException
	{
		testQuery("(SELECT timestamp " +
				"FROM TestStream) " +
				"UNION " +
				"(SELECT timestamp " +
				"FROM PullStream[FROM NOW - 10 SECONDS TO NOW SLIDE 1 SECOND]);");
	}
	
	@Test
	public void testUnionQuery_diffAttrSameType() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ParserValidationException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		LAF laf = testQuery("(SELECT timestamp FROM TestStream) " +
				"UNION " +
				"(SELECT integetColumn FROM PullStream);");
		verifyUnionQuery(laf, 1, 2);
	}
	
	@Test(expected=ParserException.class)
	public void testUnionQuery_diffAttrDiffType() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ParserValidationException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("(SELECT timestamp FROM TestStream) " +
				"UNION " +
				"(SELECT floatColumn FROM PullStream);");
	}
	
	@Test(expected=ParserException.class)
	public void testUnionQuery_diffNumAttr() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ParserValidationException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("(SELECT timestamp FROM TestStream) " +
				"UNION " +
				"(SELECT timestamp, floatColumn FROM PullStream);");
	}
	
}
