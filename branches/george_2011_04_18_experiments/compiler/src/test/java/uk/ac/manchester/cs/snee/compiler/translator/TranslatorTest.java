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
import uk.ac.manchester.cs.snee.compiler.parser.ParserException;
import uk.ac.manchester.cs.snee.compiler.parser.SNEEqlLexer;
import uk.ac.manchester.cs.snee.compiler.parser.SNEEqlParser;
import uk.ac.manchester.cs.snee.sncb.SNCBException;

import uk.ac.manchester.cs.snee.compiler.queryplan.LAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.DataAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.ExpressionException;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentDoesNotExistException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.Types;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SourceDoesNotExistException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
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
		props.setProperty(SNEEPropertyNames.SNCB_GENERATE_COMBINED_IMAGE, "false");
		SNEEProperties.initialise(props);
		MetadataManager schemaMetadata = new MetadataManager(null);
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
	ExpressionException, AssertionError, OptimizationException,
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
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("Some rubbish;");
	}
	
	@Test(expected=ParserException.class)
	public void testGibberish() 
	throws ParserException, SourceDoesNotExistException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("Some gibberish that is not a query;");
	}
	
	@Test
	public void testSimpleQuery() 
	throws ParserException, SourceDoesNotExistException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT * FROM TestStream;");
	}
	
	@Test
	public void testSimpleQuery_paren() 
	throws ParserException, SourceDoesNotExistException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("(SELECT * FROM TestStream);");
	}
	
	@Test
	public void testSimpleProject() 
	throws ParserException, SourceDoesNotExistException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT timestamp FROM TestStream;");
	}
		
	@Test
	public void testSimpleProject_paren() 
	throws ParserException, SourceDoesNotExistException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("(SELECT timestamp FROM TestStream);");
	}
	
	@Test
	public void testRename() 
	throws SourceDoesNotExistException,
	ExtentDoesNotExistException, RecognitionException, 
	ParserException, SchemaMetadataException, 
	ExpressionException, AssertionError, 
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
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT Timestamp " +
				"FROM TestStream " +
				"WHERE Timestamp < 42;");
	}
	
	@Test
	public void testSimpleSelect_attrComparison() 
	throws ParserException, SourceDoesNotExistException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT Timestamp " +
				"FROM TestStream " +
				"WHERE Timestamp < integerColumn;");
	}
	
	@Test
	public void testRowWindow() throws ParserException, 
	SourceDoesNotExistException, SchemaMetadataException, 
	ExpressionException, AssertionError, OptimizationException, 
	TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT * " +
				"FROM TestStream[FROM NOW-10 ROWS TO NOW - 0 SLIDE 5 ROWS];");
	}
	
	@Test
	public void testRowWindow_toNow() throws ParserException, 
	SourceDoesNotExistException, SchemaMetadataException, 
	ExpressionException, AssertionError, OptimizationException, 
	TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT * " +
				"FROM TestStream[FROM NOW-10 ROWS TO NOW SLIDE 5 ROWS];");
	}
	
	@Test
	public void testNowWindow() throws ParserException, 
	SourceDoesNotExistException, SchemaMetadataException, 
	ExpressionException, AssertionError, OptimizationException, 
	TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT * " +
				"FROM TestStream[NOW];");
	}
	
	@Test
	public void testNowSlideWindow() throws ParserException, 
	SourceDoesNotExistException, SchemaMetadataException, 
	ExpressionException, AssertionError, OptimizationException, 
	TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT * " +
				"FROM TestStream[NOW SLIDE 5 ROWS];");
	}
	
	@Ignore
	@Test
	public void testNowToNowWindow() throws ParserException, 
	SourceDoesNotExistException, SchemaMetadataException, 
	ExpressionException, AssertionError, OptimizationException, 
	TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		//FIXME: Correct parser/translator for FROM NOW TO NOW
		testQuery("SELECT * " +
				"FROM TestStream[FROM NOW TO NOW];");
	}
	
	@Test
	public void testTimeWindow() throws ParserException, 
	SourceDoesNotExistException, SchemaMetadataException, 
	ExpressionException, AssertionError, OptimizationException, 
	TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT * " +
				"FROM TestStream[FROM NOW-10 MINUTES TO NOW SLIDE 30 SECONDS];");
	}
	
	@Ignore
	@Test
	public void testTimeWindow_noSlide() throws ParserException, 
	SourceDoesNotExistException, SchemaMetadataException, 
	ExpressionException, AssertionError, OptimizationException, 
	TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		//FIXME: Correct translator
		testQuery("SELECT * " +
				"FROM TestStream[FROM NOW-10 MINUTES TO NOW];");
	}
	
	@Test
	public void testNowEquiJoin() throws ParserException, 
	SourceDoesNotExistException, SchemaMetadataException, 
	ExpressionException, AssertionError, OptimizationException, 
	TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT * " +
				"FROM TestStream[NOW] t, PullStream[NOW] p " +
				"WHERE t.timestamp = p.timestamp;");
	}
	
	@Test
	public void testTimeEquiJoin() throws ParserException, 
	SourceDoesNotExistException, SchemaMetadataException, 
	ExpressionException, AssertionError, OptimizationException, 
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
	ExpressionException, AssertionError, OptimizationException, 
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
	ExpressionException, AssertionError,
	OptimizationException, TypeMappingException
	{
		testQuery("RSTREAM SELECT * FROM TestStream[NOW];");
	}
	
	@Test
	public void testSimpleSelectRstreamNOW() 
	throws SourceDoesNotExistException, 
	ExtentDoesNotExistException, RecognitionException, 
	ParserException, SchemaMetadataException, 
	ExpressionException, AssertionError,
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
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		/* Union queries must contain parentheses around subqueries */
		testQuery("SELECT timestamp FROM TestStream UNION " +
				"SELECT timestamp FROM PullStream;");
	}
	
	@Test
	public void testUnionQuery_paren() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		LAF laf = testQuery("(SELECT timestamp FROM TestStream) " +
				"UNION " +
				"(SELECT timestamp FROM PushStream);");
		verifyUnionQuery(laf, 1, 2);
	}

	@Test
	public void testUnionQuery_Union3Query() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		LAF laf = testQuery("(SELECT timestamp FROM TestStream) UNION " +
				"(SELECT timestamp FROM PushStream) UNION" +
				"(SELECT timestamp FROM PushStream2);");
		verifyUnionQuery(laf, 2, 3);
	}
	
	@Test
	public void testUnionQuery_Union4Query() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		LAF laf = testQuery("(SELECT timestamp FROM TestStream) UNION " +
				"(SELECT timestamp FROM PushStream) UNION" +
				"(SELECT timestamp FROM PushStream2) UNION " +
				"(SELECT timestamp FROM PushStream3);");
		verifyUnionQuery(laf, 3, 4);
	}
	
	@Test(expected=ParserException.class)
	public void testUnionQuery_windows() 
	throws SourceDoesNotExistException, ExtentDoesNotExistException, 
	RecognitionException, ParserException, SchemaMetadataException,
	ExpressionException, AssertionError, OptimizationException, 
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
	ExpressionException, AssertionError, OptimizationException, 
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
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		LAF laf = testQuery("(SELECT timestamp FROM TestStream) " +
				"UNION " +
				"(SELECT integerColumn FROM PushStream);");
		verifyUnionQuery(laf, 1, 2);
	}
	
	@Test(expected=ParserException.class)
	public void testUnionQuery_diffAttrDiffType() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("(SELECT timestamp FROM TestStream) " +
				"UNION " +
				"(SELECT floatColumn FROM PullStream);");
	}
	
	@Test(expected=ParserException.class)
	public void testUnionQuery_diffNumAttr() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("(SELECT timestamp FROM TestStream) " +
				"UNION " +
				"(SELECT timestamp, floatColumn FROM PullStream);");
	}

	@Test(expected=ExpressionException.class)
	public void testQuery_windowOnNestedQuery() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT s.Timestamp " +
				"FROM " +
				"	(SELECT timestamp " +
				"	FROM TestStream s)[FROM NOW - 10 TO NOW];");
	}

	@Test
	public void testSubQuery_WindowClause() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT timestamp FROM " +
				"	(SELECT ts.timestamp " +
				"	FROM TestStream ts)[FROM NOW - 10 SECONDS TO NOW SLIDE 1 SECOND];");
	}

	@Test
	public void testQuery_SimpleRenaming() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ts.timestamp FROM TestStream ts;");
	}

	@Test
	public void testQuery_SimpleRenamingParen() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("(SELECT ts.timestamp FROM TestStream ts);");
	}

	@Test
	public void testQuery_SimpleRenamingWindow() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ts.timestamp " +
				"FROM TestStream[FROM NOW - 10 SECONDS TO NOW SLIDE 1 SECOND] ts;");
	}

	@Test
	public void testUnionQuery_problematicFromClause() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ts.timestamp FROM (SELECT timestamp FROM TestStream) ts;");
	}

	
	/********************************************************************************
	 * 			NEWLY ADDED JUNIT TESTS AFTER CHANGING THE TRANSLATOR				*
	 ********************************************************************************/

	@Test
	public void testQuery_TwoColumns_Paren() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("(SELECT integerColumn, floatColumn FROM PushStream);");
	}

	@Test
	public void testNamedExtentQuery_DuplicateAttributes() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("(SELECT integerColumn, integerColumn FROM PushStream ps);");
	}
	


	@Test
	public void testNamedExtentQuery_UnnamedAttribute() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT integerColumn FROM PushStream ps;");
	}
	
	@Test
	public void testNamedExtentQuery_AliasingUnnamedDuplicateAttributes() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT integerColumn AS int1, integerColumn AS int2 FROM PushStream ps;");
	}

	/* You can not have a named attribute without providing a name for the extent */
	@Test(expected=ExpressionException.class)
	public void testUnnamedExtentQuery_NamedAttribute() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ps.integerColumn FROM PushStream;");
	}

	/* You can not have a named attribute without providing a name for the extent.
	 * Just checking that parenthesis do not cause a problem. */
	@Test(expected=ExpressionException.class)
	public void testUnnamedExtentQuery_NamedAttribute_Paren() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("(SELECT ps.integerColumn FROM PushStream);");
	}


	/* You can not have a named attribute without providing a name for the extent.
	 * Making sure this is so even when there is a window on the extent */
	@Test(expected=ExpressionException.class)
	public void testUnnamedExtentQuery_NamedAttribute_Window() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ps.integerColumn FROM PushStream[NOW];");
	}

	/* You can not have a named attribute without providing a name for the extent.
	 * Simply checking this is the case, even for 2 columns and a window on the extent */
	@Test(expected=ExpressionException.class)
	public void testUnnamedExtentQuery_NamedAttribute_TwoColumns() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ps.integerColumn, ps.floatColumn FROM PushStream[NOW];");
	}

	/* This is the correct form of the query */
	@Test
	public void testNamedExtentQuery_NamedAttribute() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ps.integerColumn FROM PushStream ps;");
	}

	/* This is the correct form of a query with named attributes, where we
	 * use two columns */
	@Test
	public void testNamedExtentQuery_NamedAttribute_TwoColumns() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ps.integerColumn, ps.floatColumn FROM PushStream ps;");
	}

	/* This is the correct form of a query with named attributes, where we
	 * use two columns. We also demonstrate that we are aliasing the attributes
	 * correctly */
	@Test
	public void testNamedExtentQuery_NamedAttribute_AliasingDuplicateColumns() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ps.integerColumn AS int1, ps.integerColumn AS int2 FROM PushStream ps;");
	}


	/* Given that we have a named attribute, we require that the extents in the FROM
	 * clause have an equivalent name, even if they are nested queries */
	@Test(expected=ExpressionException.class)
	public void testUnnamedSubQuery() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ps.integerColumn FROM ( SELECT integerColumn FROM PushStream ps);");
	}

	/* Given that we have a named attribute, we require that the extents in the FROM
	 * clause have an equivalent name, even if they are nested queries. Testing with
	 * parenthesis on the outter query, to make sure that this is also valid */
	@Test(expected=ExpressionException.class)
	public void testUnnamedSubQuery_Paren() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("(SELECT ps.integerColumn FROM ( SELECT integerColumn FROM PushStream ps));");
	}


	/* Given that we want to display a named attribute, the extents in the FROM
	 * clause *MUST* have an equivalent name, even if they are nested queries or
	 * even windowed ones */
	@Test(expected=ExpressionException.class)
	public void testUnnamedSubQuery_WindowAndNameInNestedQuery() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ps.integerColumn FROM (SELECT ps.integerColumn FROM PushStream[NOW] ps);");
	}

	/* Given that we want to display a named attribute, the extents in the FROM
	 * clause *MUST* have an equivalent name. Testing with two columns in the
	 * nested query and also the fact that naming the nested query does not affect
	 * the external queries */
	@Test(expected=ExpressionException.class)
	public void testUnnamedSubQuery_TwoColumnsInNestedQuery() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ps.integerColumn, ps.floatColumn FROM " +
				"	( SELECT integerColumn, floatColumn FROM PushStream ps);");
	}


	/* This is the correct form of a nested query, where named attributes
	 * will be displayed. We also make sure that when fetching everything,
	 * the correct column will be displayed */
	@Test
	public void testNamedSubQuery_FetchAll_ProjectSingle_NameExternalAttribute() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ps.integerColumn FROM (SELECT * FROM PushStream) ps;");
	}

	/* This is the correct form of a nested query, where named attributes
	 * will be displayed. Just to make sure that the query handles projections
	 * equally well to the case of a STAR (*). */
	@Test
	public void testNamedSubQuery_FetchSingle_NameExternalAttribute() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ps.integerColumn FROM (SELECT integerColumn FROM PushStream) ps;");
	}

	/* This is the correct form of a nested query, where named attributes
	 * are requested on the output. Making sure that queries are translated
	 * correctly in the case of a windowed nested query */
	@Test
	public void testWindowedNamedSubQuery_FetchSingle_NameExternalAttribute() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ps.integerColumn FROM (SELECT integerColumn FROM PushStream[NOW]) ps;");
	}


	@Test
	public void testSubQuery_WindowAndNameInNested_FetchSingle_NameExternalAttribute() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ps.integerColumn FROM (SELECT ps.integerColumn FROM PushStream[NOW] ps) ps;");
	}

	@Test
	public void testSubQuery_FetchTwoColumns_NameExternalAttribute() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ps.integerColumn, ps.floatColumn FROM " +
				"(SELECT integerColumn, floatColumn FROM PushStream) ps;");
	}

	@Test
	public void testSubQuery_InternalExternalNames_RenameOnExternal_FetchAll() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ts.integerColumn FROM (SELECT * FROM PushStream ps) ts;");
	}

	@Test
	public void testSubQuery_InternalExternalNames_RenameOnExternal_FetchSingle() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ts.integerColumn FROM (SELECT integerColumn FROM PushStream ps) ts;");
	}

	@Test
	public void testSubQuery_InternalExternalNames_RenameOnExternal_FetchWindowed() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ts.integerColumn FROM (SELECT integerColumn FROM PushStream[NOW]) ts;");
	}

	@Test
	public void testSubQuery_InternalExternalSameNames() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ps.integerColumn FROM (SELECT ps.integerColumn FROM PushStream ps) ps;");
	}

	
	@Test
	public void testAliasing_SingleAttribute_UnnamedExtents() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT integerColumn AS intValue FROM PushStream;");
	}

	@Test
	public void testAliasing_TwoAttributes_UnnamedExtents() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT integerColumn AS intValue, floatColumn AS floatValue FROM PushStream;");
	}

	@Test
	public void testAliasingInNestedQuery_NamedExtents_WithWindow() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ts.intvalue FROM (SELECT ps.integerColumn as intvalue FROM PushStream[NOW] ps) ts;");
	}

	@Test
	public void testAliasing_NamedExtent_UnnamedAttribute() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT integerColumn AS intValue FROM PushStream ps;");
	}

	@Test
	public void testAliasingInSubquery_UnnamedExternalAttribute_NamedInternalAttribute() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT integerColumn AS intvalue FROM (SELECT ps.integerColumn FROM PushStream[NOW] ps) ts;");
	}

	@Test
	public void testAliasing_ReferencedAttributes() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ps.integerColumn AS intValue FROM PushStream ps;");
	}

	@Test
	public void testAliasing_ReferencedAttributes_SubQuery() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ts.integerColumn AS intvalue FROM (SELECT ps.integerColumn FROM PushStream[NOW] ps) ts;");
	}

	/* The sub-query in the FROM clause does not have a reference name */
	@Test(expected=ExpressionException.class)
	public void testNestedQuery_UnreferencedNestedQuery() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ps.intvalue FROM (SELECT integerColumn AS intValue FROM PushStream ps);");
	}

	/* Similar to the previous one */
	@Test(expected=ExpressionException.class)
	public void testNestedQuery_UnreferencedNestedQuery_FetchAll() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ps.intvalue AS intValue, ps.floatColumn AS floatValue " +
				"FROM (SELECT * FROM PushStream ps);");
	}

	
	/* The sub-query in the FROM clause aliases integerColumn to intValue. However,
	 * the external query requests integerColumn! */
	@Test(expected=ExpressionException.class)
	public void testNestedQuery_MistakenAliasingFromInternal() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ps.integerColumn FROM (SELECT integerColumn AS intValue FROM PushStream) ps;");
	}

	
	/* No aliasing, but we are requesting the wrong names, as we have requested to fetch all
	 * in the nested query. Notice that the reference name of the nested query is correctly
	 * applied */
	@Test(expected=ExpressionException.class)
	public void testNestedQuery_WrongAliasing_FetchAll() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ts.intvalue AS intValue, ts.floatColumn AS floatValue " + 
				"FROM (SELECT * FROM PushStream) ts;");
	}

	
	@Test
	public void testNestedQuery_CorrectAliasingInNestedQuery() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ps.intvalue FROM (SELECT integerColumn AS intValue FROM PushStream) ps;");
	}

	@Test
	public void testNestedQuery_InternalExternalAliasing() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ps.int1 AS intval, ps.int2 AS newVal FROM " +
				"	(SELECT integerColumn AS int1, integerColumn as int2 FROM PushStream) ps;");
	}

	/* Notice that the reference renaming is correctly applied. Moreover, in the
	 * subquery we also have a ps reference, which is (allowed but) ignored! (yes, this is valid) */
	@Test
	public void testNestedQuery_ExternalAliasingOnFetchAll() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ts.integerColumn AS intValue, ts.floatColumn AS floatValue " +
				"FROM (SELECT * FROM PushStream ps) ts;");
	}

	/* Testing reference renaming from the subquery to the outer one. Column names are
	 * correctly requested. However, the outer query requests attributes using the wrong
	 * reference name. This is to ensure that the reference names of nested queries are
	 * not visible to outer queries. */
	@Test(expected=ExpressionException.class)
	public void testNestedQuery_WrongExternalAliasingOnFetchAll() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ps.integerColumn AS intValue, ps.floatColumn AS floatValue " +
				"FROM (SELECT * FROM PushStream ps) ts;");
	}


	@Test
	public void testNestedQuery_ChangeFetchSequence_WithAliasing() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT ts.floatColumn AS floatValue, ts.intvalue AS intValue " +
				"FROM (SELECT ps.integerColumn AS intvalue, ps.floatColumn FROM PushStream ps) ts;");
	}

	@Test
	public void testNestedQuery_WindowedNestinng() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT t1.integerColumn FROM " +
				"(SELECT integerColumn FROM PushStream[NOW]) t1;");
	}

	@Test
	public void testNestedQuery_DoubleExternalAliasing_SameAttribute() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT t1.integerColumn AS t1, t1.integerColumn AS t2 FROM " +
				"(SELECT integerColumn FROM PushStream[NOW]) t1;");
	}

	@Test
	public void testNestedQuery_StrangeRenaming_WindowOnInternal() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT t1.t1 AS int1, t1.t2 AS int2 FROM " +
				"(SELECT integerColumn AS t1, integerColumn AS t2 FROM PushStream[NOW]) t1;");
	}

	@Test
	public void testNestedJoin_WithAliasing() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT t1.integerColumn AS int1, t2.floatColumn AS float FROM " +
				"(SELECT integerColumn FROM PushStream[NOW]) t1, PushStream[NOW] t2;");
	}

	@Test
	public void testQuery_MultipleNestedLevels() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT p.integerColumn, s.integerColumn, s.floatColumn " +
				"FROM PushStream[NOW] p, " +
				"	(SELECT integerColumn, floatColumn FROM PushStream[NOW]) s;");
	}


	/**********************************************************************************
	 * 					The following queries SHOULD not fail but they do
	 * 
	 * FIX the translator to support the following queries
	 **********************************************************************************/

	@Test
	@Ignore
	public void testSimpleQuery_FuncSUMStar() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT SUM(*) FROM PushStream;");
	}


	@Test
	@Ignore
	public void testSimpleQuery_FuncAVGStar() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT AVG(*) FROM PushStream;");
	}


	@Test
	@Ignore
	public void testSimpleQuery_FuncCOUNTStar() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT COUNT(*) FROM PushStream;");
	}

	@Test
	@Ignore
	public void testSimpleQuery_FuncSUM() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT SUM(integerColumn) FROM PushStream;");
	}


	@Test
	@Ignore
	public void testSimpleQuery_FuncAVG() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT AVG(integerColumn) FROM PushStream;");
	}


	@Test
	@Ignore
	public void testSimpleQuery_FuncCOUNT() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ExpressionException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("SELECT COUNT(integerColumn) FROM PushStream;");
	}


	

}
