package uk.ac.manchester.cs.snee.compiler.translator;

import static org.junit.Assert.fail;

import java.io.StringReader;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.metadata.Metadata;
import uk.ac.manchester.cs.snee.compiler.metadata.MetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentDoesNotExistException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceDoesNotExistException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.compiler.parser.ParserException;
import uk.ac.manchester.cs.snee.compiler.parser.SNEEqlLexer;
import uk.ac.manchester.cs.snee.compiler.parser.SNEEqlParser;
import antlr.CommonAST;
import antlr.RecognitionException;
import antlr.TokenStreamException;

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

	@Before
	public void setUp() 
	throws TypeMappingException, SchemaMetadataException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException {
		Properties props = new Properties();
		props.setProperty(SNEEPropertyNames.INPUTS_TYPES_FILE, "Types.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_UNITS_FILE, "units.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_SCHEMA_FILE, "logical-schema.xml");
		props.setProperty(SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR, "output");
		SNEEProperties.initialise(props);
		Metadata schemaMetadata = new Metadata();
		translator = new Translator(schemaMetadata);
	}

	@After
	public void tearDown() throws Exception {
	}	
	
	private void testQuery(String query) 
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
		System.out.println("Parse tree: " + parseTree.toStringList());
		logger.info("Parse tree: " + parseTree.toStringList());
		translator.mainTranslate(parseTree);
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
		testQuery("(SELECT timestamp FROM TestStream) " +
				"UNION " +
				"(SELECT timestamp FROM PullStream);");
	}
	
	@Test
	public void testUnionQuery_parenMultiUnion() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ParserValidationException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("(SELECT timestamp FROM TestStream) UNION " +
				"(SELECT timestamp FROM PullStream) UNION" +
				"(SELECT timestamp FROM PushStream);");
	}
	
	@Test
	public void testUnionQuery_multiUnion() 
	throws SourceDoesNotExistException, ParserException, 
	SchemaMetadataException, ParserValidationException, AssertionError, 
	OptimizationException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException, TokenStreamException {
		testQuery("(SELECT timestamp FROM TestStream) UNION " +
				"(SELECT timestamp FROM PullStream) UNION" +
				"(SELECT timestamp FROM PushStream);");
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
		testQuery("(SELECT timestamp FROM TestStream) " +
				"UNION " +
				"(SELECT integetColumn FROM PullStream);");
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
