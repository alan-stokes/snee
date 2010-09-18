package uk.ac.manchester.cs.snee.data.webservice;

import static org.junit.Assert.assertEquals;

import java.io.InputStream;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.Types;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.evaluator.QueryEvaluatorTest;

public class OgsadaiSchemaParserTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				QueryEvaluatorTest.class.getClassLoader().getResource(
						"etc/log4j.properties"));
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	private OgsadaiSchemaParser parser;

	@Before
	public void setUp() throws Exception {
		InputStream is = 
			OgsadaiSchemaParser.class.getClassLoader().
					getResourceAsStream("etc/ogsa-dai-schema-description.xml");
		Types types = new Types(OgsadaiSchemaParser.class.getClassLoader().
				getResource("etc/Types.xml").toString());
		byte[] bytes = new byte[is.available()];
		is.read(bytes);
		String schema = new String(bytes);
		parser = new OgsadaiSchemaParser(schema, types);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetExtentName() {
		assertEquals("envdata_folkestone", parser.getExtentName());
	}

	@Test
	public void testGetColumns() 
	throws TypeMappingException, SchemaMetadataException {
		List<Attribute> columns =
			parser.getColumns("envdata_folkestone");
		assertEquals(7, columns.size());
		Attribute attr = columns.get(0);
		assertEquals("timestamp", attr.getAttributeSchemaName());
		assertEquals("envdata_folkestone.timestamp", 
				attr.getAttributeDisplayName());
		assertEquals("integer", attr.getType().getName());
		attr = columns.get(2);
		assertEquals("error_code", attr.getAttributeSchemaName());
		assertEquals("envdata_folkestone.error_code", 
				attr.getAttributeDisplayName());
		assertEquals("string", attr.getType().getName());
		attr = columns.get(5);
		assertEquals("val1", attr.getAttributeSchemaName());
		assertEquals("envdata_folkestone.val1", 
				attr.getAttributeDisplayName());
		assertEquals("decimal", attr.getType().getName());
	}

}
