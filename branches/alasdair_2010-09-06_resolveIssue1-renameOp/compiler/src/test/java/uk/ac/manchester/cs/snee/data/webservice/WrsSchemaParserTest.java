package uk.ac.manchester.cs.snee.data.webservice;

import static org.junit.Assert.*;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.manchester.cs.snee.compiler.metadata.schema.Attribute;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.Types;
import uk.ac.manchester.cs.snee.evaluator.QueryEvaluatorTest;

public class WrsSchemaParserTest {

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

	private WrsSchemaParser parser;

	@Before
	public void setUp() throws Exception {
		InputStream is = 
			OgsadaiSchemaParser.class.getClassLoader().
					getResourceAsStream("etc/cco-wrs-metadata-schema-description.xml");
		Types types = new Types(OgsadaiSchemaParser.class.getClassLoader().
				getResource("etc/Types.xml").toString());
		byte[] bytes = new byte[is.available()];
		is.read(bytes);
		String schema = new String(bytes);
		parser = new WrsSchemaParser(schema, types);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetExtentName() 
	throws SchemaMetadataException {
		assertEquals("envdata_lymington_met", parser.getExtentName());
	}

	@Test
	public void testGetColumns() 
	throws TypeMappingException, SchemaMetadataException {
		List<Attribute> columns = 
			parser.getColumns("envdata_lymington_met");
		assertEquals(7, columns.size());
		Attribute attr = columns.get(0);
		assertEquals("Timestamp", attr.getName());
		assertEquals("integer", attr.getType().getName());
		attr = columns.get(1);
		assertEquals("DateTime", attr.getName());
		assertEquals("timestamp", attr.getType().getName());
		attr = columns.get(2);
		assertEquals("AirPressure", attr.getName());
		assertEquals("float", attr.getType().getName());
		attr = columns.get(3);
		assertEquals("WindSpeed", attr.getName());
		assertEquals("decimal", attr.getType().getName());
	}

}
