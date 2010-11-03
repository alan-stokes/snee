package uk.ac.manchester.cs.snee.data.webservice;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
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
import uk.ac.manchester.cs.snee.datasource.webservice.OgsadaiSchemaParser;

public class OgsadaiSchemaParserTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				OgsadaiSchemaParserTest.class.getClassLoader().getResource(
						"etc/log4j.properties"));
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	private OgsadaiSchemaParser createParser(String filename)
	throws TypeMappingException, IOException,
			SchemaMetadataException {
		InputStream is = 
			OgsadaiSchemaParser.class.getClassLoader().
					getResourceAsStream("etc/" + filename);
		Types types = new Types(OgsadaiSchemaParser.class.getClassLoader().
				getResource("etc/Types.xml").toString());
		byte[] bytes = new byte[is.available()];
		is.read(bytes);
		String schema = new String(bytes);
		OgsadaiSchemaParser parser = 
			new OgsadaiSchemaParser(schema, types);
		return parser;
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetExtentNames_1schema() 
	throws TypeMappingException, IOException, SchemaMetadataException {
		OgsadaiSchemaParser parser = 
			createParser("ogsa-dai-1schema-description.xml");
		assertEquals(1, parser.getExtentNames().size());
	}

	@Test
	public void testGetExtentNames_2schema() 
	throws TypeMappingException, IOException, SchemaMetadataException {
		OgsadaiSchemaParser parser = 
			createParser("ogsa-dai-2schema-description.xml");
		assertEquals(2, parser.getExtentNames().size());
	}

	@Test
	public void testGetColumns_Folkestone() 
	throws TypeMappingException, SchemaMetadataException, IOException {
		OgsadaiSchemaParser parser = 
			createParser("ogsa-dai-1schema-description.xml");
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

	@Test
	public void testGetColumns_Locations() 
	throws TypeMappingException, SchemaMetadataException, IOException {
		OgsadaiSchemaParser parser = 
			createParser("ogsa-dai-2schema-description.xml");
		List<Attribute> columns =
			parser.getColumns("locations");
		assertEquals(16, columns.size());
	}
	
}
