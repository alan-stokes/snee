package uk.ac.manchester.cs.snee.data.webservice;

import static org.junit.Assert.assertEquals;

import java.io.InputStream;
import java.util.Collection;
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

public class WrsSchemaParserTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				WrsSchemaParserTest.class.getClassLoader().getResource(
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
		Collection<String> extentNames = parser.getExtentNames();
		assertEquals(1, extentNames.size());
		assertEquals("envdata_lymington_met", extentNames.iterator().next());
	}

	@Test
	public void testGetColumns() 
	throws TypeMappingException, SchemaMetadataException {
		List<Attribute> columns = 
			parser.getColumns("envdata_lymington_met");
		assertEquals(7, columns.size());
		
		Attribute attr = columns.get(0);
		assertEquals("timestamp", attr.getAttributeSchemaName());
		assertEquals("envdata_lymington_met.timestamp", 
				attr.getAttributeDisplayName());
		assertEquals("integer", attr.getType().getName());
		
		attr = columns.get(1);
		assertEquals("datetime", attr.getAttributeSchemaName());
		assertEquals("envdata_lymington_met.datetime", 
				attr.getAttributeDisplayName());
		assertEquals("timestamp", attr.getType().getName());
		
		attr = columns.get(2);
		assertEquals("airpressure", attr.getAttributeSchemaName());
		assertEquals("envdata_lymington_met.airpressure", 
				attr.getAttributeDisplayName());
		assertEquals("float", attr.getType().getName());
		
		attr = columns.get(3);
		assertEquals("windspeed", attr.getAttributeSchemaName());
		assertEquals("envdata_lymington_met.windspeed", 
				attr.getAttributeDisplayName());
		assertEquals("decimal", attr.getType().getName());
	}

}
