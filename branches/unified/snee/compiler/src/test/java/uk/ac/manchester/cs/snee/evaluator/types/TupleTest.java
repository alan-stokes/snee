/****************************************************************************\
*                                                                            *
*  SNEE (Sensor NEtwork Engine)                                              *
*  http://snee.cs.manchester.ac.uk/                                          *
*  http://code.google.com/p/snee                                             *
*                                                                            *
*  Release 1.x, 2009, under New BSD License.                                 *
*                                                                            *
*  Copyright (c) 2009, University of Manchester                              *
*  All rights reserved.                                                      *
*                                                                            *
*  Redistribution and use in source and binary forms, with or without        *
*  modification, are permitted provided that the following conditions are    *
*  met: Redistributions of source code must retain the above copyright       *
*  notice, this list of conditions and the following disclaimer.             *
*  Redistributions in binary form must reproduce the above copyright notice, *
*  this list of conditions and the following disclaimer in the documentation *
*  and/or other materials provided with the distribution.                    *
*  Neither the name of the University of Manchester nor the names of its     *
*  contributors may be used to endorse or promote products derived from this *
*  software without specific prior written permission.                       *
*                                                                            *
*  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS   *
*  IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, *
*  THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR    *
*  PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR          *
*  CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,     *
*  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,       *
*  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR        *
*  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF    *
*  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING      *
*  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS        *
*  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.              *
*                                                                            *
\****************************************************************************/
package uk.ac.manchester.cs.snee.evaluator.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.Types;

public class TupleTest {

	private static Types types;
	DateFormat df = new SimpleDateFormat("dd/MM/yyyy");
	DateFormat tf = new SimpleDateFormat("hh:mm:ss");
	DateFormat dtf = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				TupleTest.class.getClassLoader().
				getResource("etc/log4j.properties"));
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	private Tuple emptyTuple;
	private Tuple tuple;

	@Before
	public void setUp() throws Exception {
		//Configure properties
		Properties props = new Properties();
		props.setProperty(SNEEPropertyNames.INPUTS_TYPES_FILE, "Types.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_UNITS_FILE, "units.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_LOGICAL_SCHEMA_FILE, "logical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE, "etc/physical-schema.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_COST_PARAMETERS_FILE, "etc/cost-parameters.xml");
		props.setProperty(SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR, "output");
		SNEEProperties.initialise(props);

		URL fileUrl = FieldTest.class.getClassLoader().getResource(SNEEProperties.getSetting(SNEEPropertyNames.INPUTS_TYPES_FILE));
		types = new Types(fileUrl.toURI().toString());

		emptyTuple = new Tuple();
		tuple = new Tuple();
		tuple.addField(new Field("Int", types.getType("integer"), 6));
		tuple.addField(new Field("String", types.getType("string"), "test"));
		tuple.addField(new Field("Float", types.getType("float"), 2.45));
//		tuple.addField(new Field("Date", DataType.DATE, df.parse("25/11/2009")));
//		tuple.addField(new Field("Time", DataType.TIME, tf.parse("12:59:03")));
//		tuple.addField(new Field("DateTime", DataType.DATETIME, dtf.parse("03/09/1964 21:34:09")));
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetFields_notSet() {
		assertEquals(true, emptyTuple.getFields().isEmpty());
	}

	@Test(expected=SNEEException.class)
	public void testAddField_alreadyExists() 
	throws SNEEException, TypeMappingException, SchemaMetadataException {
		tuple.addField(new Field("Int", types.getType("integer"), 1));
	}

	@Test(expected=SNEEException.class)
	public void testAddField_alreadyExistsCaseInsensitive() 
	throws SNEEException, TypeMappingException, SchemaMetadataException {
		tuple.addField(new Field("float", types.getType("float"), 17.75));
	}
	
	@Test
	public void testGetFields() {
		assertEquals(3, tuple.getFields().size());//6
		assertEquals(true, (tuple.getFields() instanceof Map<?,?>));
	}

	@Test(expected=SNEEException.class)
	public void testGetField_notSet() throws SNEEException {
		emptyTuple.getField("field");
	}
	
	@Test(expected=SNEEException.class)
	public void testGetField_notExists() throws SNEEException {
		tuple.getField("field");
	}
	
	@Test@Ignore
	public void testGetField_exists() throws SNEEException {
//		tuple.getField("Time");
	}

	@Test@Ignore
	public void testGetField_existsCaseInsensitve() throws SNEEException {
//		tuple.getField("tiMe");
	}

	@Test(expected=SNEEException.class)@Ignore
	public void testGetValue_notSet() throws SNEEException {
//		emptyTuple.getValue("name");
	}

	@Test(expected=SNEEException.class)@Ignore
	public void testGetValue_notExists() throws SNEEException {
//		tuple.getValue("name");
	}
	
	@Test@Ignore
	public void testGetValue_exists() throws SNEEException {
//		tuple.getValue("float");
	}
	
	@Test@Ignore
	public void testGetValue_existsCase() throws SNEEException {
//		tuple.getValue("FLOAT");
	}
	
	@Test
	public void testContainsField_fieldExsits() {
		assertTrue(tuple.containsField("Int"));
	}
	
	@Test
	public void testContainsField_fieldExsitsCase() {
		assertTrue(tuple.containsField("INT"));
	}

	@Test
	public void testContainsField_fieldNotExsits() {
		assertFalse(tuple.containsField("hello"));
	}

}
