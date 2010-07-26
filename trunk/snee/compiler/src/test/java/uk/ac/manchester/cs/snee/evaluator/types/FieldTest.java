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

import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.Types;

public class FieldTest {

	private static Types types;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure("etc/log4j.properties");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		// Configure properties
		Properties props = new Properties();
		props.setProperty(SNEEPropertyNames.INPUTS_TYPES_FILE, "Types.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_UNITS_FILE, "units.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_SCHEMA_FILE, "logical-schema.xml");
		props.setProperty(SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR, "output");
		SNEEProperties.initialise(props);

		URL fileUrl = FieldTest.class.getClassLoader().getResource(SNEEProperties.getSetting(SNEEPropertyNames.INPUTS_TYPES_FILE));
		types = new Types(fileUrl.toURI().toString());

	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testTypeString() 
	throws TypeMappingException, SchemaMetadataException {
		Field stringField = new Field("StringField", types.getType("string"), "hello");
		assertEquals("StringField", stringField.getName());
		assertEquals("hello", stringField.getData());
		assertEquals(types.getType("string"), stringField.getDataType());
	}

	@Test
	public void testTypeFloat() 
	throws TypeMappingException, SchemaMetadataException {
		Field field = new Field("Float Field", types.getType("float"), 17.75);
		assertEquals("Float Field", field.getName());
		assertEquals(17.75, field.getData());
		assertEquals(types.getType("float"), field.getDataType());
	}
		
	@Test
	public void testTypeInt() 
	throws TypeMappingException, SchemaMetadataException {
		Field intField = new Field("IntField", types.getType("integer"), 8);
		assertEquals(types.getType("integer"), intField.getDataType());
		assertEquals(8, intField.getData());
		assertEquals(types.getType("integer"), intField.getDataType());
	}

	@Ignore
	@Test(expected=TypeMappingException.class)
	public void testTypeInt_invalidValue() 
	throws TypeMappingException, SchemaMetadataException {
		//XXX: ideally we would want this test to fail in some way
		Field intField = new Field("IntField", types.getType("integer"), "number");
	}
	
	@Test
	public void testTypeBoolean() 
	throws TypeMappingException, SchemaMetadataException {
		Field field = new Field("BoolField", types.getType("boolean"), true);
		assertEquals("BoolField", field.getName());
		assertEquals(true, field.getData());
		assertEquals(types.getType("boolean"), field.getDataType());
	}

}
