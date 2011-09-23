package uk.ac.manchester.cs.snee.operators.evaluator;

import static org.easymock.EasyMock.expect;
import static org.easymock.classextension.EasyMock.createMock;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.DataAttribute;
import uk.ac.manchester.cs.snee.metadata.schema.Types;
import uk.ac.manchester.cs.snee.operators.logical.ReceiveOperator;

public class ReceiveOperatorImplTest {

	private static Types types;
	private ReceiveOperator mockOp;
//	private ReceiveOperatorImpl mReceiveOp;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				ReceiveOperatorImplTest.class.getClassLoader().
				getResource("etc/log4j.properties"));
	}
	
	@Before
	public void setUp() throws Exception {
		Properties props = new Properties();
		props.setProperty(SNEEPropertyNames.INPUTS_TYPES_FILE, "Types.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_UNITS_FILE, "units.xml");
		props.setProperty(SNEEPropertyNames.INPUTS_LOGICAL_SCHEMA_FILE, "logical-schema.xml");
		props.setProperty(SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR, "output");
		SNEEProperties.initialise(props);
		
		mockOp = createMock(ReceiveOperator.class);
		
		ArrayList<Attribute> attributes = new ArrayList<Attribute>();
		attributes.add(new DataAttribute("stream", "attr", 
				types.getType("integer")));
		
		expect(mockOp.getInputAttributes()).andReturn(attributes);
		
		new ReceiveOperatorImpl(mockOp, 20);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test@Ignore
	public void testGetNext() {
		fail("Not yet implemented");
	}

}
