package uk.ac.manchester.cs.snee.operators.evaluator;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;
import org.easymock.classextension.EasyMockSupport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.manchester.cs.snee.EvaluatorException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.IntLiteral;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiType;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.NoPredicate;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.StringLiteral;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.operators.logical.ScanOperator;

public class ScanOperatorImplTest extends EasyMockSupport {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				ScanOperatorImplTest.class.getClassLoader().getResource(
						"etc/log4j.properties"));
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	private AttributeType mockIntType = createMock(AttributeType.class);
	private AttributeType mockStringType = createMock(AttributeType.class);

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testConstructQuery_noProjectionNoSelection() 
	throws SchemaMetadataException, EvaluatorException {
		ScanOperator mockOp = createMock(ScanOperator.class);
		Attribute attr1 = createMock(Attribute.class);
		Attribute attr2 = createMock(Attribute.class);
		
		List<Attribute> attrList = new ArrayList<Attribute>();
		attrList.add(attr1);
		attrList.add(attr2);
		
		// Logical scan operator method calls
		expect(mockOp.getInputAttributes()).andReturn(attrList);
		expect(mockOp.getExtentName()).andReturn("relation");
		expect(mockOp.getPredicate()).andReturn(new NoPredicate());
		expect(mockOp.getRescanInterval()).andReturn(null);
		
		// Attribute 1 method calls
		expect(attr1.getAttributeSchemaName()).andReturn("attr1").anyTimes();

		// Attribute 2 method calls
		expect(attr2.getAttributeSchemaName()).andReturn("attr2").anyTimes();
		
		replayAll();
		ScanOperatorImpl scan = new ScanOperatorImpl(mockOp, 1);
		String query = scan.constructQuery();
		assertEquals("SELECT attr1, attr2 FROM relation", query);
		verifyAll();
	}

	@Test
	public void testConstructQuery_projectionNoSelection() 
	throws SchemaMetadataException, EvaluatorException {
		ScanOperator mockOp = createMock(ScanOperator.class);
//		Attribute attr1 = createMock(Attribute.class);
		Attribute attr2 = createMock(Attribute.class);
		
		List<Attribute> attrList = new ArrayList<Attribute>();
//		attrList.add(attr1);
		attrList.add(attr2);
		
		// Logical scan operator method calls
		expect(mockOp.getInputAttributes()).andReturn(attrList);
		expect(mockOp.getExtentName()).andReturn("relation");
		expect(mockOp.getPredicate()).andReturn(new NoPredicate());
		expect(mockOp.getRescanInterval()).andReturn(null);
//		
//		// Attribute 1 method calls
//		expect(attr1.getAttributeSchemaName()).andReturn("attr1").anyTimes();

		// Attribute 2 method calls
		expect(attr2.getAttributeSchemaName()).andReturn("attr2").anyTimes();
		
		replayAll();
		ScanOperatorImpl scan = new ScanOperatorImpl(mockOp, 1);
		String query = scan.constructQuery();
		assertEquals("SELECT attr2 FROM relation", query);
		verifyAll();
	}

	@Test
	public void testConstructQuery_LessEqualsSelection() 
	throws SchemaMetadataException, EvaluatorException {
		ScanOperator mockOp = createMock(ScanOperator.class);
		Attribute attr1 = createMock(Attribute.class);
		Attribute attr2 = createMock(Attribute.class);
		MultiExpression expr = createMock(MultiExpression.class);
		
		//Attribute list
		List<Attribute> attrList = new ArrayList<Attribute>();
		attrList.add(attr1);
		attrList.add(attr2);
		
		// Logical scan operator method calls
		expect(mockOp.getInputAttributes()).andReturn(attrList);
		expect(mockOp.getExtentName()).andReturn("relation");
		expect(mockOp.getPredicate()).andReturn(expr);
		expect(mockOp.getRescanInterval()).andReturn(null);
		
		// Attribute 1 method calls
		expect(attr1.getAttributeSchemaName()).andReturn("attr1").anyTimes();

		// Attribute 2 method calls
		expect(attr2.getAttributeSchemaName()).andReturn("attr2").anyTimes();
		
		//Expression
		Expression[] expressions = new Expression[2];
		expressions[0] = attr1;
		expressions[1] = new IntLiteral(42, mockIntType);
		expect(expr.getExpressions()).andReturn(expressions).anyTimes();
		expect(expr.getMultiType()).andReturn(MultiType.LESSTHANEQUALS).anyTimes();
		
		replayAll();

		ScanOperatorImpl scan = new ScanOperatorImpl(mockOp, 1);
		String query = scan.constructQuery();
		assertEquals("SELECT attr1, attr2 FROM relation WHERE attr1 <= 42", query);
		verifyAll();
	}

	@Test
	public void testConstructQuery_AndSelection() 
	throws SchemaMetadataException, EvaluatorException {
		ScanOperator mockOp = createMock(ScanOperator.class);
		Attribute attr1 = createMock(Attribute.class);
		Attribute attr2 = createMock(Attribute.class);
		MultiExpression expr1 = createMock(MultiExpression.class);
		MultiExpression expr2 = createMock(MultiExpression.class);
		MultiExpression expr3 = createMock(MultiExpression.class);
		
		//Attribute list
		List<Attribute> attrList = new ArrayList<Attribute>();
		attrList.add(attr1);
		attrList.add(attr2);
		
		// Logical scan operator method calls
		expect(mockOp.getInputAttributes()).andReturn(attrList);
		expect(mockOp.getExtentName()).andReturn("relation");
		expect(mockOp.getPredicate()).andReturn(expr3);
		expect(mockOp.getRescanInterval()).andReturn(null);
		
		// Attribute 1 method calls
		expect(attr1.getAttributeSchemaName()).andReturn("attr1").anyTimes();

		// Attribute 2 method calls
		expect(attr2.getAttributeSchemaName()).andReturn("attr2").anyTimes();
		
		//Expressions
		Expression[] expressions1 = new Expression[2];
		expressions1[0] = attr1;
		expressions1[1] = new IntLiteral(42, mockIntType);
		expect(expr1.getExpressions()).andReturn(expressions1).anyTimes();
		expect(expr1.getMultiType()).andReturn(MultiType.LESSTHANEQUALS).anyTimes();
		
		Expression[] expressions2 = new Expression[2];
		expressions2[0] = attr2;
		expressions2[1] = new StringLiteral("'hello'", mockStringType);
		expect(expr2.getExpressions()).andReturn(expressions2).anyTimes();
		expect(expr2.getMultiType()).andReturn(MultiType.EQUALS).anyTimes();
		
		Expression[] expressions3 = new Expression[2];
		expressions3[0] = expr1;
		expressions3[1] = expr2;
		expect(expr3.getExpressions()).andReturn(expressions3).anyTimes();
		expect(expr3.getMultiType()).andReturn(MultiType.AND).anyTimes();
		
		replayAll();

		ScanOperatorImpl scan = new ScanOperatorImpl(mockOp, 1);
		String query = scan.constructQuery();
		assertEquals("SELECT attr1, attr2 FROM relation WHERE attr1 <= 42 AND attr2 = 'hello'", 
				query);
		verifyAll();
	}

	@Test(expected=EvaluatorException.class)
	public void testConstructQuery_PowerSelection() 
	throws SchemaMetadataException, EvaluatorException {
		ScanOperator mockOp = createMock(ScanOperator.class);
		Attribute attr1 = createMock(Attribute.class);
		Attribute attr2 = createMock(Attribute.class);
		MultiExpression expr = createMock(MultiExpression.class);
		
		//Attribute list
		List<Attribute> attrList = new ArrayList<Attribute>();
		attrList.add(attr1);
		attrList.add(attr2);
		
		// Logical scan operator method calls
		expect(mockOp.getInputAttributes()).andReturn(attrList);
		expect(mockOp.getExtentName()).andReturn("relation");
		expect(mockOp.getPredicate()).andReturn(expr);
		expect(mockOp.getRescanInterval()).andReturn(null);
		
		// Attribute 1 method calls
		expect(attr1.getAttributeSchemaName()).andReturn("attr1").anyTimes();

		// Attribute 2 method calls
		expect(attr2.getAttributeSchemaName()).andReturn("attr2").anyTimes();
		
		//Expressions		
		Expression[] expressions = new Expression[2];
		expressions[0] = attr1;
		expressions[1] = new IntLiteral(4, mockIntType);
		expect(expr.getExpressions()).andReturn(expressions).anyTimes();
		expect(expr.getMultiType()).andReturn(MultiType.POWER).anyTimes();
		
		replayAll();

		ScanOperatorImpl scan = new ScanOperatorImpl(mockOp, 1);
		scan.constructQuery();
		verifyAll();
	}

}
