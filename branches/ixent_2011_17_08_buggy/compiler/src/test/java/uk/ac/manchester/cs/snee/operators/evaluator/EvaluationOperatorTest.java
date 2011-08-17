package uk.ac.manchester.cs.snee.operators.evaluator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.Observable;

import org.apache.log4j.PropertyConfigurator;
import org.easymock.classextension.EasyMockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiType;
import uk.ac.manchester.cs.snee.evaluator.types.EvaluatorAttribute;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.operators.evaluator.EvaluationOperator;
import uk.ac.manchester.cs.snee.operators.evaluator.EvaluatorPhysicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public class EvaluationOperatorTest extends EasyMockSupport {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				EvaluationOperatorTest.class.getClassLoader().
				getResource("etc/log4j.properties"));
	}

	final Expression mockExpression = createMock(Expression.class);
	final Tuple mockTuple = createMock(Tuple.class);
	final LogicalOperator mockOp = createMock(LogicalOperator.class);
	final Iterator mockIt = createMock(Iterator.class);
	final EvaluatorPhysicalOperator mockPhyOp = createMock(EvaluatorPhysicalOperator.class);
	final List<Attribute> mockQPAttributesList = createMock(List.class);
	final Attribute mockAttr = createMock(Attribute.class);
	final EvaluatorAttribute mockField = createMock(EvaluatorAttribute.class);

	@Before
	public void setUp() throws Exception {
//		//Record create project operator behaviour
//		expect(mockOp.getAttributes()).andReturn(mockQPAttributesList);
	}

	@After
	public void tearDown() throws Exception {
	}


//	@Test(expected=SNEEException.class)
//	public void testEvaluateSingleExpression_invalidField() 
//	throws SNEEException, SchemaMetadataException {
//		/*
//		 * Record behaviour
//		 */
//		expect(mockExpression.getRequiredAttributes()).andReturn(mockQPAttributesList);
//		expect(mockQPAttributesList.get(0)).andReturn(mockAttr);
//		expect(mockAttr.getAttributeName()).andReturn("attrName");
//		expect(mockAttr.getLocalName()).andReturn("extent");
//		expect(mockTuple.getField("extent.attrName")).andThrow(new SNEEException("Non existent field"));
//		//Test
//		replayAll();
//		EvaluationOperator op = new EvaluationOperator(mockOp) {
//			
//			@Override
//			public void update(Observable obj, Object observed) {
//				// TODO Auto-generated method stub
//				
//			}
//		};
//		op.evaluateSingleExpression(mockExpression, mockTuple);
//		verifyAll();
//	}
//
//	@Test
//	public void testEvaluateSingleExpression_validField() 
//	throws SNEEException, SchemaMetadataException {
//		// Record behaviour
//		expect(mockExpression.getRequiredAttributes()).andReturn(mockQPAttributesList);
//		expect(mockQPAttributesList.get(0)).andReturn(mockAttr);
//		expect(mockAttr.getAttributeName()).andReturn("attrName");
//		expect(mockAttr.getLocalName()).andReturn("extent");
//		expect(mockTuple.getField("extent.attrName")).andReturn(mockField);
//		//Test
//		replayAll();
//		ProjectOperatorImpl op = new ProjectOperatorImpl(mockOp);
//		Field field = op.evaluateSingleExpression(mockExpression, mockTuple);
//		assertEquals(mockField, field);
//		verifyAll();
//	}

	@Ignore@Test
	public void testEvaluateMultiExpression() {
		fail("Not yet implemented");
	}

	@Test
	public void testEvaluate_floatIntMult() 
	throws SNEEException, SchemaMetadataException {
		//Record
		//Test
		replayAll();
		EvaluationOperator op = instantiateEvalOp();
		Object answer = op.evaluateNumeric(new Float(5.28), 9, MultiType.MULTIPLY);
//		System.out.println("Answer=" + answer);
		assertEquals(47.52, ((Number) answer).doubleValue(), 0.1);
		verifyAll();
	}

	@Test
	public void testEvaluate_bigDecIntMult() {
		//Record
		//Test
		replayAll();
		EvaluationOperator op = instantiateEvalOp();
		Object answer = op.evaluateNumeric(new BigDecimal(0.0000), 9, MultiType.MULTIPLY);
//		System.out.println("Answer=" + answer);
		assertEquals(0.0000, ((Number) answer).doubleValue(), 0.1);
		verifyAll();
	}
	
	@Test
	public void testEvaluate_intFloatMult() {
		//Record
		//Test
		replayAll();
		EvaluationOperator op = instantiateEvalOp();
		Object answer = op.evaluateNumeric(9, new Float(0.13), MultiType.MULTIPLY);
//		System.out.println("Answer=" + answer);
		assertEquals(1.17, ((Number) answer).doubleValue(), 0.1);
		verifyAll();
	}
	
	@Test
	public void testEvaluate_stringEquality() {
		//Record
		//Test
		replayAll();
		EvaluationOperator op = instantiateEvalOp();
		Object answer = op.evaluateString("hello", "hello", MultiType.EQUALS);
		System.out.println("Answer=" + answer);
		assertEquals(true, answer);
		verifyAll();
	}
	
	@Test
	public void testEvaluate_stringInEquality() {
		//Record
		//Test
		replayAll();
		EvaluationOperator op = instantiateEvalOp();
		Object answer = op.evaluateString("hello", "hello", MultiType.NOTEQUALS);
//		System.out.println("Answer=" + answer);
		assertEquals(false, answer);
		verifyAll();
	}

	private EvaluationOperator instantiateEvalOp() {
		EvaluationOperator op = new EvaluationOperator(mockPhyOp) {
			
			@Override
			public void update(Observable obj, Object observed) {
				// TODO Auto-generated method stub
				
			}
		};
		return op;
	}

}
