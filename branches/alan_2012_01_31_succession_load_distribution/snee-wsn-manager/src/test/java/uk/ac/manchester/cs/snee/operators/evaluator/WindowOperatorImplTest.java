package uk.ac.manchester.cs.snee.operators.evaluator;


import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.evaluator.EndOfResultsException;
import uk.ac.manchester.cs.snee.evaluator.types.ReceiveTimeoutException;
import uk.ac.manchester.cs.snee.operators.evaluator.DeliverOperatorImpl;
import uk.ac.manchester.cs.snee.operators.evaluator.WindowOperatorImpl;

public class WindowOperatorImplTest {
	
	@SuppressWarnings("unused")
  private static final Logger logger = Logger.getLogger(DeliverOperatorImpl.class.getName());

	@SuppressWarnings("unused")
  private WindowOperatorImpl tupleWindowOp;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				WindowOperatorImplTest.class.getClassLoader().
				getResource("etc/log4j.properties"));
	}
	
	@Before
	public void setUp() {
	}

	@After
	public void tearDown() {
	}
	
	@Test@Ignore
	public void testGetNext() 
	throws ReceiveTimeoutException, SNEEException, OptimizationException, 
	EndOfResultsException {
//		tupleWindowOp = new WindowOperatorImpl(new MockWindowOperator());
//		tupleWindowOp.getNext();
	}

//	class MockWindowOperator extends WindowOperator {
//
//		Collection<Operator> childOps = new ArrayList<Operator>();
//		private String mOperatorName = "MOCKWINDOW";
//		
//		protected MockWindowOperator() throws OptimizationException {
//			// Construct a now row window with a slide of 5
//			super(0, 0, false, 0, 5, new MockOperator());
//			System.out.println("ENTER MockWindowOperator");
//			Operator childOp = new MockOperator();
//			childOps.add(childOp);
//			System.out.println("EXIT MockWindowOperator");
//		}
//
//		public int getDataMemoryCost(int numberOfInstances) {
//			// TODO Auto-generated method stub
//			return 0;
//		}
//		
//	}
	
}
