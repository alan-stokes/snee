package uk.ac.manchester.cs.snee.operators.sensornet;

import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.operators.evaluator.AggregationOperatorImpl;
import uk.ac.manchester.cs.snee.operators.evaluator.DeliverOperatorImpl;
import uk.ac.manchester.cs.snee.operators.evaluator.EvaluatorPhysicalOperator;
import uk.ac.manchester.cs.snee.operators.evaluator.JoinOperatorImpl;
import uk.ac.manchester.cs.snee.operators.evaluator.ProjectOperatorImpl;
import uk.ac.manchester.cs.snee.operators.evaluator.RStreamOperatorImpl;
import uk.ac.manchester.cs.snee.operators.evaluator.ReceiveOperatorImpl;
import uk.ac.manchester.cs.snee.operators.evaluator.SelectOperatorImpl;
import uk.ac.manchester.cs.snee.operators.evaluator.TimeWindowOperatorImpl;
import uk.ac.manchester.cs.snee.operators.evaluator.TupleWindowOperatorImpl;
import uk.ac.manchester.cs.snee.operators.evaluator.UnionOperatorImpl;
import uk.ac.manchester.cs.snee.operators.logical.AcquireOperator;
import uk.ac.manchester.cs.snee.operators.logical.AggregationOperator;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.JoinOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperatorImpl;
import uk.ac.manchester.cs.snee.operators.logical.ProjectOperator;
import uk.ac.manchester.cs.snee.operators.logical.RStreamOperator;
import uk.ac.manchester.cs.snee.operators.logical.ReceiveOperator;
import uk.ac.manchester.cs.snee.operators.logical.SelectOperator;
import uk.ac.manchester.cs.snee.operators.logical.UnionOperator;
import uk.ac.manchester.cs.snee.operators.logical.WindowOperator;

public abstract class SensornetOperatorImpl extends LogicalOperatorImpl implements
		SensornetOperator {

	protected Logger logger = Logger.getLogger(this.getClass().getName());
	
	/**
	 * The logical operator associated with this physical operator.
	 */
	private LogicalOperator logicalOp;
	
	public SensornetOperatorImpl(LogicalOperator op) 
	throws SNEEException, SchemaMetadataException {
		super(op);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SensornetOperatorImpl() " + op);
		}
		logicalOp = op;
		// Instantiate the child operator(s)
		int numChildren = op.getInDegree();
		for (int i=0; i<numChildren; i++) {
			LogicalOperator logicalChild = op.getInput(0);
			//TODO: Would we want this to be integrated with evaluator method?
			SensornetOperatorImpl phyChild = getSensornetOperator(logicalChild);
			this.setInput(phyChild, i);
			phyChild.setOutput(this, 0);			
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SensornetOperatorImpl() " + op);			
		}
	}

	@Override
	public SensornetOperatorImpl getSensornetOperator(LogicalOperator op)
	throws SNEEException, SchemaMetadataException {
		SensornetOperatorImpl phyOp = null;
		if (op instanceof AcquireOperator) {
			phyOp = new SensornetAcquireOperator(op);
		} else if (op instanceof AggregationOperator) {
			phyOp = new SensornetSingleStepAggregationOperator(op);
		} else if (op instanceof DeliverOperator) {
			phyOp = new SensornetDeliverOperator(op);
		} else if (op instanceof JoinOperator) {
			phyOp = new SensornetNestedLoopJoinOperator(op);
		} else if (op instanceof ProjectOperator) {
			phyOp = new SensornetProjectOperator(op);
		} else if (op instanceof RStreamOperator) {
			phyOp = new SensornetRStreamOperator(op);
		} else if (op instanceof SelectOperator) {
			phyOp = new SensornetSelectOperator(op);
//			} else if (op instanceof UnionOperator) {
//			phyOp = new UnionOperatorImpl(op);
		} else if (op instanceof WindowOperator) {
			phyOp = new SensornetWindowOperator(op);
//			if (((WindowOperator) op).isTimeScope()) {
//				phyOp = new SensornetTimeWindowOperator(op);
//			} else {
//				phyOp = new SensornetTupleWindowOperator(op);
//			}
		} else {
			String msg = "Unsupported operator " + op.getOperatorName();
			logger.warn(msg);
			throw new SNEEException(msg);
		}
		return phyOp;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean acceptsPredicates() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getFragID() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getPartitioningAttribute() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int[] getSourceSites() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getText(boolean showProperties) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isAttributeSensitive() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isFragmentRoot() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isLocationSensitive() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isRecursive() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<Attribute> getAttributes() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getCardinality(CardinalityType card) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public List<Expression> getExpressions() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isRemoveable() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void pushLocalNameDown(String newLocalName) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean pushProjectionDown(List<Expression> projectExpressions,
			List<Attribute> projectAttributes) throws OptimizationException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean pushSelectDown(Expression predicate)
			throws SchemaMetadataException, AssertionError,
			TypeMappingException {
		// TODO Auto-generated method stub
		return false;
	}

}
