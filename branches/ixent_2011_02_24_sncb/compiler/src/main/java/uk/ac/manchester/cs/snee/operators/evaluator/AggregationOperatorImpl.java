package uk.ac.manchester.cs.snee.operators.evaluator;

import java.util.ArrayList;
import java.util.List;
import java.util.Observable;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.AggregationExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.DataAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.EvalTimeAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.evaluator.types.EvaluatorAttribute;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.evaluator.types.Window;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.operators.logical.AggregationOperator;
import uk.ac.manchester.cs.snee.operators.logical.AggregationType;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public class AggregationOperatorImpl
extends EvaluatorPhysicalOperator {
	//TODO: Refactor to form specific implementations for each operator
	Logger logger = 
		Logger.getLogger(AggregationOperatorImpl.class.getName());

	AggregationOperator aggregation;

	private List<Expression> expressions;

	public AggregationOperatorImpl(LogicalOperator op, int qid) 
	throws SNEEException, SchemaMetadataException,
	SNEEConfigurationException {
		super(op, qid);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER AggregationOperatorImpl() " + op);
		}

		// Instantiate this as a aggregation operator
		aggregation = (AggregationOperator) op;
		expressions = aggregation.getExpressions();

		if (logger.isTraceEnabled()) {
			logger.trace("Aggregration expression: " + expressions);
		}

		if (logger.isDebugEnabled()) {
			logger.debug("RETURN AggregationOperatorImpl()");
		}
	}

	//	@Override
	//	public Collection<Output> getNext() 
	//	throws ReceiveTimeoutException, SNEEException, EndOfResultsException {
	//		if (logger.isDebugEnabled()) {
	//			logger.debug("ENTER getNext()");
	//		}
	//		// Create bag for result items
	//		Collection<Output> result = new ArrayList<Output>();
	//
	//		// Get the next bag of stream items from the child operator
	//		Collection<Output> bagOfItems = child.getNext();
	//		if (logger.isTraceEnabled()) {
	//			logger.trace("Received " + bagOfItems.size() + 
	//			" stream items.");
	//		}
	//
	//		for (Output item : bagOfItems) {
	//			/*
	//			 * Aggregation operators can only perform over windows or relations
	//			 * Currently we will only implement the window version
	//			 */
	//			//TODO: Implement relation version of aggregation
	//			if (item instanceof Window) {
	//				Window curWindow =  (Window) item;
	//				if (logger.isTraceEnabled()) {
	//					logger.trace("Computing aggregate over window: " + 
	//							curWindow);
	//				}
	//				for (Expression exp : expressions){
	//					if (exp instanceof EvalTimeAttribute) {
	//						// Ignore eval time. Added by in-network SNEE compiler
	//						logger.trace("Ignore eval time expression.");
	//					} else if (exp instanceof AggregationExpression) {
	//						// Extract operands from expression
	//						AggregationExpression agEx = (AggregationExpression)exp;
	//						AggregationType agType = agEx.getAggregationType();
	//						DataAttribute da = (DataAttribute)agEx.getExpression();
	//						String attributeName = da.getLocalName() + "." + da.getAttributeName();
	//						AttributeType dataType = da.getType();
	//						if (logger.isTraceEnabled()) {
	//							logger.trace("Compute " + agType + " on " + attributeName + 
	//									" of type " + dataType.getName());
	//						}
	//
	//						// Calculate aggregate
	//						Number agValue = computeAggregate(agType, 
	//								attributeName, dataType, curWindow.getTuples());
	//
	//						// Create result window
	//						Window newWindow = createResultWindow(curWindow,
	//								agType, da, agValue);
	//						result.add(newWindow);
	//						if (logger.isTraceEnabled()) {
	//							logger.trace("Computed aggregate tuple: " + newWindow);
	//						}
	//					} else {
	//						logger.warn("Throwing exception due to incorrect expression type. " + exp);
	//						throw new SNEEException("Unexpected or unsupported expression type in " +
	//								"aggregation operator. " + exp.getClass());
	//					}
	//				}
	//			} else {
	//				logger.warn("Item type received from child is not known or unsupported by " +
	//						"aggregation operator. " + item.getClass().getSimpleName());
	//				throw new SNEEException("Unknown type of stream item.");
	//			}
	//		}
	//
	//		if (logger.isDebugEnabled()) {
	//			logger.debug("RETURN getNext() number of windows " + result.size());
	//		}
	//		return result;
	//	}

	@Override
	public void update(Observable obj, Object observed) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER update() for query " + m_qid + " " +
					" with " + observed);
		try {
			List<Output> result = new ArrayList<Output>();
			if (observed instanceof List<?>) {
				for (Object ob : (List)observed) 
					processOutput(ob, result);
			} else if (observed instanceof Output) {
				processOutput(observed, result);
			} else {
				logger.warn("Item type received from child is not " +
						"known or unsupported by " +
						"aggregation operator. " + 
						observed.getClass().getSimpleName());
				throw new SNEEException("Unknown type of stream item.");
			}
			if (!result.isEmpty()) {
				setChanged();
				notifyObservers(result);
			}
		} catch (Exception e) {
			logger.error(e);
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN update()");
		}
	}

	private void processOutput(Object observed, List<Output> result)
	throws SNEEException, SchemaMetadataException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER processOutput() with " + observed);
		}
		/*
		 * Aggregation operators can only perform over windows or
		 * relations
		 * Currently we will only implement the window version
		 */
		//TODO: Implement relation version of aggregation
		if (observed instanceof Window) {
			Window curWindow =  (Window) observed;
			if (logger.isTraceEnabled()) {
				logger.trace("Computing aggregate over window: " + 
						curWindow);
			}
			for (Expression exp : expressions){
				if (exp instanceof EvalTimeAttribute) {
					// Ignore eval time. Added by in-network SNEE compiler
					logger.trace("Ignore eval time expression.");
				} else if (exp instanceof AggregationExpression) {
					// Extract operands from expression
					AggregationExpression agEx = 
						(AggregationExpression)exp;
					AggregationType agType = agEx.getAggregationFunction();
					DataAttribute da = 
						(DataAttribute)agEx.getExpression();
					String attrDisplayName = 
						da.getAttributeDisplayName();
					AttributeType dataType = da.getType();
					if (logger.isTraceEnabled()) {
						logger.trace("Compute " + agType + " on " + 
								attrDisplayName + 
								" of type " + dataType.getName());
					}

					// Calculate aggregate
					Number agValue = computeAggregate(agType, 
							attrDisplayName, dataType,
							curWindow.getTuples());

					// Create result window
					Window newWindow = createResultWindow(curWindow,
							agType, da, agValue, da.getAttributeSchemaName());
					result.add(newWindow);
					if (logger.isTraceEnabled()) {
						logger.trace("Computed aggregate tuple: " + 
								newWindow);
					}
				} else {
					logger.warn("Throwing exception due to incorrect " +
							"expression type. " + exp);
					throw new SNEEException("Unexpected or unsupported " +
							"expression type in " +
							"aggregation operator. " + exp.getClass());
				}
			}
		} else {
			logger.warn("Item type received from child is not known " +
					"or unsupported by " +
					"aggregation operator. " + 
					observed.getClass().getSimpleName());
			throw new SNEEException("Unknown type of stream item.");
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN processOutput()");
	}

	private Window createResultWindow(Window curWindow, 
			AggregationType agType, DataAttribute da, Number agValue, 
			String extentName) 
	throws SchemaMetadataException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER createResultWindow()");
		}
		List<Tuple> tuples = new ArrayList<Tuple>();
		/*
		 *  Window only gets a value if the result of the aggregate is 
		 *  not null. Aggregate will result in null for example when 
		 *  the average is computed over an empty window. Other empty 
		 *  windows can generate meaningful answers,for example the 
		 *  count.
		 */
		if (agValue != null) {
			String agName = 
				agType.name()+"("+ da.getAttributeDisplayName()+")";
			Attribute attr = 
				new DataAttribute(extentName, agName, da.getType());
			EvaluatorAttribute evalAttr = 
				new EvaluatorAttribute(attr, agValue);
			List<EvaluatorAttribute> attributes = 
				new ArrayList<EvaluatorAttribute>();
			attributes.add(evalAttr);
			Tuple tuple = new Tuple(attributes);
			tuples.add(tuple);
		}
		Window newWindow = new Window(tuples);
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN createResultWindow()");
		}
		return newWindow;
	}

	private Number computeAggregate(AggregationType agType, 
			String attrDisplayName, AttributeType dataType, 
			List<Tuple> tuples) 
	throws SNEEException {
		//FIXME: Write test for this method
		if (logger.isTraceEnabled()) {
			logger.debug("ENTER computeAggregate() with " + agType + 
					", " + attrDisplayName + ", " + dataType);
		}
		Number result;
		//TODO: Add aggregate functions: SUM, MIN, MAX, STDEV
		if (agType == AggregationType.AVG) {
			logger.trace("Calculate average.");
			if (tuples.isEmpty()) {
				logger.trace("Average over an empty window is undefined. Return null");
				result = null;
			} else {
				if (dataType.getName().equalsIgnoreCase("integer")) 
					result = computeIntegerAverage(attrDisplayName, 
							tuples);
				else {
					logger.warn("Unsupported data type for computing " +
							"average. " + 
							dataType.getName());
					throw new SNEEException("Unsupported data type " +
							"for computing average");
				}
			}
		} else if (agType == AggregationType.COUNT) {
			logger.trace("Calculate count.");
			result = tuples.size();
		} else {
			//FIXME: Implement aggregate calculation
			logger.warn("Unsupported aggregation operator " + agType);
			throw new SNEEException("Unsupported aggregation " +
					"operator " + agType);
		}
		if (logger.isTraceEnabled()) {
			logger.debug("RETURN computeAggregate() with " + result);
		}
		return result;
	}

	private Number computeIntegerAverage(String attributeName,
			List<Tuple> tuples) 
	throws SNEEException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER computeIntegerAverage() with " +
					attributeName);
		}
		Integer average; 
		int count=0,totalValue=0;
		for (Tuple tuple : tuples) {
			Integer value = (Integer) 
				tuple.getAttributeValueByDisplayName(attributeName);
			totalValue += value;
			count++;
		}
		average = totalValue/count;
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN computeIntegerAverage() with " + 
					average);
		}
		return average;
	}

}
