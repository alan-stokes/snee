package uk.ac.manchester.cs.snee.operators.evaluator;

import java.util.ArrayList;
import java.util.Iterator;
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
import uk.ac.manchester.cs.snee.metadata.schema.SQLTypes;
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
					AggregationType agType = agEx.getAggregationType();
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
		} else if (agType == AggregationType.SUM) {
			logger.trace("Calculate sum.");
			result = computeSum( attrDisplayName, tuples );
		} else if (agType == AggregationType.MIN) {
			logger.trace("Calculate min.");
			result = computeMin( attrDisplayName, tuples );
		} else if (agType == AggregationType.MAX) {
			logger.trace("Calculate max.");
			result = computeMax( attrDisplayName, tuples );
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


	/** 
	 * Computes the sum of the tuples contained in the tuples list
	 * 
	 * @author lebiathan
	 * @param attributeName The name of the attribute which holds the
	 * data that will be summed
	 * @param tuples The set of tuples over which we want to compute
	 * the sum
	 *  */
	private Number computeSum( String attributeName,
			List<Tuple> tuples )
	throws SNEEException {

		if (logger.isTraceEnabled()) {
			logger.trace("ENTER computeSum() with " +
					attributeName);
		}

		/* Go through all of the tuples and compute their sum. The SUM is initially 0.
		 * If there are no items in the tuples list, the result will also be 0. */
		Number result = 0;
		if ( !tuples.isEmpty() ){

			/* Simply get the first tuple (there is definitely one) */
			Tuple tuple = tuples.get(0);
			List<String> attributeNames = tuple.getAttributeNames();
			int attrIndex = attributeNames.indexOf(attributeName);

			EvaluatorAttribute evalAttr = tuple.getAttribute(attrIndex);
			int attrType = evalAttr.getAttributeType();

			if ( attrType == SQLTypes.INTEGER.getSQLType() )
				result = computeIntegerSum(attrIndex, tuples);
			else if ( attrType == SQLTypes.FLOAT.getSQLType() )
				result = computeFloatSum(attrIndex, tuples);
			else if ( attrType == SQLTypes.DECIMAL.getSQLType() )
				result = computeDecimalSum(attrIndex, tuples);
			else{
				/* The attribute is not a number. Throw an exception! */
				logger.warn("Non-numerical attribute type " + evalAttr.getAttributeType());
				throw new SNEEException("Non-numerical attribute type " + 
						evalAttr.getAttributeType());
			}
		}

		if (logger.isTraceEnabled()) {
			logger.trace("RETURN computeSum() with " + 
					result);
		}
		return result;
	}

	/**
	 * Computes the sum over a set of tuples, for which we already know
	 * that they are of integer type. This saves time in what we are
	 * computing.
	 * 
	 * @author lebiathan
	 * 
	 * @param attrIndex The index of the attribute that holds the values
	 * we want to sum over the tuples 
	 * */
	private Number computeIntegerSum( int attrIndex, List<Tuple> tuples )
	throws SNEEException{
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER computeIntegerSum()");
		}

		int returnValue = 0;

		/* Create an iterator for the tuples */
		Iterator<Tuple> tplItr = tuples.iterator();
		while ( tplItr.hasNext() ){
			Tuple t = tplItr.next();
			Number n = (Number)t.getAttributeValue(attrIndex);
			returnValue += n.intValue();
		}

		if (logger.isTraceEnabled()) {
			logger.trace("RETURN FROM computeIntegerSum() with " + 
					returnValue);
		}
		return (Number)returnValue;
	}

	/**
	 * Computes the sum over a set of tuples, for which we already know
	 * that they are of integer type. This saves time in what we are
	 * computing.
	 * 
	 * @author lebiathan
	 * 
	 * @param attrIndex The index of the attribute that holds the values
	 * we want to sum over the tuples 
	 * */
	private Number computeFloatSum( int attrIndex, List<Tuple> tuples )
	throws SNEEException{
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER computeFloatSum()");
		}

		int returnValue = 0;

		/* Create an iterator for the tuples */
		Iterator<Tuple> tplItr = tuples.iterator();
		while ( tplItr.hasNext() ){
			Tuple t = tplItr.next();
			Number n = (Number)t.getAttributeValue(attrIndex);
			returnValue += n.floatValue();
		}

		if (logger.isTraceEnabled()) {
			logger.trace("RETURN FROM computeFloatSum() with " + 
					returnValue);
		}
		return (Number)returnValue;
	}


	/**
	 * Computes the sum over a set of tuples, for which we already know
	 * that they are of integer type. This saves time in what we are
	 * computing.
	 * 
	 * @author lebiathan
	 * 
	 * @param attrIndex The index of the attribute that holds the values
	 * we want to sum over the tuples 
	 * */
	private Number computeDecimalSum( int attrIndex, List<Tuple> tuples )
	throws SNEEException{
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER computeDecimalSum()");
		}

		int returnValue = 0;

		/* Create an iterator for the tuples */
		Iterator<Tuple> tplItr = tuples.iterator();
		while ( tplItr.hasNext() ){
			Tuple t = tplItr.next();
			Number n = (Number)t.getAttributeValue(attrIndex);
			returnValue += n.doubleValue();
		}

		if (logger.isTraceEnabled()) {
			logger.trace("RETURN FROM computeDecimalSum() with " + 
					returnValue);
		}
		return (Number)returnValue;
	}

	/** 
	 * Computes the minimum value over the set of tuples contained
	 * in the provided <i>tuples</i> list. In case the list is
	 * empty, 0 is returned
	 * 
	 * @author lebiathan
	 * @param attributeName The name of the attribute which holds the
	 * data that will be searched for the minimum value
	 * @param tuples The set of tuples over which we want to compute
	 * the minimum
	 * 
	 * @return The minimum number for attribute with attributeName over
	 * the set of <i>tuples</i>. If the tuples list is empty, 0 is
	 * returned.
	 *  */
	private Number computeMin( String attributeName,
			List<Tuple> tuples )
	throws SNEEException {

		if (logger.isTraceEnabled()) {
			logger.trace("ENTER computeMin() with " +
					attributeName);
		}

		/* Go through all of the tuples and compute their sum. The SUM is initially 0.
		 * If there are no items in the tuples list, the result will also be 0. */
		Number result = 0;
		if ( !tuples.isEmpty() ){

			/* Simply get the first tuple (there is definitely one) */
			Tuple tuple = tuples.get(0);
			List<String> attributeNames = tuple.getAttributeNames();
			int attrIndex = attributeNames.indexOf(attributeName);

			EvaluatorAttribute evalAttr = tuple.getAttribute(attrIndex);
			int attrType = evalAttr.getAttributeType();

			if ( attrType == SQLTypes.INTEGER.getSQLType() )
				result = computeIntegerMin(attrIndex, tuples);
			else if ( attrType == SQLTypes.FLOAT.getSQLType() )
				result = computeFloatMin(attrIndex, tuples);
			else if ( attrType == SQLTypes.DECIMAL.getSQLType() )
				result = computeDecimalMin(attrIndex, tuples);
			else{
				/* The attribute is not a number. Throw an exception! */
				logger.warn("Non-numerical attribute type " + evalAttr.getAttributeType());
				throw new SNEEException("Non-numerical attribute type " + 
						evalAttr.getAttributeType());
			}
		}

		if (logger.isTraceEnabled()) {
			logger.trace("RETURN computeMin() with " + 
					result);
		}
		return result;
	}

	/** 
	 * Computes the minimum value over the set of tuples contained
	 * in the provided <i>tuples</i> list, for which we know they
	 * are of INTEGER value. In case the list is empty, 0 is returned.
	 * 
	 * @author lebiathan
	 * @param attributeName The name of the attribute which holds the
	 * data that will be searched for the minimum value
	 * @param tuples The set of tuples over which we want to compute
	 * the minimum
	 * 
	 * @return The minimum number for attribute with attributeName over
	 * the set of <i>tuples</i>. If the tuples list is empty, 0 is
	 * returned.
	 *  */
	private Number computeIntegerMin( int attrIndex, List<Tuple> tuples )
	throws SNEEException {

		if (logger.isTraceEnabled()) {
			logger.trace("ENTER computeIntegerMin() with " + attrIndex);
		}

		int returnValue = 0;

		/* Create an iterator for the tuples */
		Iterator<Tuple> tplItr = tuples.iterator();
		Tuple t = tplItr.next();
		Number n = (Number)t.getAttributeValue(attrIndex);

		returnValue = n.intValue();

		while ( tplItr.hasNext() ){

			t = tplItr.next();
			n = (Number)t.getAttributeValue(attrIndex);

			if ( n.intValue() < returnValue )
				returnValue = n.intValue();
		}

		if (logger.isTraceEnabled()) {
			logger.trace("RETURN FROM computeIntegerMin() with " + 
					returnValue);
		}
		return (Number)returnValue;
	}

	
	/** 
	 * Computes the minimum value over the set of tuples contained
	 * in the provided <i>tuples</i> list, for which we know they
	 * are of INTEGER value. In case the list is empty, 0 is returned.
	 * 
	 * @author lebiathan
	 * @param attributeName The name of the attribute which holds the
	 * data that will be searched for the minimum value
	 * @param tuples The set of tuples over which we want to compute
	 * the minimum
	 * 
	 * @return The minimum number for attribute with attributeName over
	 * the set of <i>tuples</i>. If the tuples list is empty, 0 is
	 * returned.
	 *  */
	private Number computeFloatMin( int attrIndex, List<Tuple> tuples )
	throws SNEEException {

		if (logger.isTraceEnabled()) {
			logger.trace("ENTER computeFloatMin() with " + attrIndex);
		}

		float returnValue = 0.0f;

		/* Create an iterator for the tuples */
		Iterator<Tuple> tplItr = tuples.iterator();
		Tuple t = tplItr.next();
		Number n = (Number)t.getAttributeValue(attrIndex);

		returnValue = n.floatValue();

		while ( tplItr.hasNext() ){

			t = tplItr.next();
			n = (Number)t.getAttributeValue(attrIndex);

			if ( n.floatValue() < returnValue )
				returnValue = n.floatValue();
		}

		if (logger.isTraceEnabled()) {
			logger.trace("RETURN FROM computeFloatMin() with " + 
					returnValue);
		}
		return (Number)returnValue;
	}

	/** 
	 * Computes the minimum value over the set of tuples contained
	 * in the provided <i>tuples</i> list, for which we know they
	 * are of INTEGER value. In case the list is empty, 0 is returned.
	 * 
	 * @author lebiathan
	 * @param attributeName The name of the attribute which holds the
	 * data that will be searched for the minimum value
	 * @param tuples The set of tuples over which we want to compute
	 * the minimum
	 * 
	 * @return The minimum number for attribute with attributeName over
	 * the set of <i>tuples</i>. If the tuples list is empty, 0 is
	 * returned.
	 *  */
	private Number computeDecimalMin( int attrIndex, List<Tuple> tuples )
	throws SNEEException {

		if (logger.isTraceEnabled()) {
			logger.trace("ENTER computeDecimalMin() with " + attrIndex);
		}

		double returnValue = 0.0f;

		/* Create an iterator for the tuples */
		Iterator<Tuple> tplItr = tuples.iterator();
		Tuple t = tplItr.next();
		Number n = (Number)t.getAttributeValue(attrIndex);

		returnValue = n.doubleValue();

		while ( tplItr.hasNext() ){

			t = tplItr.next();
			n = (Number)t.getAttributeValue(attrIndex);

			if ( n.doubleValue() < returnValue )
				returnValue = n.doubleValue();
		}

		if (logger.isTraceEnabled()) {
			logger.trace("RETURN FROM computeDecimalMin() with " + 
					returnValue);
		}
		return (Number)returnValue;
	}

	/** 
	 * Computes the minimum value over the set of tuples contained
	 * in the provided <i>tuples</i> list. In case the list is
	 * empty, 0 is returned
	 * 
	 * @author lebiathan
	 * @param attributeName The name of the attribute which holds the
	 * data that will be searched for the minimum value
	 * @param tuples The set of tuples over which we want to compute
	 * the minimum
	 * 
	 * @return The minimum number for attribute with attributeName over
	 * the set of <i>tuples</i>. If the tuples list is empty, 0 is
	 * returned.
	 *  */
	private Number computeMax( String attributeName,
			List<Tuple> tuples )
	throws SNEEException {

		if (logger.isTraceEnabled()) {
			logger.trace("ENTER computeMax() with " +
					attributeName);
		}

		/* Go through all of the tuples and compute their sum. The SUM is initially 0.
		 * If there are no items in the tuples list, the result will also be 0. */
		Number result = 0;
		if ( !tuples.isEmpty() ){

			/* Simply get the first tuple (there is definitely one) */
			Tuple tuple = tuples.get(0);
			List<String> attributeNames = tuple.getAttributeNames();
			int attrIndex = attributeNames.indexOf(attributeName);

			EvaluatorAttribute evalAttr = tuple.getAttribute(attrIndex);
			int attrType = evalAttr.getAttributeType();

			if ( attrType == SQLTypes.INTEGER.getSQLType() )
				result = computeIntegerMax(attrIndex, tuples);
			else if ( attrType == SQLTypes.FLOAT.getSQLType() )
				result = computeFloatMax(attrIndex, tuples);
			else if ( attrType == SQLTypes.DECIMAL.getSQLType() )
				result = computeDecimalMax(attrIndex, tuples);
			else{
				/* The attribute is not a number. Throw an exception! */
				logger.warn("Non-numerical attribute type " + evalAttr.getAttributeType());
				throw new SNEEException("Non-numerical attribute type " + 
						evalAttr.getAttributeType());
			}
		}

		if (logger.isTraceEnabled()) {
			logger.trace("RETURN computeMax() with " + 
					result);
		}
		return result;
	}

	/** 
	 * Computes the minimum value over the set of tuples contained
	 * in the provided <i>tuples</i> list, for which we know they
	 * are of INTEGER value. In case the list is empty, 0 is returned.
	 * 
	 * @author lebiathan
	 * @param attributeName The name of the attribute which holds the
	 * data that will be searched for the minimum value
	 * @param tuples The set of tuples over which we want to compute
	 * the minimum
	 * 
	 * @return The minimum number for attribute with attributeName over
	 * the set of <i>tuples</i>. If the tuples list is empty, 0 is
	 * returned.
	 *  */
	private Number computeIntegerMax( int attrIndex, List<Tuple> tuples )
	throws SNEEException {

		if (logger.isTraceEnabled()) {
			logger.trace("ENTER computeIntegerMax() with " + attrIndex);
		}

		int returnValue = 0;

		/* Create an iterator for the tuples */
		Iterator<Tuple> tplItr = tuples.iterator();
		Tuple t = tplItr.next();
		Number n = (Number)t.getAttributeValue(attrIndex);

		returnValue = n.intValue();

		while ( tplItr.hasNext() ){

			t = tplItr.next();
			n = (Number)t.getAttributeValue(attrIndex);

			if ( n.intValue() > returnValue )
				returnValue = n.intValue();
		}

		if (logger.isTraceEnabled()) {
			logger.trace("RETURN FROM computeIntegerMax() with " + 
					returnValue);
		}
		return (Number)returnValue;
	}

	
	/** 
	 * Computes the minimum value over the set of tuples contained
	 * in the provided <i>tuples</i> list, for which we know they
	 * are of INTEGER value. In case the list is empty, 0 is returned.
	 * 
	 * @author lebiathan
	 * @param attributeName The name of the attribute which holds the
	 * data that will be searched for the minimum value
	 * @param tuples The set of tuples over which we want to compute
	 * the minimum
	 * 
	 * @return The minimum number for attribute with attributeName over
	 * the set of <i>tuples</i>. If the tuples list is empty, 0 is
	 * returned.
	 *  */
	private Number computeFloatMax( int attrIndex, List<Tuple> tuples )
	throws SNEEException {

		if (logger.isTraceEnabled()) {
			logger.trace("ENTER computeFloatMax() with " + attrIndex);
		}

		float returnValue = 0.0f;

		/* Create an iterator for the tuples */
		Iterator<Tuple> tplItr = tuples.iterator();
		Tuple t = tplItr.next();
		Number n = (Number)t.getAttributeValue(attrIndex);

		returnValue = n.floatValue();

		while ( tplItr.hasNext() ){

			t = tplItr.next();
			n = (Number)t.getAttributeValue(attrIndex);

			if ( n.floatValue() > returnValue )
				returnValue = n.floatValue();
		}

		if (logger.isTraceEnabled()) {
			logger.trace("RETURN FROM computeFloatMax() with " + 
					returnValue);
		}
		return (Number)returnValue;
	}

	/** 
	 * Computes the minimum value over the set of tuples contained
	 * in the provided <i>tuples</i> list, for which we know they
	 * are of INTEGER value. In case the list is empty, 0 is returned.
	 * 
	 * @author lebiathan
	 * @param attributeName The name of the attribute which holds the
	 * data that will be searched for the minimum value
	 * @param tuples The set of tuples over which we want to compute
	 * the minimum
	 * 
	 * @return The minimum number for attribute with attributeName over
	 * the set of <i>tuples</i>. If the tuples list is empty, 0 is
	 * returned.
	 *  */
	private Number computeDecimalMax( int attrIndex, List<Tuple> tuples )
	throws SNEEException {

		if (logger.isTraceEnabled()) {
			logger.trace("ENTER computeDecimalMax() with " + attrIndex);
		}

		double returnValue = 0.0f;

		/* Create an iterator for the tuples */
		Iterator<Tuple> tplItr = tuples.iterator();
		Tuple t = tplItr.next();
		Number n = (Number)t.getAttributeValue(attrIndex);

		returnValue = n.doubleValue();

		while ( tplItr.hasNext() ){

			t = tplItr.next();
			n = (Number)t.getAttributeValue(attrIndex);

			if ( n.doubleValue() > returnValue )
				returnValue = n.doubleValue();
		}

		if (logger.isTraceEnabled()) {
			logger.trace("RETURN FROM computeDecimalMax() with " + 
					returnValue);
		}
		return (Number)returnValue;
	}

	/** 
	 * Computes the standard deviation over the set of tuples contained
	 * in the provided <i>tuples</i> list. In case the list has fewer
	 * than 2 elements, 0 is returned
	 * 
	 * @author lebiathan
	 * @param attributeName The name of the attribute which holds the
	 * data over which we compute the standard deviation
	 * @param tuples The set of tuples over which we want will compute
	 * the standard deviation
	 * 
	 * @return The standard deviatino for attribute with attributeName over
	 * the set of <i>tuples</i>. In case the list has fewer
	 * than 2 elements, 0 is returned.
	 *  */
	private Number computeStandardDeviation( String attributeName,
			List<Tuple> tuples )
	throws SNEEException {

		if (logger.isTraceEnabled()) {
			logger.trace("ENTER computeStandardDeviation() with " +
					attributeName);
		}

		/* Go through all of the tuples and compute their sum. The SUM is initially 0.
		 * If there are no items in the tuples list, the result will also be 0. */
		Number result = 0;
		if ( !tuples.isEmpty() ){

			/* Simply get the first tuple (there is definitely one) */
			Tuple tuple = tuples.get(0);
			List<String> attributeNames = tuple.getAttributeNames();
			int attrIndex = attributeNames.indexOf(attributeName);

			EvaluatorAttribute evalAttr = tuple.getAttribute(attrIndex);
			int attrType = evalAttr.getAttributeType();

			if ( attrType == SQLTypes.INTEGER.getSQLType() )
				result = computeIntegerStandardDeviation(attrIndex, tuples);
			else if ( attrType == SQLTypes.FLOAT.getSQLType() )
				result = computeFloatStandardDeviation(attrIndex, tuples);
			else if ( attrType == SQLTypes.DECIMAL.getSQLType() )
				result = computeDecimalStandardDeviation(attrIndex, tuples);
			else{
				/* The attribute is not a number. Throw an exception! */
				logger.warn("Non-numerical attribute type " + evalAttr.getAttributeType());
				throw new SNEEException("Non-numerical attribute type " + 
						evalAttr.getAttributeType());
			}
		}

		if (logger.isTraceEnabled()) {
			logger.trace("RETURN computeMax() with " + 
					result);
		}
		return result;
	}

	/** 
	 * Computes the standard deviation over the set of tuples contained
	 * in the provided <i>tuples</i> list, for which we know they
	 * are of INTEGER value. In case the list has less than 2 items,
	 * 0 is returned
	 * 
	 * @author lebiathan
	 * @param attributeName The name of the attribute which holds the
	 * data over which we compute the standard deviation
	 * @param tuples The set of tuples over which we want to compute
	 * the standard deviation
	 * 
	 * @return The standard deviation for the attrIndex-th attribute
	 * of all tuples in the tuples list.
	 *  */
	private Number computeIntegerStandardDeviation( int attrIndex, List<Tuple> tuples  )
	throws SNEEException {

		if (logger.isTraceEnabled()) {
			logger.trace("ENTER computeIntegerStandardDeviation() with " + attrIndex);
		}

		/* Do a simple first check. We need at least two items in the tuples list
		 * to compute the standard deviation. Otherwise, we simply return 0 */
		if ( tuples.size() < 2 )
			return (Number)0;
		
		/* Standard deviation requires that the following values are computed:
		 * 1) Average of the tuples (so we need the sum)
		 * 2) Count
		 * 3) Deviation from the mean */

		int sum = 0;

		/* Create an iterator for the tuples */
		Iterator<Tuple> tplItr = tuples.iterator();
		Tuple t = tplItr.next();
		Number n = (Number)t.getAttributeValue(attrIndex);

		/* Compute the sum */
		while ( tplItr.hasNext() ){

			t = tplItr.next();
			n = (Number)t.getAttributeValue(attrIndex);

			sum += n.intValue();
		}

		/* Compute the sum of the deviation */
		int average = sum / tuples.size();
		int deviation = 0;

		tplItr = tuples.iterator();
		while ( tplItr.hasNext() ){

			t = tplItr.next();
			n = (Number)t.getAttributeValue(attrIndex);

			deviation += (n.intValue() - average) * (n.intValue() - average);
		}

		/* We have the deviation at this point. Return the standard deviation.
		 * The result will be of Integer type */
		Number returnValue = (int)(Math.sqrt( deviation ) / Math.sqrt( tuples.size() - 1 ));

		if (logger.isTraceEnabled()) {
			logger.trace("RETURN FROM computeIntegerStandardDeviation() with " +  returnValue);
		}
		return (Number)returnValue;

	}

	/** 
	 * Computes the standard deviation over the set of tuples contained
	 * in the provided <i>tuples</i> list, for which we know they
	 * are of DECIMAL value. In case the list has less than 2 items,
	 * 0 is returned
	 * 
	 * @author lebiathan
	 * @param attributeName The name of the attribute which holds the
	 * data over which we compute the standard deviation
	 * @param tuples The set of tuples over which we want to compute
	 * the standard deviation
	 * 
	 * @return The standard deviation for the attrIndex-th attribute
	 * of all tuples in the tuples list.
	 *  */
	private Number computeFloatStandardDeviation( int attrIndex, List<Tuple> tuples  )
	throws SNEEException {

		if (logger.isTraceEnabled()) {
			logger.trace("ENTER computeFloatStandardDeviation() with " + attrIndex);
		}

		/* Do a simple first check. We need at least two items in the tuples list
		 * to compute the standard deviation. Otherwise, we simply return 0 */
		if ( tuples.size() < 2 )
			return (Number)0.0;
		
		/* Standard deviation requires that the following values are computed:
		 * 1) Average of the tuples (so we need the sum)
		 * 2) Count
		 * 3) Deviation from the mean */

		float sum = 0.0f;

		/* Create an iterator for the tuples */
		Iterator<Tuple> tplItr = tuples.iterator();
		Tuple t = tplItr.next();
		Number n = (Number)t.getAttributeValue(attrIndex);

		/* Compute the sum */
		while ( tplItr.hasNext() ){

			t = tplItr.next();
			n = (Number)t.getAttributeValue(attrIndex);

			sum += n.floatValue();
		}

		/* Compute the sum of the deviation */
		float deviation = 0;
		float average = sum / (float)tuples.size();

		tplItr = tuples.iterator();
		while ( tplItr.hasNext() ){

			t = tplItr.next();
			n = (Number)t.getAttributeValue(attrIndex);

			deviation += (n.floatValue() - average) * (n.floatValue() - average);
		}

		/* We have the deviation at this point. Return the standard deviation */
		Number returnValue = (float)(Math.sqrt( deviation ) / Math.sqrt( tuples.size() - 1 ));

		if (logger.isTraceEnabled()) {
			logger.trace("RETURN FROM computeFloatStandardDeviation() with " +  returnValue);
		}

		return (Number)returnValue;
	}

	/** 
	 * Computes the standard deviation over the set of tuples contained
	 * in the provided <i>tuples</i> list, for which we know they
	 * are of FLOAT value. In case the list has less than 2 items,
	 * 0 is returned
	 * 
	 * @author lebiathan
	 * @param attributeName The name of the attribute which holds the
	 * data over which we compute the standard deviation
	 * @param tuples The set of tuples over which we want to compute
	 * the standard deviation
	 * 
	 * @return The standard deviation for the attrIndex-th attribute
	 * of all tuples in the tuples list.
	 *  */
	private Number computeDecimalStandardDeviation( int attrIndex, List<Tuple> tuples  )
	throws SNEEException {

		if (logger.isTraceEnabled()) {
			logger.trace("ENTER computeFloatStandardDeviation() with " + attrIndex);
		}

		/* Do a simple first check. We need at least two items in the tuples list
		 * to compute the standard deviation. Otherwise, we simply return 0 */
		if ( tuples.size() < 2 )
			return (Number)0.0;
		
		/* Standard deviation requires that the following values are computed:
		 * 1) Average of the tuples (so we need the sum)
		 * 2) Count
		 * 3) Deviation from the mean */

		double sum = 0.0;

		/* Create an iterator for the tuples */
		Iterator<Tuple> tplItr = tuples.iterator();
		Tuple t = tplItr.next();
		Number n = (Number)t.getAttributeValue(attrIndex);

		/* Compute the sum */
		while ( tplItr.hasNext() ){

			t = tplItr.next();
			n = (Number)t.getAttributeValue(attrIndex);

			sum += n.doubleValue();
		}

		/* Compute the sum of the deviation */
		double deviation = 0;
		double average = (double)sum / (double)tuples.size();

		tplItr = tuples.iterator();
		while ( tplItr.hasNext() ){

			t = tplItr.next();
			n = (Number)t.getAttributeValue(attrIndex);

			deviation += (n.doubleValue() - average) * (n.doubleValue() - average);
		}

		/* We have the deviation at this point. Return the standard deviation */
		Number returnValue = (float)(Math.sqrt( deviation ) / Math.sqrt( tuples.size() - 1 ));

		if (logger.isTraceEnabled()) {
			logger.trace("RETURN FROM computeFloatStandardDeviation() with " +  returnValue);
		}

		return (Number)returnValue;
	}


}
