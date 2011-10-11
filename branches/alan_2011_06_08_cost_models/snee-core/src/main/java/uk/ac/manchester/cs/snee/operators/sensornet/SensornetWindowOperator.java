package uk.ac.manchester.cs.snee.operators.sensornet;


import java.util.ArrayList;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.WindowOperator;

public class SensornetWindowOperator extends SensornetOperatorImpl {
	
	/**
   * serialVersionUID
   */
  private static final long serialVersionUID = 8640337067957570679L;

  private static final Logger logger = Logger.getLogger(SensornetWindowOperator.class.getName());
	
	WindowOperator winOp;
	
	public SensornetWindowOperator(LogicalOperator op, CostParameters costParams) 
	throws SNEEException, SchemaMetadataException {
		super(op, costParams);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SensornetWindowOperator() " + op);
			logger.debug("Attribute List: " + op.getAttributes());
			logger.debug("Expression List: " + op.getExpressions());
		}
		winOp = (WindowOperator) op;
		this.setNesCTemplateName("window");
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SensornetWindowOperator()");
		}		
	}

    /** {@inheritDoc} 
     * @throws OptimizationException */
    public final int getCardinality(final CardinalityType card, 
    		final Site node, final DAF daf) throws OptimizationException {
    	int cardinality;
		if (winOp.getTimeScope()) {
			//evaluation covered by from 
			final int fromEval = (int) Math.ceil((-this.winOp.getFrom() + 1000.0) / 
					this.winOp.getTickInterval());
			if (fromEval == 0) {
				String msg = "QoS Query mismatch results in empty timeWindow. From: " 
					+ this.winOp.getFrom() + " does not cover any evaluations.";
				logger.warn(msg);
				throw new OptimizationException(msg);
			}
			
			//less evaluation excluded by to
			final int toEval = (int) Math.ceil(-this.winOp.getTo() / 
					this.winOp.getTickInterval()); 
			if (toEval >= fromEval) {
				String msg = "QoS Query mismatch results in empty timeWindow. From: " 
					+ this.winOp.getFrom() + " to " + this.winOp.getTo() + " evalRate " +
					this.winOp.getTickInterval()
					+ "All evaluation covered by the From are excluded by the to.";
				logger.warn(msg);
				throw new OptimizationException(msg);
			}
			final int evaluationsPerWindow = fromEval - toEval;
			final int inCard = getInputCardinality(card, node, daf, 0);
			if (inCard == 0) {
				String msg="Time window reported a zero input cardinality.";
				logger.warn(msg);
				throw new OptimizationException(msg);
			}
			cardinality = inCard * evaluationsPerWindow;			
			if (cardinality == 0) {
				String msg = "QoS Query mismatch results in empty timeWindow. From: " 
					+ this.winOp.getFrom() + " to " + this.winOp.getTo() + 
					" evalRate " + this.winOp.getTickInterval()
					+ " fromEval " + fromEval + " toEval " + toEval
					+ " inCard " + inCard;
				logger.warn(msg);
				throw new OptimizationException(msg);
			}
		} else {
			cardinality = -this.winOp.getFrom() + this.winOp.getTo() + 1;			
			if (cardinality == 0) {
				String msg = "QoS Query mismatch results in empty timeWindow. From: " 
					+ this.winOp.getFrom() + " to " + this.winOp.getTo() + " evalRate " + 
					this.winOp.getTickInterval();
				logger.warn(msg);
				throw new OptimizationException(msg);
			}	
		}
		return cardinality;
    }

	@Override
	/** {@inheritDoc} */    
	public final int getDataMemoryCost(final Site node, final DAF daf) 
	throws SchemaMetadataException, TypeMappingException, OptimizationException {
		return super.defaultGetDataMemoryCost(node, daf);
	}

	@Override
	/** {@inheritDoc} */
	public final int getOutputQueueCardinality(
			final Site node, final DAF daf) throws OptimizationException {
    	int size;
     	if (winOp.getTimeScope()) {
        	final int evaluationsInQueue;
    		if ((winOp.getTimeSlide() > 1) || 
    			(winOp.getTimeSlide() != winOp.getTickInterval())) {
    			evaluationsInQueue = (int) Math.ceil((1000 - winOp.getFrom() 
    					+ winOp.getTimeSlide()) / winOp.getTickInterval());
    		} else {
    			evaluationsInQueue 
    				= (int) Math.ceil((1000 - winOp.getFrom()) / winOp.getTickInterval());
    		}
    		//Size is always the max.
    		size = getInputCardinality(CardinalityType.MAX, 
    				node, daf, 0)
    		    * evaluationsInQueue;
     	} else {
     		size = -winOp.getFrom();
     	} 
     	if (winOp.getRowSlide() > 1) {
     		size = size + winOp.getRowSlide() - 1;
     	}
     	//logger.finest("queue =" +size);
     	return size;
    }

    /** {@inheritDoc} */
    public final ArrayList<Integer> getSourceSites() {
    	return super.defaultGetSourceSites();
    }
    
	/** {@inheritDoc} 
	 * @throws OptimizationException */
    public final double getTimeCost(final CardinalityType card, 
    		final Site node, final DAF daf) throws OptimizationException {
		final int tuples 
			= this.getInputCardinality(card, node, daf, 0);
		// call method 
		double cost = getOverheadTimeCost();
		// save tuple in outQueue	
		cost = cost + costParams.getCopyTuple() * tuples;
		if (this.winOp.getTimeScope()) {
			//add cost of checking each possible tuple
			cost = cost + costParams.getCheckTuple()
			   * this.getOutputQueueCardinality(node, daf);
			//add cost of recording input tuple arrival time
			cost = cost + costParams.getSetAValue() * tuples;
		}
		// set window eval time in each output tuple	
		cost = cost	+ costParams.getSetAValue()
			* this.getCardinality(card, node, daf);
		return cost;	
    }

    //delegate
	public int getFrom() {
		return this.winOp.getFrom();
	}

    //delegate
	public int getTo() {
		return this.winOp.getTo();
	}

    //delegate
	public boolean isTimeScope() {
		return this.winOp.isTimeScope();
	}

	//delegate
	public int getTimeSlide() {
		return this.winOp.getTimeSlide();
	}
	
	//delegate
	public int getRowSlide() {
		return this.winOp.getRowSlide();
	}

}
