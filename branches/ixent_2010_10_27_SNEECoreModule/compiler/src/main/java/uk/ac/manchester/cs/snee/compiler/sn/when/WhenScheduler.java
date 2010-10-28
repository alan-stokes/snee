package uk.ac.manchester.cs.snee.compiler.sn.when;

import java.util.Iterator;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.metadata.CostParameters;
import uk.ac.manchester.cs.snee.compiler.metadata.Metadata;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.compiler.params.qos.QoSExpectations;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaException;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaLengthException;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.ExchangePart;
import uk.ac.manchester.cs.snee.compiler.queryplan.Fragment;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;

public class WhenScheduler {

	/**
	 * Logger for this class.
	 */
	private Logger logger = 
		Logger.getLogger(WhenScheduler.class.getName());

	private boolean decreaseBetaForValidAlpha = true;
	
	private CostParameters costParams;
	
	/**
	 * Constructor for Sensor Network When-Scheduling Decision Maker.
	 */
	public WhenScheduler(boolean decreaseBetaForValidAlpha, Metadata m) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER WhenScheduler()");
		this.decreaseBetaForValidAlpha=decreaseBetaForValidAlpha;
		this.costParams = m.getCostParameters();
		if (logger.isDebugEnabled());
			logger.debug("RETURN WhenScheduler()");
	}
	
    /**
     * Carry out <i>When Scheduling</i> step of query optimization.
     * 
     * @param daf	The query plan DAF
     * @param qos	The quality-of-service expectations (in particular, the 
     * 				tuple delivery time)
     * @param queryName The name of the query being optimized
     * @throws OptimizationException An optimization exception
     * @return The generated agenda
     * @throws WhenSchedulerException 
     */
    public Agenda doWhenScheduling(final DAF daf, 
    		final QoSExpectations qos, final String queryName)
	    throws OptimizationException, WhenSchedulerException {

	try {
		if (logger.isDebugEnabled())
			logger.debug("ENTER doWhenScheduling() with " + daf.getID());
//		if (Settings.DISPLAY_COST_EXPRESSIONS) {
//			//Even if not used in this version of When Scheduling.  
//	    	CostExpressions costExpressions = CostExpressions.costExpressionFactory(daf);
//	    	costExpressions.display();
//		}
	    logger.trace("Computing maximum possible buffering factor based on "
	    		+ " memory");
	    long maxBFactorSoFar = computeMaximumBufferingFactorBasedOnMemory(
		    daf, qos);
	    logger.trace("Max possible buffering factor according to memory " 
	    		+ " available on nodes in sensor network: "	+ maxBFactorSoFar);
	    
		// optimizer should compute best buffering factor based on memory, 
		// maxBFactor and delivery time
	    if (qos.getBufferingFactor() == -1) { 
			logger.trace("qos.getBufferingFactor()==-1");			
			if ((qos.getMaxBufferingFactor() < maxBFactorSoFar)
				&& (qos.getMaxBufferingFactor() != -1)) {
			    maxBFactorSoFar = qos.getMaxBufferingFactor();
			}
	
			logger.trace("Reduce buffering factor to meet delivery time QoS, "
					+ "if given");
			maxBFactorSoFar = computeMaximumBufferingFactorToMeetDeliveryTime(
				daf, qos, maxBFactorSoFar);
			
			//We need this until we can support overlapping agendas
			logger.trace("Reduce buffering factor if agenda overlap will occur");
			maxBFactorSoFar = computeMaximumBufferingFactorWithoutAgendaOverlap(
				daf, qos, maxBFactorSoFar);
			
	    } else {
		//use the buffering factor specified in the Qos 
	    //(overrides Max buffering factor)

			if (qos.getBufferingFactor() 
				> computeMaximumBufferingFactorToMeetDeliveryTime(
				daf, qos, maxBFactorSoFar)) {
			    throw new OptimizationException(
				    "Buffering factor " + qos.getBufferingFactor() 
				    + " specified in QoS cannot meet the maximum delivery time "
					+ qos.getMaxDeliveryTime());
			}
			if (maxBFactorSoFar < qos.getBufferingFactor()) {
			    throw new OptimizationException(
				"Buffering factor " + qos.getBufferingFactor()
				+ " specified in QoS cannot be supported due to "
				+ "lack of memory");
			}
			long maxBfWithoutOverlap = computeMaximumBufferingFactorWithoutAgendaOverlap(daf,
					qos, maxBFactorSoFar);
			if (maxBfWithoutOverlap < qos.getMaxBufferingFactor()) {
			    throw new OptimizationException(
				"Buffering factor " + qos.getBufferingFactor()
				+ " specified in QoS would require an agenda overlap;"
				+ " these are currently not supported");
			}
			maxBFactorSoFar = qos.getMaxBufferingFactor();
	    }

	    try {
	    	final Agenda agenda = new Agenda(qos.getMaxAcquisitionInterval(), 
	    			maxBFactorSoFar, daf, costParams, queryName);
			if (logger.isDebugEnabled())
				logger.debug("RETURN doWhenScheduling()");
	    	return agenda;
	    } catch (Exception e) {
	    	logger.warn("When Scheduler exception", e);
	    	throw new WhenSchedulerException(e);
	    	
	    }
	} catch (final AgendaException e) {
		logger.warn(e);
	    throw new WhenSchedulerException(e.getMessage());
	} catch (SchemaMetadataException e) {
		logger.warn(e);
		throw new WhenSchedulerException(e.getMessage());
	} catch (TypeMappingException e) {
		logger.warn(e);
		throw new WhenSchedulerException(e.getMessage());	}
    }

    /**
     * Given a plan, computes the memory used by a single evaluation of the query plan fragments placed on 
     * each sensor network node, and returns the highest possible buffering factor, i.e., number of 
     * evaluations that can take place of the leaf fragments before onward transmission takes place.
     * 
     * @param plan
     * @return the maximum possible buffering factor for this plan
     * @throws OptimizationException
     * @throws TypeMappingException 
     * @throws SchemaMetadataException 
     */
    private long computeMaximumBufferingFactorBasedOnMemory(
    final DAF daf, final QoSExpectations qos) throws OptimizationException, SchemaMetadataException, TypeMappingException {
		if (logger.isTraceEnabled())
			logger.debug("ENTER computeMaximumBufferingFactorBasedOnMemory()");

		long maxBufferingFactor = 100;
		
		//traverse each sensor network node in the routing tree
		final Iterator<Site> nodeIter = daf.getRT().siteIterator(TraversalOrder.PRE_ORDER);
		while (nodeIter.hasNext()) {
	
		    final Site currentNode = nodeIter.next();
	
		    logger.trace("Computing maximum local buffering factor for node "
			    + currentNode.getID());
	
		    int totalFragmentDataMemoryCost = 0;
		    final Iterator<Fragment> fragments = 
		    	currentNode.getFragments().iterator();
		    while (fragments.hasNext()) {
				final Fragment fragment = fragments.next();
				logger.trace("fragment = " + fragment);
				totalFragmentDataMemoryCost += fragment.getDataMemoryCost(
					currentNode, daf);
		    }
		    logger.trace("Data Memory Cost of all fragments is "
			    + totalFragmentDataMemoryCost);
	
		    int totalExchangeComponentsDataMemoryCost = 0;
	
		    final Iterator<ExchangePart> comps = currentNode
			    .getExchangeComponents().iterator();
		    while (comps.hasNext()) {
				final ExchangePart comp = comps.next();
				logger.trace("exchange component =" + comp);
				totalExchangeComponentsDataMemoryCost += comp
					.getDataMemoryCost(currentNode, daf);
		    }
	
		    //compute the maximum possible buffering factor on this node, based on the memory available and
		    //the memory cost
		    long localMaxBufferingFactor;
		    if (totalExchangeComponentsDataMemoryCost > 0) {
				localMaxBufferingFactor = (currentNode.getRAM() - totalFragmentDataMemoryCost)
					/ totalExchangeComponentsDataMemoryCost;
		    } else {
		    	localMaxBufferingFactor = Long.MAX_VALUE;
		    }
		    logger.trace("Memory maximum buffering factor is "
			    + localMaxBufferingFactor);
	
		    //A buffering factor of 0 indicates this query plan is not possible, throw an error
		    if (localMaxBufferingFactor <= 0) {
			throw new OptimizationException(
				"Query plan not feasible due to lack of memory at node "
					+ currentNode.getID() + "; At least " + (totalExchangeComponentsDataMemoryCost 
					+ totalFragmentDataMemoryCost) + " is required and " + currentNode.getRAM()
					+ " is available.");
		    }
	
		    //If the local maximum buffering factor is smaller than the global one, reduce the global
		    //buffering factor accordingly
		    if (localMaxBufferingFactor < maxBufferingFactor) {
			maxBufferingFactor = localMaxBufferingFactor;
			logger
				.trace("Global buffering factor reduced because of memory at node"
					+ currentNode.getID() + "to " + maxBufferingFactor);
		    }
		}
		if (logger.isTraceEnabled())
			logger.debug("RETURN computeMaximumBufferingFactorBasedOnMemory()");
		return maxBufferingFactor;
    }

    /**
     *	Given a plan and the maximum buffering factor possible based on memory available on the sensor network nodes,
     *  checks if this buffering factor will result in an overlapping agenda.  If so, and the ini file setting 
     *  decrease_bfactor_to_avoid_agenda_overlap=true, it will iteratively decrease the buffering factor until the agenda
     *  does not overlap.  If the buffering factor=1 and the agenda still overlaps, or the ini file setting 
     *  decrease_bfactor_to_avoid_agenda_overlap=false, throw an exception.
     * @throws WhenSchedulerException 
     * @throws TypeMappingException 
     * @throws SchemaMetadataException 
     *  
     */
    private long computeMaximumBufferingFactorWithoutAgendaOverlap(
	    final DAF daf, final QoSExpectations qos, final long maxBFactorSoFar)
	    throws OptimizationException, AgendaException, WhenSchedulerException, SchemaMetadataException, TypeMappingException {

	Agenda agenda = null;
	long lowerBeta = 1;
	long upperBeta = maxBFactorSoFar;
	long beta;
	
	final long alpha_bms = (int) Agenda.msToBms_RoundUp((qos.getMaxAcquisitionInterval()));
	do {
		if (this.decreaseBetaForValidAlpha) {
			beta = (lowerBeta+upperBeta)/2;
		} else {
			beta = upperBeta;
		}
	    
		try {
	    	agenda = new Agenda(qos.getMaxAcquisitionInterval(), beta, daf, costParams, "");
		    logger.trace("Agenda constructed successfully length="
				    + agenda.getLength_bms(Agenda.INCLUDE_SLEEP) + " met target length="
				    + alpha_bms * beta + " with beta="
				    + beta);
		    lowerBeta = beta;
		    
		} catch (AgendaLengthException e) {
			
			if (this.decreaseBetaForValidAlpha) {
				String msg="Set decrease_bfactor_to_avoid_agenda_overlap=true in the "+
				 "ini file to avoid need for overlap, as overlapping agendas are not" +
				 "yet supported.";
				logger.warn(msg);
				throw new WhenSchedulerException(msg);
			}
			
	    	upperBeta = beta;
	    	
	    	logger.trace("Max Buffering factor reduced to " + beta);
	    	if (beta == 1) {
	    		String msg ="Agenda requires overlap even with buffering factor=1.  "+
	    		"Overlapping agendas are currently not supported; try increasing the "+
	    		"acquisition interval in the QoS file."; 
	    		logger.warn(msg);
	    		throw new WhenSchedulerException(msg);
	    	}
	    	
	    	continue;
	    } 
	    
	} while (lowerBeta+1<upperBeta);
	//(agenda.getLength_bms(Agenda.INCLUDE_SLEEP) > (alpha_bms * currMaxBeta))
	//	&& (currMaxBeta > 0));

	return lowerBeta;
    }

    /**
     * 
     * Given a plan and the maximum buffering factor possible based on memory available on the sensor network nodes,
     * checks if this buffering factor will meet the max delivery time QoS requirements (if any are given).  If they are not
     * met, the buffering factor is reduced until they are, or an exception is thrown if they can't be met.
     * @throws TypeMappingException 
     * @throws SchemaMetadataException 
     * 
     */
    private long computeMaximumBufferingFactorToMeetDeliveryTime(
	    final DAF daf, final QoSExpectations qos, final long maxBFactorSoFar)
	    throws OptimizationException, AgendaException, SchemaMetadataException, TypeMappingException {

    	//TODO: make this binary search like computeMaximumBufferingFactorWithoutAgendaOverlap
    	
	Agenda agenda;
	long currentMaxBuffFactor = maxBFactorSoFar;
	logger.trace("maxBFactorSoFar ="+maxBFactorSoFar);
	final long bmsDeliveryTime = Agenda.msToBms_RoundUp(qos.getMaxDeliveryTime());
	if (bmsDeliveryTime > 0) {
	    do {
	    	try {
	    		agenda = new Agenda(qos.getMaxAcquisitionInterval(), 
	    				currentMaxBuffFactor, daf, costParams, "");
	    		//CB I don't think this is neseccary as new Agenda throw an error.
	    		//CB :Left in for safety
	    		if (agenda.getLength_bms(Agenda.IGNORE_SLEEP) > bmsDeliveryTime) {
	    		    currentMaxBuffFactor--;
	    		    logger.trace("Agenda time ="
	    			    + agenda.getLength_bms(Agenda.IGNORE_SLEEP)
	    			    + " > " + bmsDeliveryTime);
	    		    logger.trace("Max Buffering factor reduced to "
	    			    + currentMaxBuffFactor);
	    		} else {
	    			break;
	    		}
	    		
	    	} catch (AgendaLengthException e) {
	    		if (currentMaxBuffFactor > 1) {
	    		    currentMaxBuffFactor--;	    			
	    		} else {
	    			e.printStackTrace();
	    			throw new OptimizationException(
	    			"It is not possible to meet the delivery" 
	    					+ " time QoS requirements "
	    					+ "even with a buffering factor of 1.");
	    		}
	    	}
	    } while (true);

	}

	return currentMaxBuffFactor;
    }
}	
	
