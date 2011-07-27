package uk.ac.manchester.cs.snee.compiler.allocator;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.params.qos.QoSExpectations;
import uk.ac.manchester.cs.snee.compiler.queryplan.DLAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.LAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.metadata.source.StreamingSourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;
import uk.ac.manchester.cs.snee.operators.logical.AcquireOperator;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;
import uk.ac.manchester.cs.snee.operators.logical.InputOperator;
import uk.ac.manchester.cs.snee.operators.logical.JoinOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.ReceiveOperator;
import uk.ac.manchester.cs.snee.operators.logical.ScanOperator;
import uk.ac.manchester.cs.snee.operators.logical.UnionOperator;
import uk.ac.manchester.cs.snee.operators.logical.ValveOperator;
import uk.ac.manchester.cs.snee.operators.logical.WindowOperator;

public class SourceAllocator {

	Logger logger = Logger.getLogger(this.getClass().getName());

	public SourceAllocator () 
	{
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SourceAllocator()");
		}

		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SourceAllocator()");
		}
	}

	public DLAF allocateSources (LAF laf, QoSExpectations qos) 
	throws SourceAllocatorException, SourceMetadataException
	{
		if (logger.isDebugEnabled())
			logger.debug("ENTER allocateSources() laf="+laf.getID());
		DLAF dlaf = new DLAF(laf, laf.getQueryName());
		Set<SourceMetadataAbstract> sources = retrieveSources(laf);
		//currently one source is supported
		validateSources(sources);		
		dlaf.setSource(sources);
		setSourceRates(laf, qos);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN allocateSources()");
		}
		return dlaf;
	}

	/**
	 * This method is used to set the source rates for each of the operators 
	 * in the laf. 
	 * 
	 * If the operator is an Acquire operator, the source rate is equivalent 
	 * to the acquisition interval defined in the QOS parameters.
	 * 
	 * If the operator is any other Input Operator, then the source metadata 
	 * contains the information about its arrival rate.
	 * 
	 * If the operator is a Join Operator, the a precedence of Stream>Sensor Network>Relation
	 * is considered for the left and right operands and for a combination of the
	 * above mentioned sources, the higher precedence one is taken and the source rate 
	 * of that operand is set as the source rate
	 * 
	 * If the operator is a Union Operator, the sum of the source rates of both
	 * operands taking part in the union operation is considered as the source rate
	 * 
	 * If the operator is a Window Operator,
	 * If it is a timescope based window operator, then windowOp.getTimeSlide() * 1000 
	 * If it is a tuple based window operator, then (windowSize / (source rate of child)) 
	 * 
	 * For any other operator, the source rate is set as the source rate of its
	 * child operator
	 * 
	 * @param laf
	 * @param qos
	 * @throws SourceMetadataException
	 */
	private void setSourceRates(LAF laf, QoSExpectations qos)
	throws SourceMetadataException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER setSourceRates()");
		}
		Iterator<LogicalOperator> opIter = laf.operatorIterator(
				TraversalOrder.POST_ORDER);
		while (opIter.hasNext()) {
			LogicalOperator op = opIter.next();
			if (op instanceof AcquireOperator) {
				AcquireOperator acquireOp = (AcquireOperator) op;
				acquireOp.setSourceRate(qos.getMinAcquisitionInterval());
			} else if (op instanceof ReceiveOperator) {
				ReceiveOperator receiveOperator = (ReceiveOperator) op;
				StreamingSourceMetadataAbstract inputSource = 
					((StreamingSourceMetadataAbstract) receiveOperator.getSource());
				double rate = inputSource.getRate(receiveOperator.getExtentName());
				op.setSourceRate(rate);
			} else if (op instanceof ScanOperator) {
				//Do nothing!!!
			} else if (op instanceof JoinOperator) {
				JoinOperator joinOperator = (JoinOperator) op;
				joinOperator.setSourceRate(joinOperator.getSourceRate(
						op.getInput(0), op.getInput(1)));
			} else if (op instanceof UnionOperator) {
				UnionOperator unionOperator = (UnionOperator) op;
				unionOperator.setSourceRate(unionOperator.getSourceRate(
						op.getInput(0), op.getInput(1)));
			} else if (op instanceof WindowOperator) {				
				WindowOperator windowOperator = (WindowOperator) op;
				if (windowOperator.isTimeScope()) {
					//This is correct
					windowOperator
					.setSourceRate(windowOperator.getTimeSlide() * 1000);
				} else {
					//TODO Needs to correct this where case where 
					//the range of the window is specified in time, 
					//but the slide is in the number of tuples
					windowOperator.setSourceRate(windowOperator
							.getCardinality(CardinalityType.MAX)/op.getInput(0).getSourceRate());
				}
				//windowOperator.setSourceRate(op.getInput(0).getSourceRate());
			} else if (op instanceof ValveOperator) {
				//TODO Need to find a way to correctly set this rate
				//in case of pull based valve operator and in case of 
				//push based valve operator
				ValveOperator valveOperator = (ValveOperator) op;
				valveOperator.setSourceRate(op.getInput(0).getSourceRate());
			} else {
				op.setSourceRate(op.getInput(0).getSourceRate());
			}
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN setSourceRates()");
		}
	}

	private Set<SourceMetadataAbstract> retrieveSources(LAF laf)
	throws SourceAllocatorException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER retrieveSources() for " + laf.getID());
		}
		Set<SourceMetadataAbstract> sources = 
			new HashSet<SourceMetadataAbstract>();
		Iterator<LogicalOperator> opIter =
			laf.operatorIterator(TraversalOrder.PRE_ORDER);
		while (opIter.hasNext()) {
			LogicalOperator op = opIter.next();
			if (op instanceof AcquireOperator) {
				AcquireOperator acquireOp = (AcquireOperator) op;
				SourceMetadataAbstract acqSource = acquireOp.getSource();
				sources.add(acqSource);
			} else if (op instanceof ReceiveOperator) {
				ReceiveOperator receiveOp = (ReceiveOperator) op;
				SourceMetadataAbstract recSource = receiveOp.getSource();
				sources.add(recSource);
			} else if (op instanceof ScanOperator) {
				ScanOperator scanOp = (ScanOperator) op;
				SourceMetadataAbstract scanSource = scanOp.getSource();
				sources.add(scanSource);
			}
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN retrieveSources() #sources=" + 
					sources.size());
		}
		return sources;
	}

	private SourceMetadataAbstract validateSources(
			Set<SourceMetadataAbstract> sources)
	throws SourceAllocatorException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER validateSources() #sources=" +
					sources.size());
		}
		if (sources.size()>1) {
			/* 
			 * Permit more than one pull-stream or push-stream
			 * source to be used 
			 */
			for (SourceMetadataAbstract source : sources) {
				SourceType sourceType = source.getSourceType();
				switch (sourceType) {
				case PULL_STREAM_SERVICE:
				case PUSH_STREAM_SERVICE:
				case UDP_SOURCE:
					break;
				case RELATIONAL:
				case WSDAIR:
					break;
				case SENSOR_NETWORK:
					String msg = "More than " +
					"one source in LAF; queries with more " +
					"than one source are currently not " +
					"supported by source allocator for " +
					"queries over a sensor network.";
					logger.warn(msg);
					throw new SourceAllocatorException(msg);
				default:
					msg = "Unsupported data source type " + sourceType;
					logger.warn(msg);
					throw new SourceAllocatorException(msg);
				}
			}
		} else if (sources.size()==0) {
			String msg = "No sources found in LAF.";
			logger.warn(msg);
			throw new SourceAllocatorException(msg);	
		}
		for (SourceMetadataAbstract source : sources) {
			if (logger.isTraceEnabled()) {
				logger.trace("RETURN validateSources()");
			}
			return source;	
		}
		return null;
	}

}
