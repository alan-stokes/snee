package uk.ac.manchester.cs.snee.compiler.allocator;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.queryplan.DLAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.LAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;
import uk.ac.manchester.cs.snee.operators.logical.AcquireOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.ReceiveOperator;
import uk.ac.manchester.cs.snee.operators.logical.ScanOperator;

public class SourceAllocator {

	private static final Logger logger = Logger.getLogger(SourceAllocator.class.getName());

	public SourceAllocator () 
	{
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SourceAllocator()");
		}

		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SourceAllocator()");
		}
	}

	public DLAF allocateSources (LAF laf) 
	throws SourceAllocatorException
	{
		if (logger.isDebugEnabled())
			logger.debug("ENTER allocateSources() laf="+laf.getID());
		DLAF dlaf = new DLAF(laf, laf.getQueryName());
		Set<SourceMetadataAbstract> sources = retrieveSources(laf);
		//currently one source is supported
		validateSources(sources);
		dlaf.setSource(sources);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN allocateSources()");
		}
		return dlaf;
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
