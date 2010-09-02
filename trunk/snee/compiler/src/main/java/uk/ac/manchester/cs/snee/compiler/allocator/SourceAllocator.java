package uk.ac.manchester.cs.snee.compiler.allocator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceMetadata;
import uk.ac.manchester.cs.snee.compiler.queryplan.DLAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.LAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.operators.logical.AcquireOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.ReceiveOperator;
import uk.ac.manchester.cs.snee.operators.logical.ScanOperator;

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

	public DLAF allocateSources (LAF laf) 
	throws SourceAllocatorException
	{
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER allocateSources() laf=" + 
					laf.getName());
		}
		DLAF dlaf = new DLAF(laf, laf.getQueryName());
		List<SourceMetadata> sources = retrieveSources(laf);
		validateSources(sources);
		dlaf.setSources(sources);
//		SourceType sourceType = onlySource.getSourceType();
//		dlaf.setSourceType(sourceType);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN allocateSources()");
		}
		return dlaf;
	}

	private List<SourceMetadata> retrieveSources(LAF laf)
			throws SourceAllocatorException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER retrieveSources() for " + laf.getName());
		}
		List<SourceMetadata> sources = new ArrayList<SourceMetadata>();
		Iterator<LogicalOperator> opIter =
			laf.operatorIterator(TraversalOrder.PRE_ORDER);
		while (opIter.hasNext()) {
			LogicalOperator op = opIter.next();
			if (op instanceof AcquireOperator) {
				AcquireOperator acquireOp = (AcquireOperator) op;
				List<SourceMetadata> acqSources = acquireOp.getSources();
				sources.addAll(acqSources);
			} else if (op instanceof ReceiveOperator) {
				ReceiveOperator receiveOp = (ReceiveOperator) op;
				List<SourceMetadata> recSources = receiveOp.getSources();
				sources.addAll(recSources);
			} else if (op instanceof ScanOperator) {
				String msg = "Scan operator found in LAF; " +
					"these are not currently supported by the " +
					"source allocator ";
				logger.warn(msg);
				throw new SourceAllocatorException(msg);
			}
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN retrieveSources() #sources=" + 
					sources.size());
		}

		return sources;
	}

	private void validateSources(List<SourceMetadata> sources)
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
			for (SourceMetadata source : sources) {
				switch (source.getSourceType()) {
				case PULL_STREAM_SERVICE:
				case PUSH_STREAM_SERVICE:
				case UDP_SOURCE:
					break;
				default:
					String msg = "More than " +
						"one source in LAF; queries with more " +
						"than one source are currently not " +
						"supported by source allocator.";
					logger.warn(msg);
					throw new SourceAllocatorException(msg);				
				}
			}
		} else if (sources.size()==0) {
			String msg = "No sources found in LAF.";
			logger.warn(msg);
			throw new SourceAllocatorException(msg);	
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN validateSources()");
		}
	}

}
