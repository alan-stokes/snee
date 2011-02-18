package uk.ac.manchester.cs.snee.compiler.allocator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.queryplan.DLAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.LAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
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
		if (logger.isDebugEnabled())
			logger.debug("ENTER allocateSources() laf="+laf.getID());
		DLAF dlaf = new DLAF(laf, laf.getQueryName());
		HashSet<SourceMetadataAbstract> sources = retrieveSources(laf);
		//currently one source is supported
		SourceMetadataAbstract onlySource = validateSources(sources);
		dlaf.setSource(onlySource);
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN allocateSources()");
		}
		return dlaf;
	}

	private HashSet<SourceMetadataAbstract> retrieveSources(LAF laf)
			throws SourceAllocatorException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER retrieveSources() for " + laf.getID());
		}
		HashSet<SourceMetadataAbstract> sources = new HashSet<SourceMetadataAbstract>();
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
				SourceMetadataAbstract recSources = receiveOp.getSource();
				sources.add(recSources);
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

	private SourceMetadataAbstract validateSources(HashSet<SourceMetadataAbstract> sources)
			throws SourceAllocatorException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER validateSources() #sources=" +
					sources.size());
		}
		if (sources.size()>1) {
			String msg = "More than " +
			"one source in LAF; queries with more " +
			"than one source are currently not " +
			"supported by source allocator.";
			logger.warn(msg);
			throw new SourceAllocatorException(msg);				
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
