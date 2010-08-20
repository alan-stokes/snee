package uk.ac.manchester.cs.snee.compiler.allocator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceMetadata;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceType;
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
		if (logger.isDebugEnabled())
			logger.debug("ENTER SourceAllocator()");

		if (logger.isDebugEnabled())
			logger.debug("RETURN SourceAllocator()");
	}
	
	public DLAF allocateSources (LAF laf) 
	throws SourceAllocatorException
	{
		if (logger.isDebugEnabled())
			logger.debug("ENTER allocateSources() laf="+laf.getID());
		DLAF dlaf = new DLAF(laf, laf.getQueryName());
		Iterator<LogicalOperator> opIter =
			laf.operatorIterator(TraversalOrder.PRE_ORDER);
		List<SourceMetadata> sources = new ArrayList<SourceMetadata>();
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
				throw new SourceAllocatorException("Scan operator found in LAF;"+
						" these are not currently supported by source allocator ");
			}
		}
		if (sources.size()>1) {
			throw new SourceAllocatorException("More than one source in "+
			" LAF; queries with more than one source are "+
			" currently not supported by source allocator.");				
		} else if (sources.size()==0) {
			throw new SourceAllocatorException("No sources found in LAF.");	
		}
		SourceMetadata onlySource = sources.get(0);
		dlaf.setSource(onlySource);
		if (logger.isDebugEnabled())
			logger.debug("RETURN allocateSources()");
		return dlaf;
	}
	
}
