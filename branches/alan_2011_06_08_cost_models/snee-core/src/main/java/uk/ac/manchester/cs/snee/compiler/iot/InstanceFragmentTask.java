/****************************************************************************\ 
*                                                                            *
*  SNEE (Sensor NEtwork Engine)                                              *
*  http://code.google.com/p/snee                                             *
*  Release 1.0, 24 May 2009, under New BSD License.                          *
*                                                                            *
*  Copyright (c) 2009, University of Manchester                              *
*  All rights reserved.                                                      *
*                                                                            *
*  Redistribution and use in source and binary forms, with or without        *
*  modification, are permitted provided that the following conditions are    *
*  met: Redistributions of source code must retain the above copyright       *
*  notice, this list of conditions and the following disclaimer.             *
*  Redistributions in binary form must reproduce the above copyright notice, *
*  this list of conditions and the following disclaimer in the documentation *
*  and/or other materials provided with the distribution.                    *
*  Neither the name of the University of Manchester nor the names of its     *
*  contributors may be used to endorse or promote products derived from this *
*  software without specific prior written permission.                       *
*                                                                            *
*  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS   *
*  IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, *
*  THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR    *
*  PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR          *
*  CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,     *
*  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,       *
*  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR        *
*  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF    *
*  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING      *
*  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS        *
*  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.              *
*                                                                            *
\****************************************************************************/
package uk.ac.manchester.cs.snee.compiler.iot;

import java.util.logging.Logger;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.CommunicationTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.Task;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
/**
 * This class represents a tasks which involves executing a query plan fragment.
 * @author Ixent Galpin
 *
 */
public class InstanceFragmentTask extends Task {

    /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -2135654280036828205L;

    private static final Logger logger = Logger.getLogger(InstanceFragmentTask.class.getName());

    //The query plan fragment
    private InstanceFragment fragment;

    //The node this task is executing on
    private Site site;

    private DAF daf;

    //If a leaf fragment, the occurrence (i.e., evaluation) of the node.
    private long occurrence;

    //private long alpha;
    
    private long beta;

    /**
     * Creates a instance of a FragmentTask.
     * @param startTime
     * @param fragment
     * @param site
     * @param l
     * @throws OptimizationException 
     */
    public InstanceFragmentTask(final long startTime, final InstanceFragment fragment,
	    final Site site, final long l, final long alpha, final long beta,
	    final DAF daf, CostParameters costParams) throws OptimizationException {
	super(startTime, costParams);
	this.fragment = fragment;
	this.daf = daf;
	//this.alpha = alpha;
	this.beta = beta;
	this.site = site;
	this.endTime = startTime + this.getTimeCost(daf);
	this.occurrence = l;
    }

    /**
     * Returns the duration of this task.
     * @throws OptimizationException 
     */
    @Override
	public final long getTimeCost(final DAF daf) throws OptimizationException {

	logger.finest("fragment = " + this.fragment.getID());
	logger.finest("timeCost = "
		+ this.fragment.getTimeCost(site, this.daf, costParams));
	logger.finest("bufferingFactor = " + this.beta);
	double calcTime;
	if (this.fragment.isLeaf()) {
	    calcTime = this.fragment.getTimeCost(site, this.daf, costParams);
	} else {
	    calcTime = this.fragment.getTimeCost(site, this.daf, costParams)
		    * this.beta;
	}
	if (this.fragment.isDeliverFragment()) {
		calcTime += CommunicationTask.getTimeCostOverhead(this.costParams);
	}
//IG: Not sure if I should have removed this...
//	assert (calcTime <= fragment.getTimeExpression(
//			CardinalityType.PHYSICAL_MAX, site, daf, true)
//			.evaluate(this.alpha, this.beta));
	
	//System.out.println (this.fragment.getID());
	//System.out.println ("calcTime:" + calcTime + " Beta = " + this.bufferingFactor);
	//System.out.println (fragment.getTimeExpression(CardinalityType.PHYSICAL_MAX, site, daf, true));
	logger.finest("calc Time = " + calcTime);

	if (calcTime > costParams.getMinimumTimerInterval()) {
	    return (long) Math.ceil(calcTime);
	}
	return (long) costParams.getMinimumTimerInterval();
    }

    public final InstanceFragment getFragment() {
    	
    		return fragment;
		
    }

    public final long getOccurrence() {
	return this.occurrence;
    }

    @Override
	public final Site getSite() {
	return this.site;
    }

    @Override
	public final String getSiteID() {
	return this.site.getID();
    }

    /**
     * String representation
     */
    @Override
	public final String toString() {
	return "F" + this.fragment.getID() + "n" + this.site.getID() + "_"
		+ this.occurrence;
    }

}
