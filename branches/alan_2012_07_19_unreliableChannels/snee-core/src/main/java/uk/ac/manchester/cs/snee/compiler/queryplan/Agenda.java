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

package uk.ac.manchester.cs.snee.compiler.queryplan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.AgendaException;
import uk.ac.manchester.cs.snee.compiler.AgendaLengthException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;


/**
 * Class responsible for recording the schedules of nodes in a sensor network.
 * @author 	Ixent Galpin
 *
 */
public class Agenda extends SNEEAlgebraicForm{

    /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -4391275568035871005L;

    /**
     * Logger for this class.
     */
    private final static Logger logger = Logger.getLogger(Agenda.class.getName());
	
    /**
     * The acquisition interval, in binary ms.  The finest granularity that 
     * may be given is 1/32ms (=32 bms)
     */
    private long alpha;

    /**
     * The buffering factor.
     */
    private long beta;

	boolean allowDiscontinuousSensing = true;
    
    /**
     * The task schedule for all the sites. 
     */
    //the list of tasks for each sensor network node
    private HashMap<Site, ArrayList<Task>> tasks = 
    	new HashMap<Site, ArrayList<Task>>();

    //the list of all start times (used to display schedule)
    private ArrayList<Long> startTimes = new ArrayList<Long>();

    public static final boolean IGNORE_SLEEP = true;

    public static final boolean INCLUDE_SLEEP = false;

    private DAF daf;

    private String name;

    private CostParameters costParams;
    
    /**
     * Counter to assign unique id to different candidates.
     */
    private static int candidateCount = 0;
    
    /**
     * Start of nonLeaf part of the agenda.
     * This will be where the last leaf jumps to at the end of the query duration 
     */
    private long nonLeafStart = Integer.MAX_VALUE;

    /**agenda containing instance version*/
    private AgendaIOT agendaIOT = null;
    
    public Agenda(final long acquisitionInterval, final long bfactor,
	final DAF daf, CostParameters costParams, final String queryName,
	boolean allowDiscontinuousSensing) 
    throws AgendaException, AgendaLengthException, OptimizationException, 
    SchemaMetadataException, TypeMappingException {
    	super(queryName);
		this.alpha = msToBms_RoundUp(acquisitionInterval);
		this.beta = bfactor;
		this.daf = daf;
		this.allowDiscontinuousSensing=allowDiscontinuousSensing;
		tasks.clear();
		
		if (!queryName.equals("")) {
			this.name = generateID(queryName);
		}
		this.costParams=costParams;
		
		logger.trace("Scheduling leaf fragments alpha=" + this.alpha + " bms beta=" + this.beta);
		scheduleLeafFragments();
		logger.trace("Scheduling the non-leaf fragments");
		scheduleNonLeafFragments();
		logger.trace("Scheduling network management section");
		logger.trace("add radioOn and radioOff tasks");
		insertRadioOnOffTasks();
		logger.trace("Scheduled final sleep task");
		scheduleFinalSleepTask();
		
		long length = this.getLength_bms(Agenda.INCLUDE_SLEEP);
		logger.trace("Agenda alpha=" + this.alpha + " beta=" + this.beta + " alpha*beta = " + this.alpha * this.beta + " length="+length);
		
		if (length > (this.alpha * this.beta) && (!allowDiscontinuousSensing)) {
 			//display the invalid agenda, for debugging purposes
// 			this.display(QueryCompiler.queryPlanOutputDir,
//					this.getName()+"-invalid");
 			String msg = "Invalid agenda: alpha*beta = " + 
 				bmsToMs(this.alpha * this.beta) + "ms, length = " + 
 				bmsToMs(length) + "ms, alpha = "+bmsToMs(alpha) + "ms, beta = "
 				+ bmsToMs(beta);
 			logger.warn(msg);
			throw new AgendaLengthException(msg);
			
		}
    }

    /*
    private void scheduleNetworkManagementSection() throws AgendaException {
		final long start = this.getLength_bms(Agenda.INCLUDE_SLEEP);
		final long end = start + costParams.getManagementSectionDuration();
		
		if (start < 0) {
		    throw new AgendaException("Start time < 0");
		}
		final Iterator<Site> siteIter = this.daf.getRT()
			.siteIterator(TraversalOrder.POST_ORDER);
		while (siteIter.hasNext()) {
		    final Site site = siteIter.next();
		    this.assertConsistentStartTime(start, site);
		    ManagementTask mt = new ManagementTask(start, end, site, this.costParams);
		    this.addTask(mt, site);
		}
	}

    private void scheduleEndNetworkManagementSection() throws AgendaException {
		final long start = this.getLength_bms(Agenda.INCLUDE_SLEEP);
		final long end = start + costParams.getEndManagementSectionDuration();
		
		if (start < 0) {
		    throw new AgendaException("Start time < 0");
		}
		final Iterator<Site> siteIter = this.daf.getRT()
			.siteIterator(TraversalOrder.POST_ORDER);
		while (siteIter.hasNext()) {
		    final Site site = siteIter.next();
		    this.assertConsistentStartTime(start, site);
		    EndManagementTask mt = new EndManagementTask(start, end, site, this.costParams);
		    this.addTask(mt, site);
		}
	}*/
    
	/**
     * Resets the counter; use prior to compiling the next query.
     */
    public static void resetCandidateCounter() {
    	candidateCount = 0;
    }
    
	/**
	 * Generates a systematic name for this query plan strucuture, of the form
	 * {query-name}-{structure-type}-{counter}.
	 * @param queryName	The name of the query
	 * @return the generated name for the query plan structure
	 */
    protected String generateID(final String queryName) {
    	candidateCount++;
    	return queryName + "-Agenda-" + candidateCount;
    }
    
	 /** {@inheritDoc} */
	public String getDescendantsString() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getDescendantsString()"); 
		if (logger.isDebugEnabled())
			logger.debug("RETURN getDescendantsString()"); 
		return this.getID()+"-"+this.daf.getDescendantsString();
	}
    
    
    public DAF getDAF() {
    	return this.daf;
    }
    
    public String getName() {
    	return this.name;
    }
    
    /**
     * Returns acquisition interval, in bms
     * @return the acquisition interval, in bms
     */
    public final long getAcquisitionInterval_bms() {
	return this.alpha;
    }

    /**
     * Returns acquisition interval, in ms
     * @return the acquisition interval, in ms
     */
	public long getAcquisitionInterval_ms() {
		return Agenda.bmsToMs(this.alpha);
	}

    /**
     * Returns the buffering factor for this agenda
     * @return the buffering factor
     */
    public final long getBufferingFactor() {
	return this.beta;
    }

    /**
     * Returns the processing time for this agenda, in bms
     * @return the processing time, in bms
     */
	public long getProcessingTime_bms() {
		return this.getDeliveryTime_bms() - this.alpha * (this.beta -1);
	}
	
    /**
     * Returns the processing time for this agenda, in ms
     * @return the processing time, in ms
     */
	public long getProcessingTime_ms() {
		return Agenda.bmsToMs(this.getDeliveryTime_bms() - this.alpha * (this.beta -1));
	}
	
    /**
     * Returns the delivery time for this agenda, in bms
     * @return the delivery time, in bms
     */
	public long getDeliveryTime_bms() {
	
		return this.getLength_bms(true);
	}

    /**
     * Returns the delivery time for this agenda, in ms
     * @return the delivery time, in ms
     */
	public long getDeliveryTime_ms() {
		
		return Agenda.bmsToMs(this.getLength_bms(true));
	}
	
    public static int bmsToMs(final long startTime) {
	return Utils.divideAndRoundUp(startTime * 1000, 1024);
    }

    public static int msToBms_RoundUp(final long ms) {
	final long temp = ms * 1024;
	return Utils.divideAndRoundUp(temp, 1000);
    }

    /**
     * Returns the list of start times, in order, for each task on every node
     * @return
     */
    public final ArrayList<Long> getStartTimes() {
	return this.startTimes;
    }

    public final long getNonLeafStart() {
	return this.nonLeafStart;
    }

    public final void setNonLeafStart(final long nonLeafStart2) {
	this.nonLeafStart = nonLeafStart2;
    }

    /**
     * Given a time and a sensor network node, raises an exception if the given time is before the node's
     * next available time.
     * @param startTime
     * @param site
     * @throws AgendaException
     */
    private void assertConsistentStartTime(final double startTime,
	    final Site site) throws AgendaException {

	if (startTime < this.getNextAvailableTime(site, INCLUDE_SLEEP)) {
	    throw new AgendaException("Attempt to Schedule task on node "
		    + site.getID() + " at time " + startTime
		    + " which is before the nodes next available time ("
		    + this.getNextAvailableTime(site, INCLUDE_SLEEP) + ")");
	}

    }

    /**
     * Given a node, returns true if it has been assigned any tasks.
     * @param node
     * @return
     */
    public final boolean hasTasks(final Site node) {
	return (this.tasks.get(node) != null);
    }

    //adds a start time to the list of start times
    private void addStartTime(final long startTime) {

	boolean found = false;
	int i = 0;

	final Iterator<Long> startTimesIter = this.startTimes.iterator();
	while (startTimesIter.hasNext() && !found) {
	    final Long s = startTimesIter.next();
	    if (s.intValue() == startTime) {
		found = true;
	    }

	    if (s.intValue() > startTime) {
		break;
	    }

	    i++;
	}

	if (found == false) {
	    this.startTimes.add(i, new Long(startTime));
	    found = true;
	}

    }


//    //adds a task to a nodes schedule, performing the necessary checks
    private void addTask(final Task t, final Site site) {
	
		//add node to schedule if necessary
		if (this.tasks.get(site) == null) {
		    this.tasks.put(site, new ArrayList<Task>());
		}
	
		//add task to the node schedule
		final ArrayList<Task> taskList = this.tasks.get(site);
		taskList.add(t);
	
		//add to list of start times
		this.addStartTime(t.getStartTime());
    }

    public final Task getTask(final long startTime, final Site site) {

	if (!this.tasks.keySet().contains(site)) {
	    return null;
	}
	final Iterator<Task> taskListIter = this.tasks.get(site).iterator();
	while (taskListIter.hasNext()) {
	    final Task t = taskListIter.next();
	    if (t.getStartTime() == startTime) {
		return t;
	    }
	}
	return null;
    }

    public final FragmentTask getFirstFragmentTask(final Site site,
	    final Fragment frag) {
	final Iterator<Task> taskIter = this.taskIterator(site);
	while (taskIter.hasNext()) {
	    final Task t = taskIter.next();
	    if (t instanceof FragmentTask) {
		final FragmentTask fragTask = (FragmentTask) t;
		if (fragTask.getFragment() == frag) {
		    return fragTask;
		}
	    }
	}
	return null;
    }

    public final CommunicationTask getFirstCommTask(final Site site, final int mode) {
	final Iterator<Task> taskIter = this.taskIterator(site);
	while (taskIter.hasNext()) {
	    final Task t = taskIter.next();
	    if (t instanceof CommunicationTask) {
		final CommunicationTask commTask = (CommunicationTask) t;
		if ((commTask.getSourceNode() == site)
			&& (commTask.getMode() == mode)) {
		    return commTask;
		}
	    }
	}
	return null;
    }
    
    

    /**
     * Adds a fragment task at the specified startTime on the specified node
     * @param startTime		the time the task should start
     * @param frag			the query plan fragment to be executed
     * @param node			the sensor network node
     * @param occurrence	the nth evaluation of a leaf fragment
     * @throws AgendaException
     * @throws OptimizationException 
     */
    public final void addFragmentTask(final long startTime, final Fragment frag,
	    final Site node, final long occurrence) throws AgendaException, OptimizationException {

	this.assertConsistentStartTime(startTime, node);
	final FragmentTask fragTask = new FragmentTask(startTime, frag, node,
		occurrence, this.alpha, this.beta, this.daf, costParams);
	this.addTask(fragTask, node);

	logger.trace("Scheduled Fragment " + frag.getID() + " on node "
		+ node.getID() + " at time " + startTime);

    }

    public final void addFragmentTask(final int startTime, final Fragment frag,
	    final Site node) throws AgendaException, OptimizationException {

	this.addFragmentTask(startTime, frag, node, 1);
    }

    /**
     * Adds a fragment task at the next available time on the specified node
     * @param fragment		the query plan fragment to be executed
     * @param node			the sensor network node 
     * @throws AgendaException	
     * @throws OptimizationException 
     */
    public final void addFragmentTask(final Fragment fragment, final Site node)
	    throws AgendaException, OptimizationException {

	final long startTime = this.getNextAvailableTime(node, INCLUDE_SLEEP);
	logger.trace("start time =" + startTime);
	this.addFragmentTask(startTime, fragment, node, 1);
    }

    public final void addFragmentTask(final Fragment fragment, final Site node,
	    final long ocurrence) throws AgendaException, OptimizationException {
	final long startTime = this.getNextAvailableTime(node, INCLUDE_SLEEP);
	this.addFragmentTask(startTime, fragment, node, ocurrence);
    }

    /**
     * Appends a communication task between two nodes in the sensor network
     * @param sourceNode				the node transmitting data
     * @param destNode					the node receiving data
     * @param exchangeComponents		the data being sent
     * @throws TypeMappingException 
     * @throws SchemaMetadataException 
     * @throws OptimizationException 
     */ 
    
    public final void appendCommunicationTask(final Site sourceNode,
	    final Site destNode,
	    final HashSet<ExchangePart> exchangeComponents)
	    throws AgendaException, OptimizationException, SchemaMetadataException, TypeMappingException {

    final long startTime = this.getLength_bms(true);

	final CommunicationTask commTaskTx = new CommunicationTask(startTime,
		sourceNode, destNode, exchangeComponents,
		CommunicationTask.TRANSMIT, this.alpha, this.beta, this.daf, costParams);
	final CommunicationTask commTaskRx = new CommunicationTask(startTime,
		sourceNode, destNode, exchangeComponents,
		CommunicationTask.RECEIVE, this.alpha, this.beta, this.daf, costParams);

	this.addTask(commTaskTx, sourceNode);
	this.addTask(commTaskRx, destNode);

	logger.trace("Scheduled Communication task from node "
		+ sourceNode.getID() + " to node " + destNode.getID()
		+ " at time " + startTime + "(size: "
		+ exchangeComponents.size() + " exchange components )");
    }

    public final void addSleepTask(final long sleepStart, final long sleepEnd,
	    final boolean lastInAgenda) throws AgendaException {
	if (sleepStart < 0) {
	    throw new AgendaException("Start time < 0");
	}
	final Iterator<Site> siteIter = this.daf.getRT()
		.siteIterator(TraversalOrder.POST_ORDER);
	while (siteIter.hasNext()) {
	    final Site site = siteIter.next();
	    //this.assertConsistentStartTime(sleepStart, site);
	    final SleepTask t = new SleepTask(sleepStart, sleepEnd, site,
		    lastInAgenda, costParams);
	    this.addTask(t, site);
	}
    }

    /**
     * @param node		node in the sensor network
     * @return 			the time the node has completed all the tasks it has been allocated so far
     */
    public final long getNextAvailableTime(final Site node, final boolean ignoreSleep) {
	if (this.tasks.get(node) == null) {
	    return 0;
	} else {
	    //find the last task and return its end time
	    final ArrayList<Task> taskList = this.tasks.get(node);
	    int taskNum = taskList.size() - 1;
	    Task last = taskList.get(taskNum);
	    if (ignoreSleep) {
		while (last.isSleepTask()) {
		    taskNum = taskNum - 1;
		    if (taskNum < 0) {
			return 0;
		    } else {
			last = taskList.get(taskNum);
		    }
		}
	    }
	    long nextAvailableTime = last.getEndTime();
	    //			if (nextAvailableTime % 10 != 0) { //TODO: unhardcode this
	    //				nextAvailableTime = (nextAvailableTime / 10 + 1)* 10;
	    //			}

	    return nextAvailableTime;
	}

    }

    /**
     * @return if ignoreLastSleep is true, returns the time that the last task on all nodes ends.  
     * Otherwise returns the length of the agenda. 
     * 
     */
    public final long getLength_bms(final boolean ignoreLastSleep) {
	long tmp = 0;

	final Iterator<Site> nodeIter = this.tasks.keySet().iterator();
	while (nodeIter.hasNext()) {
	    final Site n = nodeIter.next();
	    if (this.getNextAvailableTime(n, ignoreLastSleep) > tmp) {
		tmp = this.getNextAvailableTime(n, ignoreLastSleep);
	    }
	}

	return tmp;
    }


    /**
     * Returns an iterator which gives the all the sites in the schedule
     * @return
     */
    public final Iterator<Site> siteIterator() {
	return this.tasks.keySet().iterator();
    }

    /**
     * Returns an iterator which gives the tasks, in order, for the given node
     * @param node
     * @return
     */
    public final Iterator<Task> taskIterator(final Site node) {
	return this.tasks.get(node).iterator();
    }

    //TODO: review thie taskschedule data structure in view of this inefficient access algorithm
    /**
     * Returns an iterator which gives the tasks at a given start time
     * @param node
     * @return
     */
    public final Iterator<Task> taskIterator(final long startTime) {
	final ArrayList<Task> results = new ArrayList<Task>();

	final Iterator<Site> nodeIter = this.tasks.keySet().iterator();
	while (nodeIter.hasNext()) {
	    final Site node = nodeIter.next();

	    final Iterator<Task> taskIter = this.tasks.get(node).iterator();
	    while (taskIter.hasNext()) {
		final Task t = taskIter.next();

		if (t.getStartTime() == startTime) {
		    results.add(t);
		}
	    }
	}

	return results.iterator();
    }

    /**
     * Returns an iterator which gives all the start times
     * @return
     */
    public final Iterator<Long> startTimeIterator() {
	return this.startTimes.iterator();
    }

    public final long getFragmentTimerOffsetVal(final String fragID,
	    final String siteID) {
	final Site site = (Site) this.daf.getRT().getSite(siteID);
	final Iterator<Task> taskIter = this.taskIterator(site);
	while (taskIter.hasNext()) {
	    final Task task = taskIter.next();
	    if (task instanceof FragmentTask) {
		final FragmentTask fragTask = (FragmentTask) task;
		final Fragment frag = fragTask.getFragment();
		if (frag.getID().equals(fragID)
			&& (fragTask.getOccurrence() == 1)) {

		    return fragTask.getStartTime();
		}
	    }
	}

	return -1;
    }

    public final long getFragmentTimerRepeatVal(final String fragID) {
	final Fragment frag = this.daf.getFragment(fragID);

	if (frag.isLeaf()) {
	    return this.getAcquisitionInterval_bms();
	}

	return this.getAcquisitionInterval_bms() * this.beta;
    }

    public final long getCommTaskTimerOffsetVal(final String sourceID,
	    final String destID, final int mode) {
	Site site;

	if (mode == CommunicationTask.RECEIVE) {
	    site = (Site) this.daf.getRT().getSite(destID);
	} else {
	    site = (Site) this.daf.getRT().getSite(sourceID);
	}

	final Iterator<Task> taskIter = this.taskIterator(site);
	while (taskIter.hasNext()) {
	    final Task task = taskIter.next();
	    if (task instanceof CommunicationTask) {
		final CommunicationTask commTask = (CommunicationTask) task;
		if (commTask.getSourceID().equals(sourceID)
			&& commTask.getDestID().equals(destID)) {
		    return commTask.getStartTime();
		}
	    }
	}

	return -1;
    }

    public final long getCommTaskTimerRepeatVal() {
	return this.alpha * this.beta;
    }

    /**
     * Schedule the leaf fragments in a query plan.  These are executed bFactor times at the
     * acquisition frequency specified by the user  
     * @param plan
     * @param bFactor
     * @param agenda
     * @throws AgendaException
     * @throws OptimizationException 
     */
    protected void scheduleLeafFragments()
	    throws AgendaException, OptimizationException {

	//First schedule the leaf fragments, according to the buffering factor specified 
	//Note: a separate task needs to be scheduled for each execution of a leaf fragment
	for (long n = 0; n < this.beta; n++) {
	    final long startTime = this.alpha * n;

	    //For each leaf fragment
	    HashSet<Fragment> leafFrags = daf.getLeafFragments();
	    final Iterator<Fragment> fragIter = leafFrags.iterator();
	    while (fragIter.hasNext()) {
		final Fragment frag = fragIter.next();

		//For each site the fragment is executing on 
		final Iterator<Site> nodeIter = frag.getSites().iterator();
		while (nodeIter.hasNext()) {
		    final Site node = nodeIter.next();

		    try {

			this.addFragmentTask(startTime, frag, node, (n + 1));

		    } catch (final AgendaException e) {

			final long taskDuration = new FragmentTask(startTime,
				frag, node, (n + 1), this.alpha, this.beta, this.daf, costParams)
				.getTimeCost(this.daf);

			//If time to run task before the next acquisition time:
			if (this.getNextAvailableTime(node,
				Agenda.INCLUDE_SLEEP)
				+ taskDuration <= startTime
				+ this.alpha) {
			    //TODO: change this to time synchronisation QoS
			    this.addFragmentTask(frag, node, (n + 1));
			} else {
			    throw new AgendaException(
				    "Aquisition interval is smaller than duration of acquisition fragments on node "
					    + node.getID());
			}
		    }
		}
	    }
	    //Go active uses the disactivated sleep to represent all nodes do nothing
	    //if ((Settings.NESC_DO_SNOOZE) && ((n+1) != bFactor)) {
	    if ((n + 1) != this.beta) {
		final long sleepStart = this
			.getLength_bms(Agenda.INCLUDE_SLEEP);
		final long sleepEnd = (this.alpha * (n + 1));
		this.addSleepTask(sleepStart, sleepEnd, false);
	    }
	}
    }

    /**
     * Schedule the non-leaf fragments.  Then are executed as soon as possible after the leaf fragments
     * have finished executing.
     * @param plan
     * @param factor
     * @param agenda
     * @throws AgendaException
     * @throws OptimizationException 
     * @throws TypeMappingException 
     * @throws SchemaMetadataException 
     */
    protected void scheduleNonLeafFragments()
	    throws AgendaException, OptimizationException, SchemaMetadataException, TypeMappingException {

	long nonLeafStart = Long.MAX_VALUE;

	final Iterator<Site> siteIter = daf.getRT().siteIterator(TraversalOrder.POST_ORDER);
	while (siteIter.hasNext()) {
	    final Site currentNode = siteIter.next();

	    final long startTime = this.getNextAvailableTime(currentNode,
		    Agenda.IGNORE_SLEEP);
	    if (startTime < nonLeafStart) {
		nonLeafStart = startTime;
	    }

	    //Schedule all fragment which have been allocated to execute on this node,
	    //ensuring the precedence conditions are met
	    final Iterator<Fragment> fragIter = daf
		    .fragmentIterator(TraversalOrder.POST_ORDER);
	    while (fragIter.hasNext()) {
		final Fragment frag = fragIter.next();
		if (currentNode.hasFragmentAllocated(frag) && (!frag.isLeaf())) {
		    this.addFragmentTask(frag, currentNode);
		}
	    }

	    //Then Schedule any onward transmissions
	    if (currentNode.getOutputs().length > 0) {
		final HashSet<ExchangePart> tuplesToSend = new HashSet<ExchangePart>();
		final Iterator<ExchangePart> exchCompIter = currentNode
			.getExchangeComponents().iterator();
		while (exchCompIter.hasNext()) {
		    final ExchangePart exchComp = exchCompIter.next();
		    if ((exchComp.getComponentType() == ExchangePartType.PRODUCER)
			    || (exchComp.getComponentType() == ExchangePartType.RELAY)) {
			tuplesToSend.add(exchComp);
		    }
		}

		if (tuplesToSend.size() > 0) {
		    this.appendCommunicationTask(currentNode, (Site) currentNode
			    .getOutput(0), tuplesToSend);
		}
	    }
	}
    }

	protected void scheduleFinalSleepTask() throws AgendaException {
		final long sleepStart = this.getLength_bms(Agenda.INCLUDE_SLEEP);
		
		//A sleep task of at least 10 ms needs to be added here, to turn the radio off
		final long sleepEnd = Math.max(this.alpha * this.beta, 
				new Float(sleepStart + costParams.getTurnOffRadio()).longValue());
		logger.trace("Sleep task scheduled from "+sleepStart+" to "+sleepEnd);
		this.addSleepTask(sleepStart, sleepEnd, true);
		this.setNonLeafStart(nonLeafStart);
	}

    /**
     * Checks if site has been scheduled a task (including sleep tasks) at a given time.
     * @param site
     * @param startTime
     * @return
     */
    public final boolean isFree(final Site site, final long startTime, final long endTime) {
    	final Iterator<Task> taskListIter = this.tasks.get(site).iterator();
    	while (taskListIter.hasNext()) {
    	    final Task t = taskListIter.next();
    	    long taskStartTime = t.getStartTime();
    	    long taskEndTime = t.getEndTime();
    	    if (taskStartTime <= startTime && startTime < taskEndTime) {
    	    	return false;
    	    }
    	    if (taskStartTime <= endTime-1 && endTime-1 < taskEndTime) {
    	    	return false;
    	    }
    	}
    	return true;
    }
    

    public final boolean hasRadioOn(final Site site, final long time) {
    	boolean radioOn = false;
    	
    	final Iterator<Task> taskListIter = this.tasks.get(site).iterator();
    	while (taskListIter.hasNext()) {
    	    final Task t = taskListIter.next();
    	    long taskStartTime = t.getStartTime();

    	    if (taskStartTime > time) {
    	    	return radioOn;
    	    }
    	    
    	    if (t instanceof RadioOnTask) {
    	    	radioOn = true;
    	    }
    	    if (t instanceof RadioOffTask) {
    	    	radioOn = false;
    	    }
    	}
    	return radioOn;
    }
    
    /**
     * Shifts all the tasks for all the sites starting on or after a given time by offset.
     * @param time
     * @param offset
     */
    public final void shift(final long time, final long offset) {
    	
    	//update the list of startTimes
    	for (int i=0; i < this.startTimes.size(); i++) {
    		if (this.startTimes.get(i)>=time) {
    			long oldStartTime = this.startTimes.get(i);
    			long newStartTime = oldStartTime+offset;
    			this.startTimes.set(i, newStartTime);    			
    		}
    	}
    	
    	//update the list of tasks at each site
    	Iterator<Site> siteIter = this.getDAF().getRT().siteIterator(TraversalOrder.POST_ORDER);
    	while (siteIter.hasNext()) {
    		Site s = siteIter.next();
    		
    		ArrayList<Task> siteTaskList = this.tasks.get(s);
    		for (int i=0; i < siteTaskList.size(); i++) {
    			Task t = siteTaskList.get(i);
    			if (t.startTime>=time) {
	    			t.startTime += offset;
			    	t.endTime += offset;
    			}
    		}
    	}
    }

	
	private void insertRadioOnOffTasks() {
		Iterator<Site> siteIter;
		siteIter = this.daf.getRT().siteIterator(TraversalOrder.POST_ORDER);
		long radioOnTimeCost = (long) costParams.getTurnOnRadio();
		long radioOffTimeCost = (long) costParams.getTurnOffRadio();
		
		while (siteIter.hasNext()) {
			Site site = siteIter.next();
			ArrayList<Task> siteTasks = this.tasks.get(site);
			for (int i = 0; i<siteTasks.size(); i++) {
				Task t = siteTasks.get(i);
				long startTime = t.startTime;
				if ((t instanceof CommunicationTask || t.isDeliverTask()) && !this.hasRadioOn(site, startTime)) {
					if (this.isFree(site, startTime - radioOnTimeCost, startTime)) {
						this.insertTask(startTime - radioOnTimeCost, site, 
								new RadioOnTask(startTime - radioOnTimeCost, site, costParams));
					} else {
						this.shift(startTime, radioOnTimeCost);
						this.insertTask(startTime, site, new RadioOnTask(startTime, site, costParams));
					}
				}
				if ((t instanceof CommunicationTask || t.isDeliverTask())&& (radioNextNeededTime(siteTasks, i+1)==-1 
						|| radioNextNeededTime(siteTasks, i+1) > t.endTime + (radioOnTimeCost*1.5))) {
					
					if (this.isFree(site, t.endTime, t.endTime+radioOffTimeCost)) {
						this.insertTask(t.endTime, site, new RadioOffTask(t.endTime, site, costParams));
					} else {
						this.shift(t.endTime, radioOffTimeCost);
						this.insertTask(t.endTime, site, new RadioOffTask(t.endTime, site, costParams));
					}
				}
			}
		}
	}
	
    public final void insertTask(final long time, final Site site, final Task t) {
    	boolean found = false;
    	
    	for (int i=0; i < this.startTimes.size(); i++) {
    		if (time == this.startTimes.get(i)) {
    			found = true;
    			break;
    		}
    		if (time < this.startTimes.get(i)) {
    			this.startTimes.add(i, new Long(time));
    			found = true;
    			break;
    		}
    	}

    	//append to the end
    	if (!found) {
    		this.startTimes.add(new Long(time));
    	}
    	
    	found = false;
    	ArrayList<Task> taskList = this.tasks.get(site);
    	for (int i=0; i < taskList.size(); i++) {
    		if (time < taskList.get(i).getStartTime()) {
    			taskList.add(i, t);
    			found = true;
    			break;
    		}
    	}
    
    	//append to end
    	if (!found) {
    		taskList.add(t);
    	}
    }	
	
    /**
     * Given the list of tasks for a particular site, and a index to start searching, returns
     * the startTime of the next task which requires use of the radio.
     * If there is none, returns -1.
     * @param siteTasks
     * @param startTaskNum
     * @return
     */
    public static long radioNextNeededTime(ArrayList<Task> siteTasks, int startTaskNum) {
    	
    	//startTaskNum is beyond the last task; radio never needed again 
    	if (startTaskNum >= siteTasks.size()) {
    		return -1;
    	}
    	
    	for (int i=startTaskNum; i<siteTasks.size(); i++) {
    		Task t = siteTasks.get(i);
    		
    		//check if communication task or deliver task
    		if (t instanceof CommunicationTask || t.isDeliverTask()) {
    			return t.startTime;
    		}    		
    	}
    	
    	return -1;
    }
	
	public CostParameters getCostParameters() {
		return this.costParams;
	}    
	
	public void removeNodeFromAgenda(int siteID)
	{
	  Site toBeRemove = daf.getRT().getSite(siteID);
	  tasks.remove(toBeRemove);
	}

	/**
	 * takes a set of children sites, and orders them by start transmission times. 
	 * (assumes 1 transmission task per site. (tree structure)
	 * @param children
	 */
  public void orderNodesByTransmissionTasks(ArrayList<Node> children)
  {
    Iterator<Node> childIterator = children.iterator();
    ArrayList<CommunicationTask> transmissionTasks = new ArrayList<CommunicationTask>();
    while(childIterator.hasNext())
    {
      Node child = childIterator.next();
      CommunicationTask transmissionTask = getTransmissionTask(child);
      transmissionTasks.add(transmissionTask);
    }
    Collections.sort(transmissionTasks);
    children.clear();
    Iterator<CommunicationTask> transmissionTaskIterator = transmissionTasks.iterator();
    while(transmissionTaskIterator.hasNext())
    {
      CommunicationTask task = transmissionTaskIterator.next();
      Site node = task.getSourceNode();
      children.add(node);
    }
  }

  /**
   * gets the transmission task for the child node
   * @param child node to get task for.
   * @return first communication task found during task iterator which is a transmission task.
   */
  public CommunicationTask getTransmissionTask(Node child)
  {
    final Iterator<Task> taskIter = this.taskIterator((Site) child);
    while (taskIter.hasNext()) 
    {
      final Task t = taskIter.next();
      if (t instanceof CommunicationTask) 
      {
        final CommunicationTask commTask = (CommunicationTask) t;
        if ((commTask.getSourceNode() == child)
           && (commTask.getMode() == CommunicationTask.TRANSMIT)) 
        {
           return commTask;
        }
      }
    }
    return null;
  }

  /**
   * gets last communication task temporally which is a receive.
   * @param nodePrime node to find last receive task on.
   * @return communication task
   */
  public CommunicationTask getLastCommunicationTask(Node nodePrime)
  {
    final Iterator<Site> siteIterator = this.siteIterator();
    boolean inQEP = false;
    //check site is within QEP, if not, thenr eturn null. (as no tasks).
    //needs to be done here, as task iterator will fail with a node with no tasks.
    while(siteIterator.hasNext() && !inQEP)
    {
      Site currentSite = siteIterator.next();
      if(currentSite.getID().equals(nodePrime.getID()))
      {
        inQEP = true;
      }
    }
    
    if(!inQEP)
      return null;
      
    final Iterator<Task> taskIter = this.taskIterator((Site) nodePrime);
    
    CommunicationTask currentLastReceiveTask = null;
    while (taskIter.hasNext()) 
    {
      final Task t = taskIter.next();
      if (t instanceof CommunicationTask) 
      {
        final CommunicationTask commTask = (CommunicationTask) t;
        if ((commTask.getSourceNode() == nodePrime)
           && (commTask.getMode() == CommunicationTask.RECEIVE)) 
        {
          currentLastReceiveTask = commTask;
        }
      }
    }
    return currentLastReceiveTask;
  }
  
  /**
   * returns the communication task between child and parent
   * @param child transmitter of communication task
   * @param parent receiver of communication task
   * @return communication task.
   */
  public CommunicationTask getCommunicationTaskBetween(Node child, Node parent)
  {
    final Iterator<Task> taskIter = this.taskIterator((Site)child);
    while(taskIter.hasNext())
    {
      Task task = taskIter.next();
      if (task instanceof CommunicationTask) 
      {
        final CommunicationTask commTask = (CommunicationTask) task;
        if ((commTask.getSourceNode() == child) && 
            (commTask.getDestNode() == parent))
        {
          return commTask;
        }
      }
    }
    return null;
  }
  
  /**
   * removes the communication task from the tasks hash map.
   * @param taskToBeRemoved 
   */
  public int removeCommunicationTask(CommunicationTask taskToBeRemoved)
  {
    //remove tx
    ArrayList<Task>  tasksOfNode = tasks.remove(taskToBeRemoved.getSourceNode());
    Iterator<Task> taskIterator = tasksOfNode.iterator();
    boolean removedtx = false;
    int index = -1;
    while(taskIterator.hasNext() && !removedtx)
    {
      Task task = taskIterator.next();
      index ++;
      if(task instanceof CommunicationTask) 
      {
        final CommunicationTask commTask = (CommunicationTask) task;
        if ((commTask.getSourceNode() == taskToBeRemoved.getSourceNode()) && 
            (commTask.getDestNode() == taskToBeRemoved.getDestNode()))
        {
          taskIterator.remove();
          removedtx = true;
        }
      }
    }
    tasks.put(taskToBeRemoved.getSourceNode(), tasksOfNode);
    
    return index;
    
  }
  
  /**
   * Appends a communication task between two nodes in the sensor network at a given time
   * @param sourceNode        the node transmitting data
   * @param destNode          the node receiving data
   * @param time              The start time of this communication task
   * @param exchangeComponents    the data being sent
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */ 
  
  public final void appendCommunicationTask(final Site sourceNode,
    final Site destNode, final long time,
    final HashSet<ExchangePart> exchangeComponents, int childIndex)
    throws AgendaException, OptimizationException, SchemaMetadataException, TypeMappingException 
  {

    final long startTime = time;

    final CommunicationTask commTaskTx = new CommunicationTask(startTime,
    sourceNode, destNode, exchangeComponents,
    CommunicationTask.TRANSMIT, this.alpha, this.beta, this.daf, costParams);
    final CommunicationTask commTaskRx = new CommunicationTask(startTime,
    sourceNode, destNode, exchangeComponents,
    CommunicationTask.RECEIVE, this.alpha, this.beta, this.daf, costParams);
  
    this.addTask(commTaskTx, sourceNode, childIndex);
    this.addTask(commTaskRx, destNode);
  
    logger.trace("Scheduled Communication task from node "
    + sourceNode.getID() + " to node " + destNode.getID()
    + " at time " + startTime + "(size: "
    + exchangeComponents.size() + " exchange components )");
  }
  
  /**
   * adds a task at a specfic index in the arraylist
   * @param t the task to be added
   * @param site the site the task is being added to
   * @param index the index in the list.
   */
  private void addTask(final Task t, final Site site, final int index) 
  {
    //add node to schedule if necessary
    if (this.tasks.get(site) == null) 
    {
        this.tasks.put(site, new ArrayList<Task>());
    }
  
    //add task to the node schedule
    final ArrayList<Task> taskList = this.tasks.get(site);
    taskList.add(index, t);
  
    //add to list of start times
    this.addStartTime(t.getStartTime());
  }

  public void setAgendaIOT(AgendaIOT agendaIOT)
  {
    this.agendaIOT = agendaIOT;
  }

  public AgendaIOT getAgendaIOT()
  {
    return agendaIOT;
  }
}
