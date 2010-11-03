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
package uk.ac.manchester.cs.snee.sncb.tos;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.CommunicationTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.Fragment;
import uk.ac.manchester.cs.snee.compiler.queryplan.FragmentTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.SleepTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.Task;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.sncb.TinyOSGenerator;


public class QueryPlanModuleComponent extends NesCComponent {

    SensorNetworkQueryPlan plan;

    Agenda agenda;
    
    Integer sink;

    NesCConfiguration tossimConfig;

    String targetName;

    private static String tosSiteAddress;

    private CostParameters costParams;
    
	private boolean controlRadioOff;

	private boolean enablePrintf;

	private boolean useStartUpProtocol;

	private boolean enableLeds;

	private boolean usePowerManagement;

	private boolean deliverLast;

	private boolean adjustRadioPower;
    
    public QueryPlanModuleComponent(final String name,
	    final NesCConfiguration config, final SensorNetworkQueryPlan plan,
	    final int sink, int tosVersion, boolean tossimFlag, 
	    String targetName, CostParameters costParams, boolean controlRadioOff,
	    boolean enablePrintf, boolean useStartUpProtocol, boolean enableLeds,
	    boolean usePowerManagement, boolean deliverLast, boolean adjustRadioPower) {
		super(config, tosVersion, tossimFlag);
		this.id = name;
		this.plan = plan;
		this.agenda = plan.getAgenda();
		this.sink = sink;
		this.costParams = costParams;

		this.controlRadioOff =controlRadioOff;
		this.enablePrintf = enablePrintf;
		this.useStartUpProtocol = useStartUpProtocol;
		this.enableLeds = enableLeds;
		this.usePowerManagement = usePowerManagement;
		this.deliverLast = deliverLast;
		this.adjustRadioPower = adjustRadioPower;
	
		if (tosVersion == 1) {
		    tosSiteAddress = "TOS_LOCAL_ADDRESS";
		} else {
		    tosSiteAddress = "TOS_NODE_ID";
		}
		
		this.targetName = targetName;
	
    }

    public String toString() {
	return this.getID();
    }

    public void setTossimConfig(final NesCConfiguration tossimConfig) {
	this.tossimConfig = tossimConfig;
    }

    private static boolean radioOn = false; 
    
    public void writeNesCFile(final String outputDir)
	    throws IOException, CodeGenerationException {

		final PrintWriter out = new PrintWriter(new BufferedWriter(
			new FileWriter(outputDir + this.getID() + ".nc")));
	
		//QueryPlan Module preamble
		final ArrayList<Long> startTimeList = agenda.getStartTimes();
		final StringBuffer firedTimerTaskBuff = new StringBuffer();
		final StringBuffer agendaCheckingBuff = new StringBuffer();
		final StringBuffer radioOnTaskBuff = new StringBuffer();
	
		if (tossimFlag && (this.tossimConfig != null)) {
		    doQueryPlanModulePreamble(this.tossimConfig,
			    tossimFlag, out, startTimeList);
		} else if (!tossimFlag) {
		    doQueryPlanModulePreamble(this.configuration,
			    tossimFlag, out, startTimeList);
		} else {
		    throw new CodeGenerationException(
			    "No tossim configuration specified for query plan module");
		}
	
		radioOn = false;
		//for each task start time in the agenda
		for (int i = 0; i < startTimeList.size(); i++) {
	
		    //Get values for lastTime, startTime  and nextDelta
		    boolean lastTask = false;
		    if (i == startTimeList.size() - 1) {
			lastTask = true;
		    }
		    final long startTime = startTimeList.get(i).intValue();
		    final long nextDelta = getNextDelta(startTimeList, i);
	
		    agendaCheckingBuff.append("\t\t\tif (agendaRow == " + i + ") // agendaTime = " + startTime + "\n");
		    agendaCheckingBuff.append("\t\t\t{\n");
	
		    //for each task starting at this time
		    boolean first = true;
		    final boolean txFirst = true;
		    boolean agendaRowContainsSleepTask = false;
		    final Iterator<Task> taskIter = agenda.taskIterator(startTime);
		    while (taskIter.hasNext()) {
			final Task task = taskIter.next();
			if ((this.configuration.getSiteID() == task.getSiteID())
				|| (tossimFlag == true)) {
	
			    //TOSSIM only and not applicable to SleepTasks, as they are scheduled on all nodes simultaneously
			    int indentation = 0;
			    if ((tossimFlag == true) && !(task instanceof SleepTask)) {
				if (first) {
				    agendaCheckingBuff.append("\t\t\t\tif ("
					    + tosSiteAddress + "==" + task.getSiteID()
					    + ")\n");
				} else {
				    agendaCheckingBuff.append("\t\t\t\telse if ("
					    + tosSiteAddress + "==" + task.getSiteID()
					    + ")\n");
				}
				agendaCheckingBuff.append("\t\t\t\t{\n");
				indentation = 1;
			    }
	
			    //task is a fragment task
			    if (task instanceof FragmentTask) {
				doInvokeFragmentTask(firedTimerTaskBuff,
					agendaCheckingBuff, lastTask, first, task,
					indentation, radioOnTaskBuff);
				first = false;
	
				//task is a communication task
			    } else if (task instanceof CommunicationTask) {
				final CommunicationTask commTask = (CommunicationTask) task;
				final int mode = commTask.getMode();
				if ((mode == CommunicationTask.RECEIVE)) {
				    this.doInvokeCommunicationTask(firedTimerTaskBuff,
					    agendaCheckingBuff, lastTask, first, task,
					    indentation, tossimFlag, radioOnTaskBuff);
				    first = false;
				} else if ((mode == CommunicationTask.TRANSMIT)) {
				    this.doInvokeCommunicationTask(firedTimerTaskBuff,
					    agendaCheckingBuff, lastTask, txFirst,
					    task, indentation, tossimFlag, radioOnTaskBuff);
				    first = false;
				}
	
				// task is a sleep task
				// only needs to be invoked once because all nodes sleep at the same time
			    } else if ((task instanceof SleepTask)
				    && (agendaRowContainsSleepTask == false)) {
				doInvokeSleepTask(agendaCheckingBuff, task);
				agendaRowContainsSleepTask = true;
			    }
	
			    if ((tossimFlag == true) && !(task instanceof SleepTask)) {
				agendaCheckingBuff.append("\t\t\t\t}\n");
			    }
			}//if ((this.configuration.getSiteID()
		    }//while (taskIter.hasNext())
	
		    //invoke an idle task
		    if ((!agendaRowContainsSleepTask) &&
		    	(tossimFlag || first)){
			invokeIdleTask(tossimFlag, agendaCheckingBuff, first);
		    }
		    
		    if (i < startTimeList.size()-1) {
			    agendaCheckingBuff.append("\t\t\t\tagendaRow = agendaRow + 1;\n");
		    } else {
		    	agendaCheckingBuff.append("\t\t\t\tagendaRow = 0;\n");
		    }
	
		    agendaCheckingBuff.append("\t\t\t\tnextDelta = " + nextDelta
			    + ";\n");
		    agendaCheckingBuff.append("\t\t\t\treturn;\n");
		    agendaCheckingBuff.append("\t\t\t}\n");
		}//for (int i = 0;
	
		//Now dump all the string buffers onto a file
	
		boolean usesRadio = configUsesRadio();
		doQueryPlanModuleBody(this.sink, out, startTimeList,
			firedTimerTaskBuff, agendaCheckingBuff, radioOnTaskBuff, usesRadio);
    }

    /**
     * Checks whether the radio is used by the configuration.
     * @return
     */
	private boolean configUsesRadio() {
		boolean usesRadio = false;
		
		Iterator<NesCComponent> compIter = this.configuration.componentIterator();
		while (compIter.hasNext()) {
			NesCComponent comp = compIter.next();
			if (comp.getID().equals(TinyOSGenerator.COMPONENT_RADIO)) {
				usesRadio = true;
			}
		}
		return usesRadio;
	}

    private void invokeIdleTask(final boolean tossimFlag,
	    final StringBuffer agendaCheckingBuff, final boolean first) {
	int ind = 0;
	if (tossimFlag == true) {
	    agendaCheckingBuff.append("\t\t\t\telse\n");
	    agendaCheckingBuff.append("\t\t\t\t{\n");
	    ind = 1;
	}
	if ((tossimFlag == true) || ((tossimFlag == false) && (first == true))) {
	    agendaCheckingBuff.append(Utils.indent(ind)
		    + "\t\t\t\t//idle task\n");
	}
	if (tosVersion == 1) {
	    agendaCheckingBuff
		    .append(Utils.indent(ind)
			    + "\t\t\t\tcall AgendaTimer.start(TIMER_ONE_SHOT, nextDelta);//QueryPalnModule.invokeIdleTask()\n");
	} else {
	    agendaCheckingBuff.append(Utils.indent(ind)
		    + "\t\t\t\tcall AgendaTimer.startOneShot(nextDelta);\n");
	}
	if (this.controlRadioOff && radioOn == true && tossimFlag == false) {
		agendaCheckingBuff.append(Utils.indent(ind+4) + "call CommControl.stop();\n");
		radioOn = false;
	}
	if (tossimFlag == true) {
	    agendaCheckingBuff.append("\t\t\t\t}\n");
	}
    }

    private void doQueryPlanModuleBody(final Integer sink,
	    final PrintWriter out, final ArrayList<Long> startTimeList,
	    final StringBuffer firedTimerTaskBuff,
	    final StringBuffer agendaCheckingBuff, 
	    final StringBuffer radioOnTaskBuff, 
	    final boolean usesRadio) {

	int firstDelta;
	firstDelta = startTimeList.get(1).intValue()
		- startTimeList.get(0).intValue();
	if (tosVersion == 1) {
	    doT1startupMethods(out, firstDelta, tossimFlag, sink);
	} else {
		doT2StartupMethods(out, firstDelta, usesRadio, sink.toString(), this.targetName);
	}

	out.println(firedTimerTaskBuff);

	out.println(radioOnTaskBuff);
	
	doAgendaChecking(out, agendaCheckingBuff);

	doAgendaTimerFired(out);

	out.println("}\n"); //end of queryPlanM
	out.close();
    }

    /**
     * Performs Tossim synchronization by incrementing a global variable.  Note that
     * this doesn't need to be done for TinyOS2 because we can control the time
	 * each mote boots by means of Tossim Python scripting
     * @param sink
     * @param out
     * @param firstDelta
     */
    private void doTossimSynchronization(
    		Integer sink, 
    		final PrintWriter out, 
    		final int firstDelta) {

    	out.println("\tevent result_t SyncTimer.fired()");
		out.println("\t{");
	    out.println("\t\tif(" + tosSiteAddress + "==" + String.valueOf(sink)+ ")");
	    out.println("\t\t{");
	    out.println("\t\t\tsysTime+=" + this.costParams.getSynchronizationError() + ";");
	    out.println("\t\t}");
	    out.println("\t\tif(sysTime==" + this.costParams.getTossimSynchronizationPeriodLength() + ")");
	    out.println("\t\t{");
		out.println("\t\t\tcall SyncTimer.stop();");
		out.println("\t\t\tpost initialize();");
		out.println("\t\t}\n"); //end of if(sysTime=="+Settings.NESC_SYNCHRONIZATION_PERIOD+")");
	
		out.println("\t\treturn SUCCESS;\n");
	
		out.println("\t}\n"); //end of sync time fired
    }

    private void doAgendaTimerFired(final PrintWriter out) {
	if (tosVersion == 1) {
	    out.println("\tevent result_t AgendaTimer.fired()");
	} else {
	    out.println("\tevent void AgendaTimer.fired()");
	}
	out.println("\t{\n");
	out.println("\t\tpost processAgendaItemsTask();\n");
	
	if (tosVersion == 1) {
	    out.println("\t\treturn SUCCESS;\n");
	}
	out.println("\t}\n\n");
    }

    private static void doAgendaChecking(final PrintWriter out,
	    final StringBuffer agendaCheckingBuff) {
	out.println("\ttask void processAgendaItemsTask()");
	out.println("\t{");

	out.println(agendaCheckingBuff);
	out.println("\t}\n\n");
    }

    
	private static void doInitialize(final PrintWriter out, final int firstDelta) {
		
		out.println("\ttask void initialize()");
		out.println("\t{");
		out.println("\t\tnextDelta = " + (firstDelta - 1) + ";");
		out.println("\t\tagendaRow = 0;");
		out.println("\t\tpost processAgendaItemsTask();");
		out.println("\t}\n");
	}
	
	private void doT2StartupProtocol(final PrintWriter out, String sinkID) {
		out.println("\tbool beaconSent = FALSE;");
		out.println("\tbool beaconReceived = FALSE;");
		out.println("\tmessage_t pkt;");
		out.println("\ttypedef nx_struct BeaconMessage { nx_bool beaconMessage; } BeaconMessage;\n\n");
		
		out.println("\ttask void doStartUpProtocol()");
		out.println("\t{");
		if (this.debugLeds) {
			out.println("\t\tcall Leds.set(LEDS_LED0 | LEDS_LED1 | LEDS_LED2);");
		}
		out.println("\t}\n\n");
		
		out.println("\ttask void sendBeaconTask()");
		out.println("\t{");
		out.println("\t\tBeaconMessage* bpkt = (BeaconMessage*)(call BeaconPacket.getPayload(&pkt, NULL));");
		out.println("\t\tcall BeaconAMSend.send(AM_BROADCAST_ADDR, &pkt, sizeof(BeaconMessage));");
		out.println("\t}\n\n");
		
		out.println("\tevent void BeaconAMSend.sendDone(message_t* msg, error_t error)");
		out.println("\t{");
		out.println("\tif ( &pkt == msg )");
		out.println("\t\t{");
		out.println("\t\t\tbeaconSent = TRUE;");
		out.println("\t\t\tpost initialize();");
		out.println("\t\t}");
		out.println("\t}\n\n");
		
		out.println("\tevent message_t* BeaconReceive.receive(message_t* msg, void* payload, uint8_t len)");
		out.println("\t{");
		out.println("\t\tif (! beaconReceived && ! beaconSent )");
		out.println("\t\t{");
		out.println("\t\t\tbeaconReceived = TRUE;");
		out.println("\t\t\tpost sendBeaconTask();");
		if (this.debugLeds) {
			out.println("\t\t\tcall Leds.set(0);");
		}
		out.println("\t\t}");
		out.println("\t\treturn msg;");
		out.println("\t}\n\n");
		
		out.println("\tevent message_t* SerialStartUp.receive( message_t* msg, void* payload, uint8_t len )");
		out.println("\t{");
		if (this.debugLeds) {
			out.println("\t\tcall Leds.set(0);");
		}
		out.println("\t\tpost sendBeaconTask();");
		out.println("\t\treturn msg;");
		out.println("\t}\n\n");
	}
	
	private void doT2StartupMethods(final PrintWriter out,
			final int firstDelta, boolean usesRadio, String sinkID, String targetName) {
		
    	out.println("\ttask void processAgendaItemsTask();\n");
	
    	doInitialize(out, firstDelta);
	
		out.println("\tevent void Boot.booted()");
		out.println("\t{");
		
		if (this.enablePrintf && targetName.equals("z1")) {
			out.println("\t\tprintfz1_init();");
		}
		
			
		if (usesRadio) {
			out.println("\t\tcall CommControl.start();");
		} else {
			out.println("\t\tpost initialize();");
		}			
		out.println("\t}\n");
		
		if (usesRadio) {
			if (this.useStartUpProtocol) {
				doT2StartupProtocol(out, sinkID);
			}
			
			out.println("\tevent void CommControl.startDone(error_t err)");
			out.println("\t{");
			out.println("\t\tif (err == SUCCESS) {");
			if (this.useStartUpProtocol) {
				out.println("\t\t\tcall SerialControl.start();");
			} else {
				out.println("\t\t\tpost initialize();");				
			}
			out.println("\t\t} else {");
			out.println("\t\t\tcall CommControl.start();");			
			out.println("\t\t}");
			out.println("\t}\n\n");
	
			out.println("\tevent void CommControl.stopDone(error_t err)");
			out.println("\t{");
			out.println("\t\t//Do nothing");
			out.println("\t}\n\n");

			if (this.useStartUpProtocol) {
				out.println("\tevent void SerialControl.startDone(error_t err)");
				out.println("\t{");
				out.println("\t\tif (err == SUCCESS) {");
				out.println("\t\t\tpost doStartUpProtocol();");		
				out.println("\t\t} else {");
				out.println("\t\t\tcall SerialControl.start();");			
				out.println("\t\t}");
				out.println("\t}\n");			
				
				out.println("\tevent void SerialControl.stopDone(error_t err)");
				out.println("\t{");
				out.println("\t\t//Do nothing");
				out.println("\t}\n\n");
			}
		}
    }


    private void doT1startupMethods(final PrintWriter out, int firstDelta, boolean tossimFlag, int sink) {
	out.println("\tcommand result_t StdControl.init()");
	out.println("\t{");
	out.println("\t\tcall " + TinyOSGenerator.INTERFACE_TIMER
		+ TinyOSGenerator.INTERFACE_STDCONTROL + ".init();");
	out.println("\t\tcall CommControl.init();");

	if (this.enableLeds) {
	    out.println("\t\tcall Leds.init();");
	}

	if (this.controlRadioOff) {
	    out.println("\t\tcall CommControl.stop();  // nesc-control-radio-off=true");
	} else {
	    out.println("\t\tcall CommControl.start(); // nesc-control-radio-off=false");
	}

	if (this.usePowerManagement) {
	    out.println("\t\tcall PowerEnable(); \n");
	} else {
	    out.println("\t\t//not using HP Power Management \n");
	}
	out.println("\t\treturn SUCCESS;");
	out.println("\t}\n");

	out.println("\ttask void processAgendaItemsTask();\n");
	doInitialize(out, firstDelta);
	
	out.println("\tcommand result_t StdControl.start()");
	out.println("\t{");
	out.println("\t\tcall " + TinyOSGenerator.INTERFACE_TIMER
		+ TinyOSGenerator.INTERFACE_STDCONTROL + ".start();");
	if (tossimFlag) {
		out.println("\t\tcall SyncTimer.start(TIMER_REPEAT, "
			+ costParams.getSynchronizationError() + ");");
	} else {
		out.println("\t\tpost initialize();");
	}
	out.println("\t\treturn SUCCESS;");
	out.println("\t}");
	out.println();

	out.println("\tcommand result_t StdControl.stop()");
	out.println("\t{");
	out.println("\t\tcall " + TinyOSGenerator.INTERFACE_TIMER
		+ TinyOSGenerator.INTERFACE_STDCONTROL + ".stop();");

	if (tossimFlag) {
		out.println("\t\tcall SyncTimer.stop();");
	}

	out.println("\t\treturn SUCCESS;");
	out.println("\t}");
	out.println();
	
	if (tossimFlag) {
		doTossimSynchronization(sink, out, firstDelta);		
	}


    }

    private void doInvokeSleepTask(
	    final StringBuffer agendaCheckingBuff, final Task task) {
	agendaCheckingBuff.append("\t\t\t\t//sleep task\n");

	int ind = 0;
	agendaCheckingBuff.append("\t\t\t\tbusyUntil = 0;\n");
	//TODO: make this the responsiblity of the deliver
	if (this.controlRadioOff && radioOn == true && tossimFlag == false) {
		agendaCheckingBuff.append(Utils.indent(ind+4) + "call CommControl.stop();\n");
		radioOn = false;
	}

	agendaCheckingBuff.append(Utils.indent(ind)
		+ "\t\t\t\t//Sleep done by power management\n");
	
	if (tosVersion==1) {
		agendaCheckingBuff.append(Utils.indent(ind)
			+ "\t\t\t\tcall AgendaTimer.start(TIMER_ONE_SHOT, nextDelta);//in QueryPalnModuleComp.doInvokeSleepTask()\n");
	} else {
	    agendaCheckingBuff.append(Utils.indent(ind)
			    + "\t\t\t\tcall AgendaTimer.startOneShot(nextDelta);\n");		
	}

	if (this.deliverLast) {
	    agendaCheckingBuff.append("\t\t\t\t}\n");
	    agendaCheckingBuff.append("\t\t\t\telse\n");
	    agendaCheckingBuff.append("\t\t\t\t{\n");
	    agendaCheckingBuff
		    .append(Utils.indent(ind)
			    + "\t\t\t\tdbg(DBG_USR3,\"AGENDA: Mote %d End of Query Duration at %lld \\n\", NODE_NUM,tos_state.tos_time);\n");
	    //CB: Better would be to call snooze with max value
	    agendaCheckingBuff
		    .append(Utils.indent(ind)
			    + "\t\t\t\tdbg(DBG_POWER,\"POWER: Mote %d CPU_STATE POWER_SAVE at %lld \\n\", NODE_NUM,tos_state.tos_time);\n");
	    if (!((SleepTask) task).isLastInAgenda()) {
		agendaCheckingBuff
			.append(Utils.indent(ind)
				+ "\t\t\t\tdbg(DBG_USR3,\"AGENDA: Mote %d Jumping to non leaf start at %lld \\n\", NODE_NUM,tos_state.tos_time);\n");
		agendaCheckingBuff.append(Utils.indent(ind)
			+ "\t\t\t\tagendaTime=NON_LEAF_START;\n"); //TODO: change to agendaRow?
		agendaCheckingBuff.append(Utils.indent(ind)
			+ "\t\t\t\tnextDelta=NON_LEAF_DELTA;\n");
		agendaCheckingBuff
			.append(Utils.indent(ind)
				+ "\t\t\t\t//Jump straight to buffered task to deliver remaining results without sleeping.;\n");
		agendaCheckingBuff.append(Utils.indent(ind)
			+ "\t\t\t\tpost processAgendaItemsTask();\n");
		agendaCheckingBuff.append(Utils.indent(ind)
			+ "\t\t\t\treturn;\n");
	    } else {
		agendaCheckingBuff.append(Utils.indent(ind)
			+ "\t\t\t\t//Finished all buffered task.;\n");
	    }
	    agendaCheckingBuff.append(Utils.indent(ind) + "\t\t\t}\n");
	}
    }

    private void doInvokeCommunicationTask(
	    final StringBuffer firedTimerTaskBuff,
	    final StringBuffer agendaCheckingBuff, final boolean lastTime,
	    final boolean first, final Task task, final int ind,
	    boolean tossimFlag, StringBuffer radioOnTaskBuff) {
	if (tosVersion == 1) {
	    this.doInvokeT1CommunicationTask(firedTimerTaskBuff,
		    agendaCheckingBuff, lastTime, first, task, ind, tossimFlag, radioOnTaskBuff);
	} else {
	    this.doInvokeT2CommunicationTask(firedTimerTaskBuff,
		    agendaCheckingBuff, lastTime, first, task, ind); //TODO: ?tossimFlag?, radioOnTaskBuff?
	}
    }

    private String generateTaskName(final String commWiringName) {
	return commWiringName.substring(6) + "Task()";
    }

    private void doInvokeT1CommunicationTask(
	    final StringBuffer firedTimerTaskBuff,
	    final StringBuffer agendaCheckingBuff, final boolean lastTime,
	    final boolean first, final Task task, final int ind,
	    final boolean tossimFlag, StringBuffer radioOnTaskBuff) {
	final CommunicationTask commTask = (CommunicationTask) task;
	final String sourceNodeID = commTask.getSourceID();
	final String destNodeID = commTask.getDestID();
	final int mode = commTask.getMode();
	String prefix;
	if (mode == CommunicationTask.RECEIVE) {
	    prefix = "rx";
	} else {
	    prefix = "tx";
	}

	agendaCheckingBuff
		.append(Utils.indent(ind)
			+ "\t\t\t\tcall AgendaTimer.start(TIMER_ONE_SHOT, nextDelta);//doInvokeCommunicationTask\n");
	agendaCheckingBuff.append(Utils.indent(ind) + "\t\t\t\tdbg(DBG_USR2,\""
		+ prefix + "_n" + sourceNodeID + "n" + destNodeID
		+ " timer fired at row %d\\n\",agendaRow);\n");
	if (lastTime) {
	    agendaCheckingBuff.append(Utils.indent(ind)
		    + "\t\t\t\tbusyUntil = 0;\n");
	} else {
//TODO: fix		
//	    agendaCheckingBuff.append(Utils.indent(ind)
//		    + "\t\t\t\tbusyUntil = " + task.getEndTime() + ";\n");
	}

	String commTaskName = prefix + "_n" + sourceNodeID + "_n" + destNodeID + "M";
	
	//For mica2, if radio off, turn it on here!
	if (tossimFlag==false && radioOn==false && this.controlRadioOff) {
		agendaCheckingBuff.append(Utils.indent(ind+4) + "call CommControl.start();\n");
		radioOn = true;

		//Also set the radio power appropriately
	    if (this.adjustRadioPower) {
    		if (this.site.getOutputsList().size()>0) {
	    		Site parent = (Site)this.site.getOutput(0);
	    		int txPower = 255; //TODO: Need to fix this, need to get it from metadata now.
	    		//int txPower = (int)this.plan.getRT().getLinkEnergyCost(this.site, parent);
	    		agendaCheckingBuff.append(Utils.indent(ind+4) + 
	    				"call CC1000Control.SetRFPower("+
	    				txPower+"); // nesc-adjust-radio-power=true\n");
    		}
    	} //if (Settings.NESC_ADJUST_RADIO_POWER)	    
	}
	
	//In Tossim we don't turn the radio on/off
	//If we are receiving, we want the radio to be ready asap
	//These are cases when we don't need a delay after starting the radio up
	if (tossimFlag || mode == CommunicationTask.RECEIVE) { 
		agendaCheckingBuff.append(Utils.indent(ind+4) + "post " + commTaskName + "Task();\n");
	} else {
		//If we are going to transmit, and have just turned the radio on, we
		//need a delay after turning the radio on, prior to the radio starting to send packets
		if (this.controlRadioOff) {
			invokeRadioOnTask(commTaskName, agendaCheckingBuff, radioOnTaskBuff,
				ind+4, tosVersion);
		} else {
			agendaCheckingBuff.append(Utils.indent(ind+4) + "post " + commTaskName + "Task();\n");
		}
	}
	
	firedTimerTaskBuff.append("\ttask void " + commTaskName + "Task()\n");
	firedTimerTaskBuff.append("\t{\n");
	firedTimerTaskBuff.append("\t\tcall DoTask" + commTaskName + ".doTask();\n");
	firedTimerTaskBuff.append("\t}\n");		
	
    }

	private void invokeRadioOnTask(String commTaskName,
			final StringBuffer agendaCheckingBuff,
			StringBuffer radioOnTaskBuff, final int ind,
			int tosVersion) {
		
		if (tosVersion==1) {
			agendaCheckingBuff.append(Utils.indent(ind) + "call RadioOnTimer.start(TIMER_ONE_SHOT, " +
				this.costParams.getTurnOnRadio() + ");\n");
				radioOnTaskBuff.append("\tevent result_t RadioOnTimer.fired()\n");
				radioOnTaskBuff.append("\t{\n");
				radioOnTaskBuff.append("\t\tpost " + commTaskName + "Task();\n");
				radioOnTaskBuff.append("\t\treturn SUCCESS;\n");
				radioOnTaskBuff.append("\t}\n\n");
		} else {
//			agendaCheckingBuff.append(Utils.indent(ind) + "call RadioOnTimer.startOneShot("+
//					CostParameters.getTurnOnRadio()+");\n");
//			radioOnTaskBuff.append("\tevent void RadioOnTimer.fired()\n");
//			radioOnTaskBuff.append("\t{\n");
//			radioOnTaskBuff.append("\t\tpost " + commTaskName + "Task();\n");
//			radioOnTaskBuff.append("\t}\n\n");
		}

	}

    private void doInvokeT2CommunicationTask(
	    final StringBuffer firedTimerTaskBuff,
	    final StringBuffer agendaCheckingBuff, final boolean lastTime,
	    final boolean first, final Task task, final int ind) {
	final CommunicationTask commTask = (CommunicationTask) task;
	final int mode = commTask.getMode();

	//build array list of txs or rxs that need to be invoked as result of this commTask
	final ArrayList<String> txrxUserAsNames = new ArrayList<String>();

	final Iterator<Wiring> wiringIter = this.tossimConfig
		.wiringsIteratorForUser(this.getID());
	while (wiringIter.hasNext()) {
	    final Wiring w = wiringIter.next();
	    final NesCComponent comp = 
	    	this.tossimConfig.getComponent(w.getProvider());
	    final String userAsName = w.getUserAsName();
	    if (comp instanceof LedComponent) {
	    	continue;
	    }
	    if (!comp.getSite().getID().equals(commTask.getSiteID())) {
	    	continue;
	    }

	    if ((mode == CommunicationTask.TRANSMIT) && (comp instanceof TXT2Component)) {
	    	txrxUserAsNames.add(userAsName);
	    	continue;
	    }
	    
	    if ((mode == CommunicationTask.RECEIVE) && (comp instanceof RXT2Component)) {
	    	if (((RXT2Component)comp).getTxSite()==((CommunicationTask)task).getSourceNode()) {
		    	txrxUserAsNames.add(userAsName);
		    	continue;	    		
	    	}
	    }
	}

    agendaCheckingBuff.append(Utils.indent(ind)
		    + "\t\t\t\tcall AgendaTimer.startOneShot(nextDelta);\n");

	agendaCheckingBuff.append(Utils.indent(ind) + "\t\t\t\tdbg(\"DBG_USR2\",\""
		+ commTask.toString()
		+ " timer fired at row %d\\n\",agendaRow);\n");
	agendaCheckingBuff.append(Utils.indent(ind) + "\t\t\t\tpost "
		+ this.generateTaskName(txrxUserAsNames.get(0)) + ";\n");

	final StringBuffer taskBuffer = new StringBuffer();
	final StringBuffer eventBuffer = new StringBuffer();

	for (int i = 0; i < txrxUserAsNames.size(); i++) {
	    taskBuffer.append("\ttask void "
		    + this.generateTaskName(txrxUserAsNames.get(i)) + "\n");
	    taskBuffer.append("\t{\n");
	    taskBuffer.append("\t\tcall " + txrxUserAsNames.get(i)
		    + ".doTask();\n");
	    taskBuffer.append("\t}\n\n");
	    eventBuffer.append("\tevent void " + txrxUserAsNames.get(i)
		    + ".doTaskDone(error_t err)\n");
	    eventBuffer.append("\t{\n");
	    if (i + 1 < txrxUserAsNames.size()) {
		eventBuffer
			.append("\t\t//transmit tuples from the next tray\n");
		eventBuffer.append("\t\tpost "
			+ this.generateTaskName(txrxUserAsNames.get(i + 1))
			+ ";\n");
	    } else {
		eventBuffer
			.append("\t\t//tuples from all trays transmitted, do nothing\n");
	    }
	    eventBuffer.append("\t}\n\n"); //TODO: only applicable for T2?
	} 

	firedTimerTaskBuff.append(taskBuffer);
	firedTimerTaskBuff.append(eventBuffer);
    }

    private void doInvokeFragmentTask(
	    final StringBuffer firedTimerTaskBuff,
	    final StringBuffer agendaCheckingBuff, final boolean lastTime,
	    final boolean first, final Task task, final int ind,
	    StringBuffer radioOnTaskBuff) {
	final FragmentTask fragTask = (FragmentTask) task;
	final String fragID = fragTask.getFragment().getID();
	final String nodeID = fragTask.getSiteID();

	if (tosVersion == 1) {
	    agendaCheckingBuff
		    .append(Utils.indent(ind)
			    + "\t\t\t\tcall AgendaTimer.start(TIMER_ONE_SHOT, nextDelta);//QueryPlanModuleComp.doIinvokeFragmentTask()\n");
	} else {
	    agendaCheckingBuff.append(Utils.indent(ind)
		    + "\t\t\t\tcall AgendaTimer.startOneShot(nextDelta);\n");
	}

	if (lastTime) {
	    agendaCheckingBuff.append(Utils.indent(ind)
		    + "\t\t\t\tbusyUntil = 0;\n");
	} else {
//TODO: fix
//	    agendaCheckingBuff.append(Utils.indent(ind)
//		    + "\t\t\t\tbusyUntil = " + task.getEndTime() + ";\n");
	}

	if (tosVersion==1) {
		agendaCheckingBuff.append(Utils.indent(ind) + "\t\t\t\tdbg(DBG_USR2"
				+ ",\" F" + fragID + "n" + nodeID
				+ " timer fired at row %d\\n\",agendaRow);\n");		
	} else {
		agendaCheckingBuff.append(Utils.indent(ind) + "\t\t\t\tdbg(\"DBG_USR2\""
				+ ",\" F" + fragID + "n" + nodeID
				+ " timer fired at row %d\\n\",agendaRow);\n");		
	}
	
	
	String taskName = "F" + fragID + "n" + nodeID + "C";
	if (fragTask.getFragment().containsOperatorType(DeliverOperator.class) && tossimFlag == false) {
		//mica2 only: turn the radio on if not already on, and wait for delay
		if (this.controlRadioOff) {
			agendaCheckingBuff.append(Utils.indent(ind+4) + "call CommControl.start();\n");
			radioOn = true;
			invokeRadioOnTask(taskName, agendaCheckingBuff, radioOnTaskBuff, ind+4, tosVersion);
		} else {
			agendaCheckingBuff.append(Utils.indent(ind+4) + "post " + taskName + "Task();\n");
		}
		
	} else {
		agendaCheckingBuff.append(Utils.indent(ind+4) + "post " + taskName + "Task();\n");
	}
	if (fragTask.getOccurrence() == 1) {
	    firedTimerTaskBuff.append("\ttask void " + taskName + "Task()\n");
	    firedTimerTaskBuff.append("\t{\n");
	    Fragment frag = fragTask.getFragment();
	    firedTimerTaskBuff.append("\t\t\tcall "
	    		+ CodeGenUtils.generateUserAsDoTaskName(fragTask
			    .getFragment(), fragTask.getSiteID())
		     + ".doTask();\n");

	    firedTimerTaskBuff.append("\t}\n\n");
    
	    if (tosVersion==2) {
	    	firedTimerTaskBuff.append("\tevent void "
		    		+ CodeGenUtils.generateUserAsDoTaskName(fragTask
				    .getFragment(), fragTask.getSiteID())
			     + ".doTaskDone(error_t err)\n");
	    	firedTimerTaskBuff.append("\t{\n");
	    	firedTimerTaskBuff.append("\t}\n\n");
	    }
	    
    /*			firedTimerTaskBuff.append("\tevent void "+CodeGenUtils.generateUserAsDoTaskName(fragTask.getFragment(),
     fragTask.getSiteID())+".doTaskDone(error_t err)\n");
     firedTimerTaskBuff.append("\t{\n");
     firedTimerTaskBuff.append("\t\t//do nothing\n");
     firedTimerTaskBuff.append("\t}\n\n");*///TODO: only applicable for T2?
	}	
    }

    private long getNextDelta(final ArrayList<Long> startTimeList, final int i) {
	long nextDelta;
	if (i + 2 < startTimeList.size()) {
	    nextDelta = startTimeList.get(i + 2).intValue()
		    - startTimeList.get(i + 1).intValue();
	} else if (i + 1 < startTimeList.size()) {
	    nextDelta = Agenda.msToBms_RoundUp((int) (plan
		    .getAcquisitionInterval_ms() * plan.getBufferingFactor()))
		    - startTimeList.get(i + 1);
	} else {
	    nextDelta = startTimeList.get(1).intValue()
		    - startTimeList.get(0).intValue();
	}
	//CB: It takes one to start a timer
	nextDelta = nextDelta - 1;
	return nextDelta;
    }

    private void doQueryPlanModulePreamble(
	    final NesCConfiguration config, 
	    final boolean tossimFlag, final PrintWriter out,
	    final ArrayList<Long> startTimeList) {

    	final String header = config
		.generateModuleHeader(TinyOSGenerator.COMPONENT_QUERY_PLAN);
	out.println(header);

	if (this.deliverLast) {
	    out.println("\t#define NON_LEAF_START " + agenda.getNonLeafStart()
		    + "\n");
	    out.println("\t#define NON_LEAF_DELTA "
		    + (startTimeList.get(startTimeList.indexOf(agenda
			    .getNonLeafStart()) + 1)
			    - agenda.getNonLeafStart() - 1) + "\n");
	}
	out.println("\n");
	out.println("\tuint32_t nextDelta;\n");
	out.println("\tuint32_t agendaRow;\n");
	out.println("\tint busyUntil;\n");
    }
}