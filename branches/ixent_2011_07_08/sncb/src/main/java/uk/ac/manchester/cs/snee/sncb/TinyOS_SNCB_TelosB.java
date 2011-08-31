package uk.ac.manchester.cs.snee.sncb;

import java.io.IOException;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

public class TinyOS_SNCB_TelosB extends TinyOS_SNCB implements SNCB {
  
//	private SensorNetworkQueryPlan qep;
//	private String queryOutputDir;

	public TinyOS_SNCB_TelosB(double duration) throws SNCBException {
	  super(duration);
	  this.target = CodeGenTarget.TELOSB_T2;
      try {
	  	if (SNEEProperties.isSet(SNEEPropertyNames.SNCB_INCLUDE_COMMAND_SERVER)) {	
			useNodeController = SNEEProperties
				.getBoolSetting(SNEEPropertyNames.SNCB_INCLUDE_COMMAND_SERVER);				
			}
	    this.serialPort = this.getBaseStation();
      } catch (Exception e) {
			this.serialPort = null;
	  }
	  //More TinyOS environment variables
	  if (serialPort != null) {
			this.tinyOSEnvVars.put("MOTECOM", "=serial@" + serialPort);
			this.tinyOSEnvVars.put("SERIAL_PORT", serialPort);
	  }

	}
	
	
	public TinyOS_SNCB_TelosB() throws SNCBException 
	{
		this(Double.POSITIVE_INFINITY);
	}

	private String getBaseStation() throws SNCBException {
		if (logger.isTraceEnabled())
			logger.debug("ENTER getBaseStation()");
		if (!this.useNodeController) {
			return null;
		}
		String serialPort;
		try {
			String pythonScript = Utils
					.getResourcePath("etc/sncb/tools/python/utils/basestation.py");
			String params[] = { pythonScript };
			String outputLines = Utils.runExternalProgram("python", params,
					this.tinyOSEnvVars, workingDir);
			String outputList[] = outputLines.split("\n");
			if (outputList.length < 2) {
				System.out.println("No basestation found.\n\n");
				return null;
			} else if (outputList.length > 2) { // FIXME: This doesn't work.
				throw new SNCBException(
						"Unable to determine base station mote, as more than one mote is plugged in.");
			}
			serialPort = outputList[1];
		} catch (Exception e) {
			logger.warn(e.getLocalizedMessage(), e);
			throw new SNCBException(e.getLocalizedMessage(), e);
		}
		if (logger.isTraceEnabled())
			logger.debug("RETURN getBaseStation()");
		return serialPort;
	}
	
	@Override
	public void init(String topFile, String resFile) throws SNCBException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER init()");
		try {
			logger.trace("Forming network and collecting metadata");
			System.out.println("Forming network and collecting metadata");
	
			String pythonScript = Utils
					.getResourcePath("etc/sncb/tools/python/init");
			String params[] = { pythonScript, topFile, resFile };
			Utils.runExternalProgram("python", params, this.tinyOSEnvVars,
					workingDir);
		} catch (Exception e) {
			logger.warn(e);
			throw new SNCBException(e.getLocalizedMessage(), e);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN init()");
	}
	
  public SerialPortMessageReceiver register(SensorNetworkQueryPlan qep,
      String queryOutputDir, MetadataManager costParams)
      throws SNCBException {
    if (logger.isDebugEnabled())
      logger.debug("ENTER register()");
	super.register(qep, queryOutputDir, costParams);
//    this.qep = qep;
//    this.queryOutputDir = queryOutputDir;
	
    SerialPortMessageReceiver mr = null;
    try {
        if (this.useNodeController && this.serialPort!=null)
        {
			logger.trace("Disseminating Query Plan images");
			System.out.println("Disseminating Query Plan images");
			disseminateQueryPlanImages(qep, queryOutputDir);
			
			if (demoMode) {
			    System.out
			        .println("Query plan image dissemination complete.\n");
			    System.in.read();
			}
			
			logger.trace("Setting up result collector");
			System.out.println("Setting up result collector");
			mr = setUpResultCollector(qep, queryOutputDir);

			if (demoMode) {
			    System.out.println("Serial port listener for query results ready.");
			    System.in.read();
			}
      } else {
	        if (!this.useNodeController || this.serialPort==null) {
	        	System.out.println("Not using node controller, or no mote "+
	            "plugged in, so unable to send query plan using" +
	            "Over-the-air Programmer. ");
	        	System.out.println("Please proceed using manual commands.\n");
	        	if (this.target == CodeGenTarget.TELOSB_T2) {
	        		TinyOS_SNCB_Utils.printTelosBCommands(queryOutputDir, qep,
	        				this.getTargetDirName(), this.serialPort);
	        	} 
	        	System.exit(0);
	        }
      }

    } catch (Exception e) {
      logger.warn(e.getLocalizedMessage(), e);
      throw new SNCBException(e.getLocalizedMessage(), e);
    }
    if (logger.isDebugEnabled())
      logger.debug("RETURN register()");
    return mr;
  }

	@Override
	public void start() throws SNCBException {
		// Call python start script
		if (logger.isDebugEnabled())
			logger.debug("ENTER start()");
		try {
				logger.trace("Invoking start command");
				System.out.println("Invoking start command");
	
				String pythonScript = Utils
						.getResourcePath("etc/sncb/tools/python/start");
				String params[] = { pythonScript };
				Utils.runExternalProgram("python", params, this.tinyOSEnvVars,
						workingDir);
	    } catch (IOException e) {
			logger.warn(e.getLocalizedMessage());
			throw new SNCBException(e.getLocalizedMessage(), e);
		}
		
		isStarted = true;

		// Kick off a thread to do synchronisation
		(new Thread(new Runnable() {

			@Override
			public void run() {
				// loop continuously until the stop() method is invoked
				while (isStarted) {
					try {
						Thread.sleep(60 * 1000);

						// cycle a stop/start to synchronise nodes. NOTE: This
						// start/stop mechanism does not guarantee
						// synchronisation. It was chosen as a trade-off
						// between code size and accuracy.
						//pause();
						//resume();
					} catch (Exception e) {
						// General fail case
						System.out.println("Execution failed. See logs for detail.");
						logger.fatal(e);
						System.exit(1);
					}
				}
			}
		})).start();

		if (logger.isDebugEnabled())
			logger.debug("RETURN start()");
	}

	@Override
	public void stop(SensorNetworkQueryPlan qep) throws SNCBException {
		isStarted = false;
		
		if (logger.isDebugEnabled())
			logger.debug("ENTER stop()");
		try {
			logger.trace("Invoking stop command");
			System.out.println("Invoking stop command");

			String pythonScript = Utils
					.getResourcePath("etc/sncb/tools/python/stop");
			String gatewayID = Integer.toString(qep.getGateway());
			String params[] = { pythonScript, gatewayID };
			Utils.runExternalProgram("python", params, this.tinyOSEnvVars, workingDir);
		} catch (Exception e) {
			logger.warn(e.getLocalizedMessage());
			throw new SNCBException(e.getLocalizedMessage(), e);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN stop()");
	}

	@Override
	public void deregister(SensorNetworkQueryPlan qep) throws SNCBException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER deregister()");
		try {
			logger.trace("Invoking deregister command");
			System.out.println("Invoking deregister command");

			String pythonScript = Utils
					.getResourcePath("etc/sncb/tools/python/deregister");
			String gatewayID = "" + qep.getGateway();
			Iterator<Site> siteIter = qep
					.siteIterator(TraversalOrder.POST_ORDER);
			StringBuffer siteString = new StringBuffer();
			while (siteIter.hasNext()) {
				String siteID = siteIter.next().getID();
				// skip the gateway
				if (siteID.equals(gatewayID))
					continue;
				siteString.append(siteID + " ");
			}
			String params[] = { pythonScript, siteString.toString() };
			Utils.runExternalProgram("python", params, this.tinyOSEnvVars,
					workingDir);
		} catch (Exception e) {
			logger.warn(e.getLocalizedMessage());
			throw new SNCBException(e.getLocalizedMessage(), e);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN deregister()");
	}

	private void pause() throws SNCBException {
		if (!isStarted) return;
		
		if (logger.isDebugEnabled())
			logger.debug("ENTER pause()");
		
//		// release the serial port		
//		mr.pause();
		
		try {
			logger.trace("Invoking pause command");
			System.out.println("Invoking pause command");

			String pythonScript = Utils.getResourcePath("etc/sncb/tools/python/pause");
			String params[] = { pythonScript };
			Utils.runExternalProgram("python", params, this.tinyOSEnvVars, workingDir);
		} catch (Exception e) {
			logger.warn(e.getLocalizedMessage());
			throw new SNCBException(e.getLocalizedMessage(), e);
		}
		
		if (logger.isDebugEnabled())
			logger.debug("RETURN pause()");
	}

	private void resume() throws SNCBException {
		if (!isStarted) return;
		
		if (logger.isDebugEnabled())
			logger.debug("ENTER resume()");
		
		try {
			logger.trace("Invoking resume command");
			System.out.println("Invoking resume command");

			String pythonScript = Utils.getResourcePath("etc/sncb/tools/python/resume");
			String params[] = { pythonScript };
			Utils.runExternalProgram("python", params, this.tinyOSEnvVars, workingDir);
		} catch (Exception e) {
			logger.warn(e.getLocalizedMessage());
			throw new SNCBException(e.getLocalizedMessage(), e);
		}
				
//		// reclaim the serial port
//		mr.resume();
		
		if (logger.isDebugEnabled())
			logger.debug("RETURN resume()");
	}

  
//  @Override
//  public void waitForQueryEnd() throws InterruptedException
//  {
//    if(duration == Double.POSITIVE_INFINITY)
//      Thread.currentThread().sleep((long)duration); 
//    else
//      Thread.currentThread().sleep((long)duration * 1000); 
//  }
}
