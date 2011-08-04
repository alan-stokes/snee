package uk.ac.manchester.cs.snee.sncb;

import java.util.HashMap;

import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;

public class TinyOS_SNCB_TelosB extends TinyOS_SNCB implements SNCB {

  private double duration = 0;
  
	public TinyOS_SNCB_TelosB(double duration) throws SNCBException {
	  this.duration = duration;
		if (logger.isDebugEnabled())
			logger.debug("ENTER TinyOS_SNCB()");
		try {
			// TinyOS environment variables
			this.tinyOSEnvVars = new HashMap<String, String>();
			workingDir = Utils.getResourcePath("etc/sncb/tools/python");
			String currentPath = System.getenv("PATH");
			this.tinyOSEnvVars.put("PATH", currentPath + ":" + workingDir + ":"
					+ workingDir + "/utils");
			
			//Check if a mote is plugged in.  If no mote is plugged in, this is null.
			this.serialPort = this.getBaseStation();
			
			// Check if we are using the node controller
			if (SNEEProperties.isSet(SNEEPropertyNames.SNCB_INCLUDE_COMMAND_SERVER)) {
				useNodeController = SNEEProperties
				.getBoolSetting(SNEEPropertyNames.SNCB_INCLUDE_COMMAND_SERVER);				
			}
			// Check whether to generate combined image or individual image
			if (SNEEProperties.isSet(SNEEPropertyNames.SNCB_GENERATE_COMBINED_IMAGE)) {
				this.combinedImage = SNEEProperties
					.getBoolSetting(SNEEPropertyNames.SNCB_GENERATE_COMBINED_IMAGE);
			}
			// Parse the code generation target
			if (SNEEProperties.isSet(SNEEPropertyNames.SNCB_CODE_GENERATION_TARGET)) {
				this.target = CodeGenTarget.parseCodeTarget(SNEEProperties
					.getSetting(SNEEPropertyNames.SNCB_CODE_GENERATION_TARGET));
			}
			// Node controller is only compatible with Tmote Sky/Tiny OS2
			if (this.target != CodeGenTarget.TELOSB_T2 && useNodeController) {
				logger.warn("Node controller is only compatible with Tmote Sky/Tiny OS2. " +
						"Excluding controller from generated code.");
				useNodeController = false;
			}
			targetDirName = target.toString().toLowerCase();
			
			//More TinyOS environment variables
			if (serialPort != null) {
				this.tinyOSEnvVars.put("MOTECOM", "=serial@" + serialPort);
				this.tinyOSEnvVars.put("SERIAL_PORT", serialPort);
			}
		} catch (Exception e) {
			//If an error occurs (e.g., TinyOS is not installed so motelist command fails) serialPort is null.
			this.serialPort = null;
			this.target = CodeGenTarget.TELOSB_T2;
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN TinyOS_SNCB()");
	}
	
  public SerialPortMessageReceiver register(SensorNetworkQueryPlan qep,
      String queryOutputDir, MetadataManager costParams)
      throws SNCBException {
    if (logger.isDebugEnabled())
      logger.debug("ENTER register()");
    SerialPortMessageReceiver mr = null;
    try {
      if (demoMode) {
        System.out.println("Query compilation complete.\n");
        System.in.read();
      }

      logger.trace("Generating TinyOS/nesC code for query plan.");
      System.out.println("Generating TinyOS/nesC code for query plan.");
      generateNesCCode(qep, queryOutputDir, costParams);

      if (demoMode) {
        System.out.println("nesC code generation complete.\n");
        System.in.read();
      }

      logger.trace("Compiling TinyOS/nesC code into executable images.");
      System.out
          .println("Compiling TinyOS/nesC code into executable images.");
      compileNesCCode(queryOutputDir);

      if (demoMode) {
        System.out.println("nesC code compilation complete.\n");
        System.in.read();
      }

      if (!this.useNodeController || this.serialPort==null) {
        System.out.println("Not using node controller, or no mote "+
            "plugged in, so unable to send query plan using" +
            "Over-the-air Programmer. ");
        System.out.println("Please proceed using manual commands.\n");
        if (this.target == CodeGenTarget.TELOSB_T2) {
          TinyOS_SNCB_Utils.printTelosBCommands(queryOutputDir, qep,
              this.targetDirName, this.serialPort);
        } 
      }
      else
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
  public void waitForQueryEnd() throws InterruptedException
  {
    if(duration == Double.POSITIVE_INFINITY)
      Thread.currentThread().sleep((long)duration); 
    else
      Thread.currentThread().sleep((long)duration * 1000); 
  }
}
