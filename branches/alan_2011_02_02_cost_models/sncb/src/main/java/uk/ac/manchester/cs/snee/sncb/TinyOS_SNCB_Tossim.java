package uk.ac.manchester.cs.snee.sncb;

import java.util.HashMap;

import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.Utils;

public class TinyOS_SNCB_Tossim extends TinyOS_SNCB implements SNCB 
{
  private double duration = 0;
  
  public TinyOS_SNCB_Tossim(double duration) throws SNCBException 
  {
    if (logger.isDebugEnabled())
      logger.debug("ENTER TinyOS_SNCB()");
    try {
      // TinyOS environment variables
      this.tinyOSEnvVars = new HashMap<String, String>();
      this.duration = duration;
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

  @Override
  public void waitForQueryEnd() throws InterruptedException
  {
    if(duration == Double.POSITIVE_INFINITY)
      Thread.currentThread().sleep((long)duration); 
    else
      Thread.currentThread().sleep((long)duration * 1000); 
  }
  
  
}
