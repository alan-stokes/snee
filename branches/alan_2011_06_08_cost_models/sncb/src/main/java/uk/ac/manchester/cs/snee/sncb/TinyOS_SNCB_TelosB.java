package uk.ac.manchester.cs.snee.sncb;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;

public class TinyOS_SNCB_TelosB extends TinyOS_SNCB implements SNCB {

  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -5687714259713060991L;
  private double duration = 0;
  private static final Logger logger = Logger.getLogger(TinyOS_SNCB_TelosB.class.getName());
  
	public TinyOS_SNCB_TelosB(double duration) throws SNCBException {
	  this.duration = duration;
		setup();
	}
	
  public TinyOS_SNCB_TelosB() throws SNCBException 
  {
     setup();
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
    {
      Thread.currentThread();
      Thread.sleep((long)duration); 
    }
    else
    {
      Thread.currentThread();
      Thread.sleep((long)duration * 1000); 
    }
  }
}
