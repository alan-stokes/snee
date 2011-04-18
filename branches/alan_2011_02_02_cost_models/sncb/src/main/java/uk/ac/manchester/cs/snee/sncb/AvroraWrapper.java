package uk.ac.manchester.cs.snee.sncb;

import avrora.Main;

public class AvroraWrapper implements Runnable

{
  String commands = ""; 
  
  public AvroraWrapper(String commands)
  {
    this.commands = commands;
  }
  
  @SuppressWarnings("static-access")
  public void run()
  {
   // Main avrora = new Main();
    String [] args = commands.split(" ");
    //avrora.main(args);
    // parse the command line options
    try
    {
      Main.main(args);
      Thread.currentThread().yield();
    } catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
