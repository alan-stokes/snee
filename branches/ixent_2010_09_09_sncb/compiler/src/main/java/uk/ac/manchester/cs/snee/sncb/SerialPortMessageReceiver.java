package uk.ac.manchester.cs.snee.sncb;

import net.tinyos.message.Message;
import net.tinyos.message.MoteIF;
import net.tinyos.packet.BuildSource;
import net.tinyos.util.PrintStreamMessenger;

//Based on TinyOS MsgReader.java class
public class SerialPortMessageReceiver implements net.tinyos.message.MessageListener {

	  private MoteIF moteIF;
	  
	  public SerialPortMessageReceiver(String source) throws Exception {
	    if (source != null) {
	      moteIF = new MoteIF(BuildSource.makePhoenix(source, PrintStreamMessenger.err));
	    }
	    else {
	      moteIF = new MoteIF(BuildSource.makePhoenix(PrintStreamMessenger.err));
	    }
	  }

	  public void start() {
	  }
	  
	  @Override
	  public void messageReceived(int to, Message message) {
	    long t = System.currentTimeMillis();
	    //    Date d = new Date(t);
	    System.out.print("" + t + ": ");
	    System.out.println(message);
	  }

	  protected void addMsgType(Message msg) {
	    moteIF.registerListener(msg, this);
	  }

}
