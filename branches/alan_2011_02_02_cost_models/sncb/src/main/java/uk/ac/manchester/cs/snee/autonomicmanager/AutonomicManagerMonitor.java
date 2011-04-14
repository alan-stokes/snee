package uk.ac.manchester.cs.snee.autonomicmanager;

import java.util.Observable;
import java.util.Observer;

import uk.ac.manchester.cs.snee.ResultStore;
import uk.ac.manchester.cs.snee.sncb.SNCBSerialPortReceiver;
import uk.ac.manchester.cs.snee.sncb.SerialPortMessageReceiver;

public class AutonomicManagerMonitor implements Observer
{
  private AutonomicManager manager;
  private SerialPortMessageReceiver listener;
  private float sneeTuplesPerEpoch = 0;
  private float sneeTuplesPerAgendaCycle = 0;
  private ResultStore _results;

  public AutonomicManagerMonitor(AutonomicManager autonomicManager)
  {
    manager = autonomicManager;
  }

  @Override
  public void update(Observable o, Object arg)
  {
    // TODO Auto-generated method stub
    System.out.println("got packets");
    CECMCollection();
    
    manager.callAnaysliserAnaylsisSNEECard(sneeTuplesPerEpoch, sneeTuplesPerAgendaCycle);
  }

  private void CECMCollection()
  {
    // TODO Auto-generated method stub
    
  }

  public void addPacketReciever(SNCBSerialPortReceiver mr)
  {
    listener = (SerialPortMessageReceiver) mr;
    listener.addObserver(this);// TODO Auto-generated method stub
  }

}
