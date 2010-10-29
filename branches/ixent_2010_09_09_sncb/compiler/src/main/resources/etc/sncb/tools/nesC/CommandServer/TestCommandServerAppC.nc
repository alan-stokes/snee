configuration TestCommandServerAppC {
}
implementation {
  components TestCommandServerC, CommandServerAppC, MainC;
  components LedsC;    

  TestCommandServerC.Boot -> MainC.Boot;
  TestCommandServerC.Leds -> LedsC;  
  TestCommandServerC.SplitControl -> CommandServerAppC.SplitControl;
  TestCommandServerC.StateChanged -> CommandServerAppC.StateChanged;

#ifdef COMMAND_SERVER_BASESTATION
  components SerialStarterC;
#endif
}

