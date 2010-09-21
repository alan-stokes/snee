configuration TestCommandServerAppC {
}
implementation {
    components TestCommandServerC, CommandServerAppC, MainC, LedsC;

    TestCommandServerC.Boot -> MainC.Boot;
    TestCommandServerC.Leds -> LedsC;  
    TestCommandServerC.SplitControl -> CommandServerAppC.SplitControl;
    TestCommandServerC.StateChanged -> CommandServerAppC.StateChanged;
}

