configuration TestAppC {
}
implementation {
    components TestC, TestDisseminationAppC, MainC, LedsC;

    TestC.Boot -> MainC.Boot;
    TestC.Leds -> LedsC;  
    TestC.SplitControl -> TestDisseminationAppC;
    TestC.StateChanged -> TestDisseminationAppC;
}

