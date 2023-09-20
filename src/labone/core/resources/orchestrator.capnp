@0x9c2e0a9bb9b48f32;

using Version = Text;

interface Orchestrator @0xc4f8eb57ff6a6601 {
  const capabilityVersion :Version = "1.6.0";

    enum ErrorCode @0xfbae13420614145a {
        ok @0;
        unknown @1;
        kernelNotFound @2;
        illegalDeviceIdentifier @3;
        deviceNotFound @4;
        kernelLaunchFailure @5;
        firmwareUpdateRequired @6;
        interfaceMismatch @7;
        # The device is visible, but cannot be connected through the requested
        # interface.
        differentInterfaceInUse@8;
        deviceInUse @9;
        unsupportedApiLevel @10;
        # Generic problem interpreting the incoming request
        badRequest @11;
    }

    struct Error @0x8e3d8f0587488365 {
        code @0 :ErrorCode;
        message @1 :Text;
    }
}