@0x9c2e0a9bb9b48f32;

using Cxx = import "/capnp/c++.capnp";
# zhinst::capnp would be nicer than zhinst_capnp, but there's the chance of namespace
# collisions with the "capnp" namespace

using import "result.capnp".Result;
using import "uuid.capnp".Uuid;

using Version = Text;

interface Orchestrator @0xc4f8eb57ff6a6601 {
  # 1.0.0: initial version
  # 1.1.0: Added version to hello message
  # 1.2.0: Extend ErrorCode with `differentInterfaceInUse` and `deviceInUse`
  # 1.3.0: Add API level field to DeviceConnectionSettings struct
  # 1.4.0: Added "http" as protocol in the hello message
  # 1.5.0: Deprecated sessionProtocolVersion in favor of version
  # 1.6.0: Added "bad_request" error code
  const capabilityVersion :Version = "1.6.0";

  getCapabilityVersion @3 () -> (version :Version);

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

    getKernelInfo @0 (device :DeviceIdentifier, devConnSettings :DeviceConnectionSettings) -> (result :Result(KernelDescriptor, Error));
    # Get information about the kernel currently in charge of the communication
    # with the requested device. If the device is currently disconnected, the
    # Orchestrator will launch a new kernel automatically, unless
    # disableKernelAutoLaunch is set to true, in which case "kernelNotFound" is
    # returned. If the kernel has to be started, getKernelInfo will return only
    # once the kernel has been launched, or will return "kernelLaunchFailure" if
    # the launch fails (which should rarely happen). Note however that
    # getKernelInfo won't wait for the device to be connected to the kernel. The
    # client should contact the kernel to retrieve the status of the
    # kernel-device connection.
    #
    # Possible errors:
    #  * illegalDeviceIdentifier: if the DeviceIndetifier cannot be understood
    #  * deviceNotFound: if there is no device visible with the given id
    #  * kernelNotFound: if disableKernelAutoLaunch is true and there is no kernel
    #    connected to the requested device
    #  * kernelLaunchFailure: if the Orchestrator fails to launch a new kernel
    #    (this should rarely occur)
    #  * firmwareUpdateRequired: if the device requires a firmware update

    getSelfKernelInfo @1 () -> (result :Result(KernelDescriptor, Error));
    # Get information about the orhcestrator own kernel. The orchestrator kernel is
    # a special, virtual kernel that provides access to those nodes that do not
    # belong to any device, i.e. the /zi/* nodes.

    getMdkKernelInfo @2 () -> (result :Result(KernelDescriptor, Error));
    # Get information about the orhcestrator MDK kernel. The MDK kernel wraps
    # the old data server (AsyncServerSocket).
}

using DeviceId = Text;

# Mirrors ziCommons/src/main/include/zhinst/device/common/device_interface.hpp,
# but the numbering for pcie and unknown is different because capnp doesn't
# allow nonsequential ordinals
enum DeviceInterface @0xb2cb5725fb3efecc {
    none @0;
    usb @1;
    ip @2;
    pcie @3;
    unknown @4;
}

# Note: apiLevel is not defined as an enum because the ZIAPIVersion_enum is not
# sequential, so the capnp enum would have values that are not consistent with
# the names, e.g. the capnp enum value for ZIAPIVersion_enum::ZI_API_VERSION_6
# would have the value 4, which could cause some confusion.
struct DeviceConnectionSettings @0x9e93174950a60922 {
    interface @0 :DeviceInterface;
    disableKernelAutoLaunch @1 :Bool;
    apiLevel @2 :UInt8 = 6;
}

struct DeviceIdentifier @0xad910ed0a4f023e7 {
    # In the future, the DeviceIdentifier may contain a union of DeviceId and
    # e.g. the ip address of the device. Changing "deviceId" to become a union
    # is a backward-compatible change
    deviceId @0 :DeviceId;
}

enum ClientWireProtocol @0xe1f874dc54525678 {
    binmsg @0;
    capnp @1;
}

struct KernelDescriptor @0xb16d65575faa88ac {
    host @0 :Text;
    port @1 :UInt16;
    protocol @2 :ClientWireProtocol;
    uid @4 :Uuid;
    # If isRemote = false, then the Kernel runs on the same host of the Orchestrator.
    # Clients can then safely use the host of the Orchestrator, instead of the one
    # reported in the "host" field and avoid a DNS lookup.
    # This allows clients to connect to kernels also in situations where DNS cannot
    # be relied upon (e.g. when running the data server in a Docker container).
    isRemote @5 :Bool;
    version @6 :Version;

    # sessionProtocolVersion was added very early, despite not being used, because we thought
    # it would eventually become useful. The reasoning was correct, but we made the mistake
    # of using uint16 as type. This is not compatible with our VersionTriple class, and thus
    # it has been deprecated in favor of "version" which encodes the version triple as text.
    deprecatedSessionProtocolVersion @3 :UInt16;
}
