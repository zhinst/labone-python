@0xef99a05432b7c2c9;

using Cxx = import "/capnp/c++.capnp";
# zhinst::capnp would be nicer than zhinst_capnp, but there's the chance of namespace
# collisions with the "capnp" namespace
$Cxx.namespace("zhinst_capnp");

using Version = Text;

struct HelloMsg @0xd62994dbff882318 {
  const fixedLength :UInt16 = 256;

  kind @0 :Kind;
  protocol @1 :Protocol = capnp;
  schema @2 :Version;
  l1Ver @3 :Version;

  enum Kind @0x8dc0ce66c2de04e2 {
    unknown @0;
    orchestrator @1;
    hpk @2;
    client @3;
  }
  enum Protocol @0xe737d5f7b51820cf {
      unknown @0;
      capnp @1;
      http @2;
    }
}

