@0x8461c2916c39ad0e;

using Schema = import "/capnp/schema.capnp";

struct CapSchema @0xcb31ef7a76eb85cf {
  typeId @0 :UInt64;
  theSchema @1 :List(Schema.Node);
}

interface Reflection @0xf9a52e68104bc776 {
  getTheSchema @0 () -> (theSchema :CapSchema);
}

