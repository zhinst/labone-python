@0x8440bbc31dc1ee7e;

using Cxx = import "/capnp/c++.capnp";
# zhinst::capnp would be nicer than zhinst_capnp, but there's the chance of namespace
# collisions with the "capnp" namespace
$Cxx.namespace("zhinst_capnp");

# Uuid is expected to always be 16 bytes (128 bits) long.
# We can't enforce this unfortunately because Capnp doesn't have a way to declare
# fixed size arrays.
using Uuid = Data;

# At the moment, this is used only for testing. In the future we may rename it
# and reuse it, if we see that using a raw buffer as Uuid is too inconvenient.
struct TestUuid @0xe6dd15a768032fd6 {
    uuid @0 :Uuid;
}

