@0xf7e8e31fdca4abd5;

using Cxx = import "/capnp/c++.capnp";
# zhinst::capnp would be nicer than zhinst_capnp, but there's the chance of namespace
# collisions with the "capnp" namespace
$Cxx.namespace("zhinst_capnp");

struct Result @0xbab0f33e1934323d (Type, Error) {
  union {
    ok @0 :Type;
    err @1 :Error;
  }
}

