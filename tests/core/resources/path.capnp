@0xc5aec659eb26a09e;

using Cxx = import "/capnp/c++.capnp";
# zhinst::capnp would be nicer than zhinst_capnp, but there's the chance of namespace
# collisions with the "capnp" namespace
$Cxx.namespace("zhinst_capnp");

using Path = Text;
using Paths = List(Path);
using PathExpression = Text;

