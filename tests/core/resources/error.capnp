@0xb97062d62cb99beb;

using Cxx = import "/capnp/c++.capnp";
# zhinst::capnp would be nicer than zhinst_capnp, but there's the chance of namespace
# collisions with the "capnp" namespace
$Cxx.namespace("zhinst_capnp");

# The error kind advises the client on how to handle the error.
enum ErrorKind @0xb7e671e24a9802bd {
                    # approximate HTTP status codes:
  ok            @0; # HTTP 200
  cancelled     @1;
  unknown       @2;
  notFound      @3; # HTTP 404
  overwhelmed   @4; # HTTP 429
  badRequest    @5; # HTTP 400
  unimplemented @6; # HTTP 501
  internal      @7; # HTTP 500
  unavailable   @8; # HTTP 593
  timeout       @9; # HTTP 504
}

struct Error @0xc4e34e4c517d11d9 {
  code @0 :UInt32;
  message @1 :Text;
  category @2 :Text;
  kind @3 :ErrorKind = unknown;
  source @4 :Text;
}

