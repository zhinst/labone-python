@0xb6c21289d5437347;

using Cxx = import "/capnp/c++.capnp";
# zhinst::capnp would be nicer than zhinst_capnp, but there's the chance of namespace
# collisions with the "capnp" namespace
$Cxx.namespace("zhinst_capnp");

using import "error.capnp".Error;
using import "path.capnp".Path;
using import "path.capnp".Paths;
using import "path.capnp".PathExpression;
using import "result.capnp".Result;
using import "uuid.capnp".Uuid;
using import "value.capnp".AnnotatedValue;
using import "value.capnp".Value;
using import "value.capnp".Void;

using Version = Text;
# A unique identifier for a client. Note that the HPK makes no assumption about
# what a client is. In particular, the HPK does not assume there's any relationship
# between sockets/ip and client id. The same connection can be used by different clients,
# and the same client can use different connections
using ClientId = Uuid;
const defaultClientId :ClientId = "";

struct Subscription @0xedac21a53de1b1d4 {
  path @0 :Path;
  streamingHandle @1 :StreamingHandle;
  subscriberId @2 :ClientId;
}

# Unless otherwise specified, all Paths accepted by the Session interface are
# case insensitive and can be either absolute (starts with a '/') or relative.
# If a path is relative, it is assumed to be relative to the device id. For
# example path "raw/int" will be expanded to "/devXXX/raw/int".
interface Session @0xb9d445582da4a55c {
  # 1.0.0: initial version.
  # 1.1.0: Added version to hello message.
  # 1.2.0: Existing setValue was deprecated in favour of a new version that returns the acknowledged value.
  #        Added None type to Value union.
  # 1.3.0: Added ClientId to setValue and getValue. This allows reordering of requests with different clientIds.
  # 1.4.0: Added "http" as protocol in the hello message
  # 1.5.0: Added setValue and new getValue function that accept PathExpressions as input.
  const capabilityVersion :Version = "1.5.0";

  getCapabilityVersion @7 () -> (version :Version);

  listNodes @0 (pathExpression :PathExpression, flags :UInt32, client :ClientId) -> (paths :Paths);
  # The "client" field is needed only when subscribedonly is used.

  getValue @10 (pathExpression :PathExpression,
                lookupMode :LookupMode = directLookup,
                flags :UInt32 = 0,
                client :ClientId = .defaultClientId)
            -> (result :List(Result(AnnotatedValue, Error)));
  # Note 1: A pathExpressions can be anything that listNodes can resolve.
  #         This means that, e.g., wildcards are allowed. Passing multiple
  #         comma-separated paths as a single pathExpression is also possible.
  # Note 2: The lookupMode controls if and how pathExpressions are resolved.
  #         By default the pathExpression is expected to be a node path pointing
  #         to an existing leaf node. If lookupMode is set to withExpansion,
  #         the server tries to resolve the pathExpression with listNodes internally
  #         and returns the value for all matching nodes.

  setValue @9 (pathExpression :PathExpression,
               value        :Value,
               lookupMode :LookupMode = directLookup,
               completeWhen :ReturnFromSetWhen = deviceAck,
               client :ClientId = .defaultClientId)
            -> (result :List(Result(AnnotatedValue, Error)));
  # Note 1: in certain cases, the HPK allows setting nodes with a type different
  #         from that of the value passed in. For example, it is allowed to set
  #         a double from an integer. The returned value is always of the type
  #         of the node. So it can happen that setValue returns a value of a type
  #         different from the one that was passed in.
  # Note 2: The AnnotatedValue is only returned if `completeWhen` is set to `deviceAck`.
  # Note 3: The lookupMode controls if and how pathExpressions are resolved.
  #         By default the pathExpression is expected to be a node path pointing
  #         to an existing leaf node. If lookupMode is set to withExpansion,
  #         the server tries to resolve the pathExpression with listNodes internally
  #         and sets the value to all matching nodes.


  subscribe @3 (subscription :Subscription) -> (result :Result(Void, Error));
  # Note 1: this function currently does not support wildcards in the path
  # Note 2: it is safe to have multiple subscriptions with the same subscriberId
  #         and path. This is convenient as it allows rapid resubscription.
  #         You can unsubscribe, then subscribe again even before the unsubscribe
  #         has returned.

  unsubscribe @6 (subscriberId :ClientId, paths :Paths) -> Void;
  # Cancel the given subscriptions. Wildcards in the paths are not expanded.
  #
  # It is not an error to pass a non-existing or a non-subscribed path to unsubscribe.
  #
  # Calling "unsubscribe" is not strictly required. It is perfectly safe to
  # destroy the client side StreamingHandle without calling unsubscribing first.
  # However, it may take a while for the HPK to notice that the client side of the
  # subscription has been dropped. This can cause surprising results when using
  # the subscribedonly flag in listNodes.
  #
  # The HPK does not forbid the existence of multiple subscriptions with the same
  # subscriberId and path. If multiple subscriptions exist with the same
  # subscriberId and path, they are all deleted. It's not possible at the moment
  # to selectively delete a single subscription.

  disconnectDevice @4 () -> Void;
  # disconnectDevice effectively causes the HPK to be torn down. By design,
  # disconnectDevice will never return a response, and requests will always
  # raise a DISCONNECTED exception.

  listNodesJson @5 (pathExpression :PathExpression, flags :UInt32, client :ClientId) -> (nodeProps :Text);
  # The "client" field is needed only when subscribedonly is used.

  # DEPRECATED
  # SetValue returns the acknowledged value as part of 1.2.0
  deprecatedSetValue @2 (path         :Path,
                         value        :Value,
                         completeWhen :ReturnFromSetWhen = deviceAck)
                     -> (result       :Result(Void, Error));
  # DEPRECATED
  # Server side path expansion was added in 1.5.0
  deprecatedSetValue2 @8 (path         :Path,
                          value        :Value,
                          completeWhen :ReturnFromSetWhen = deviceAck,
                          client :ClientId = .defaultClientId)
                      -> (result       :Result(AnnotatedValue, Error));
  deprecatedGetValues @1 (paths :Paths, client :ClientId = .defaultClientId) -> (result :List(Result(AnnotatedValue, Error)));
}

interface StreamingHandle @0xf51e28a7f5b41574 {
  sendValues @0 (values :List(AnnotatedValue)) -> stream;
  # Note: Capnp documentation reccommends that, when using streaming a "done()"
  # function is provided as well. This is necessary for the sending side to
  # ensure that the streaming was entirely successful. In the Session interface
  # however a done() call would not make sense, because the streaming goes on
  # "forever" and is fire and forget.
}

enum ReturnFromSetWhen @0xdd2da53aac55edf9 {
  # Return an answer as soon as the request has been forwarded to the device,
  # but before it has been acknowledged. This mode exists to mimick the MDK
  # default "set" behavior. Eventually, it should be deprecated.
  asap @0;

  # Wait until the device has responded to the set request before returning the
  # answer to the client. This is the default for the HPK
  deviceAck @1;

  unusedAsync @2;
  unusedTransactional @3;
}

enum LookupMode @0xda5049b5e072f425 {
  # The path expected to be a single node path.
  directLookup @0;
  # The server tries to resolve the path with listNodes internally.
  withExpansion @1;
}
