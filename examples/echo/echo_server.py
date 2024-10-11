import asyncio

from echo_schema import get_schema_loader
from zhinst.comms import CapnpContext, DynamicStructBase

from labone.server.server import CapnpResult, CapnpServer, capnp_method


class EchoServer(CapnpServer):
    ECHO_SCHEMA_ID = 0x8F15BD68902CC51E

    def __init__(self):
        CapnpServer.__init__(self, schema=get_schema_loader())

    @capnp_method(ECHO_SCHEMA_ID, 0)
    async def echo(
        self,
        call_input: DynamicStructBase,
    ) -> CapnpResult:
        return {"response": "Hello " + call_input["request"]}


async def main():
    ctx = CapnpContext()
    server = EchoServer()
    # Start the server
    await server.start(8005, open_overwrite=False, context=ctx)
    # wait forever
    await asyncio.get_running_loop().create_future()


asyncio.run(main())
