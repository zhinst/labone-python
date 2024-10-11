import asyncio

from echo_schema import get_schema_loader
from zhinst.comms import CapnpContext


async def main():
    ctx = CapnpContext()
    client = await ctx.connect("127.0.0.1", 8005, schema=get_schema_loader())
    response = await client.reply(request="Anthony")
    print(response.response)


asyncio.run(main())
