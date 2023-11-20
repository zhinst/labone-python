import asyncio
import typing as t


_T = t.TypeVar("_T")


class SweepResultQueue(asyncio.Queue, t.Generic[_T]):
    async def put(self, item: _T) -> None:
        return await super().put(item)

    async def get(self) -> _T:
        return await super().get()

    def empty(self) -> bool:
        return super().empty()
