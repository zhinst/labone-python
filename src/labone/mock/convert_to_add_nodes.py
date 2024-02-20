from uuid import uuid4
from labone.node_info import NodeInfo
from labone.nodetree.helper import split_path

NUMBER_PLACEHOLDER = 'N'

class DynamicNestedStructure:
    def nest(self, name):
        sub = DynamicNestedStructure()
        self.__dict__[name] = sub
        return sub


class BuildSegment:
    """Cannot deal with ranges yet. Does not use info yet."""
    def __init__(self, name) -> None:
        self.id = uuid4()
        self.name = name
        self.sub_segments = {} # name -> BuildSegment
        self.range_end = 0

    def add(self, segments) -> None:
        next_segment = segments.pop(0)
        nr = 0
        if next_segment.isnumeric():
            nr = int(next_segment)
            next_segment = NUMBER_PLACEHOLDER

        if next_segment not in self.sub_segments:
            self.sub_segments[next_segment] = BuildSegment(next_segment)

        self.sub_segments[next_segment].range_end = max(nr, self.sub_segments[next_segment].range_end)
        if segments:
            self.sub_segments[next_segment].add(segments)

    def to_capnp(self):
        mock_capnp = DynamicNestedStructure()
        mock_capnp.id = self.id
        mock_capnp.name = self.name
        mock_capnp.rangeEnd = self.range_end
        mock_capnp.subNodes = [sub for name, sub in self.sub_segments.items()]

        info = mock_capnp.nest("info")
        info.description = "some description"
        info.properties = ['read', 'write', 'setting']
        info.type = "int64"
        info.unit = "None"

        result =  [mock_capnp] 
        for sub in self.sub_segments.values():
            result += sub.to_capnp()
        return result
        

def list_nodes_info_to_get_nodes(listed_nodes_info: dict[str, NodeInfo]):
    virtual_root = BuildSegment("virtual_root")
    for path, info in listed_nodes_info.items():
        virtual_root.add(split_path(path))

    result = []
    for sub_node in virtual_root.sub_segments.values():
        result += sub_node.to_capnp()
    return result