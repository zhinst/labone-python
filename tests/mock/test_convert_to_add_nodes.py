import pytest

from labone.mock.convert_to_add_nodes import NUMBER_PLACEHOLDER, DynamicNestedStructure, BuildSegment, list_nodes_info_to_get_nodes



# @pytest.mark.parametrize(("listed_nodes"), [
#     ["/a"],
#     ["/a/b/c"],
#     ["/a/b", "/a/c"],
# ])
def test_conversion():
    listed_nodes = ["/a/b", "/a/c"]
    conversion = list_nodes_info_to_get_nodes({path:{} for path in listed_nodes})
    print(conversion)

    root = conversion[0]
    assert root.name == "a"
    assert {"a","b","c"} == {segment.name for segment in conversion}


def test_conversion_range():
    listed_nodes = ["/a/0/b", "/a/1/b"]
    conversion = list_nodes_info_to_get_nodes({path:{} for path in listed_nodes})
    print(conversion)

    root = conversion[0]
    assert root.subNodes[0].name == NUMBER_PLACEHOLDER
    assert root.subNodes[0].range_end == 1