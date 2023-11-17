import json
from pathlib import Path

from labone.nodetree import construct_nodetree
from labone.nodetree.node import Node, NodeTreeManager, PartialNode
from labone.sweeper.local_session import LocalSession


class Sweeper(PartialNode):

    # node on device -> rule how to set it (may be a function or a sweeper path)
    configure_device_rules = {("spectroscopy", "delay"): ("avg", "delay")}

    def __init__(self, *, session, model_node: Node):
        super().__init__(
            tree_manager=model_node.tree_manager,
            path_segments=model_node.path_segments,
            subtree_paths=model_node.subtree_paths,
            path_aliases=model_node.path_aliases,
        )
        self._server_session = session



    @classmethod
    async def create(cls, session):
        path_to_info = json.loads(
            Path.open(Path(__file__).parent / "sweeper_nodes.json").read()
        )

        model_node = await construct_nodetree(
            session=LocalSession(path_to_info),
            hide_kernel_prefix=False,
            use_enum_parser=False,
        )
        instance = cls(session=session, model_node=model_node)

        # setting the default values
        await instance.sweep.start_freq(-300e6)
        await instance.sweep.stop_freq(300e6)

        return instance
