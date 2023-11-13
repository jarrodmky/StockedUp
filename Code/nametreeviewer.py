import typing

from kivy.lang import Builder
from kivy.properties import ObjectProperty
from kivy.uix.scrollview import ScrollView

from PyJMy.debug import debug_message

from string_tree import StringTree

Builder.load_file("nametreeviewer.kv")

MakeInternalNodeCallable = typing.Callable[[str, bool], typing.Any]
MakeExternalNodeCallable = typing.Callable[[str], typing.Any]

class NameTreeViewer(ScrollView) :

    tree_view = ObjectProperty(None)

    def __init__(self, **kwargs) :
        super(NameTreeViewer, self).__init__(**kwargs)

    def init_tree_viewer(self, make_internal_fxn : MakeInternalNodeCallable, make_external_fxn : MakeExternalNodeCallable) -> None :
        self.tree_view.bind(minimum_height = self.tree_view.setter("height"))
        self.tree_view.disabled = True

        self.make_internal_node = make_internal_fxn
        self.make_external_node = make_external_fxn

    def __add_interior_node(self, name : str, is_active : bool, parent : typing.Any) -> typing.Any :
        return self.tree_view.add_node(self.make_internal_node(name, is_active), parent)

    def __add_leaf_node(self, name : str, parent : typing.Any) -> typing.Any :
        return self.tree_view.add_node(self.make_external_node(name), parent)

    def add_list(self, name : str, elements : typing.List[str]) -> typing.Any :
        self.tree_view.disabled = False
        base_node = self.__add_interior_node(name, True, None)
        for element in elements :
            self.__add_leaf_node(element, base_node)
        return base_node

    def add_tree(self, name : str, tree : StringTree) -> None :
        self.tree_view.disabled = False
        root_node = self.__add_interior_node(name, True, None) #ignores root node name from tree
        self.__add_tree_nodes_recursive(tree, root_node, tree.expand_root())

    def __add_tree_nodes_recursive(self, tree : StringTree, parent : typing.Any, children : typing.List[str]) -> None :
        for child_name in children :
            if child_name not in tree :
                self.__add_leaf_node(child_name, parent)
            else :
                internal_node = self.__add_interior_node(child_name, False, parent)
                self.__add_tree_nodes_recursive(tree, internal_node, tree.expand_node(child_name))

    def get_all_subtree_nodes(self, root_node : typing.Any) -> typing.Iterable :
        return self.tree_view.iterate_all_nodes_df(root_node)

    def get_visible_frontier_nodes(self) :
        debug_message(f"Finding node frontier:")
        for node in self.tree_view.iterate_visible_nodes_df() :
            if not node.is_open or node.is_leaf :
                debug_message(f"Name {node.name_entry.text} is on frontier")
                yield node
