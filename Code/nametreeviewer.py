import typing

from kivy.lang import Builder
from kivy.properties import ObjectProperty
from kivy.uix.scrollview import ScrollView

Builder.load_file("nametreeviewer.kv")

NameTree = typing.Dict[str, typing.List[str]]
MakeInternalNodeCallable = typing.Callable[[str, bool], typing.Any]
MakeExternalNodeCallable = typing.Callable[[str], typing.Any]

class NameTreeViewer(ScrollView) :

    tree_view = ObjectProperty(None)

    def __init__(self, **kwargs) :
        super(NameTreeViewer, self).__init__(**kwargs)

    def init_tree_viewer(self, make_internal_fxn : MakeInternalNodeCallable, make_external_fxn : MakeExternalNodeCallable) :
        self.tree_view.bind(minimum_height = self.tree_view.setter("height"))
        self.tree_view.disabled = True

        self.make_internal_node = make_internal_fxn
        self.make_external_node = make_external_fxn

    def add_internal_node(self, name, is_active, parent) :
        return self.tree_view.add_node(self.make_internal_node(name, is_active), parent)

    def add_external_node(self, name, parent) :
        return self.tree_view.add_node(self.make_external_node(name), parent)

    def add_list(self, name : str, elements : typing.List[str]) -> typing.Any :
        self.tree_view.disabled = False
        base_node = self.add_internal_node(name, True, None)
        for element in elements :
            self.add_external_node(element, base_node)
        return base_node

    def add_tree(self, name : str, tree : NameTree) -> None :
        self.tree_view.disabled = False
        root_name = next(iter(tree)) #ignores root node name in tree
        root_node = self.add_internal_node(name, True, None)
        self.__add_nodes_recursive(tree, root_node, tree[root_name])

    def __add_nodes_recursive(self, tree, parent, children) :
        for child_name in children :
            if child_name not in tree :
                self.add_external_node(child_name, parent)
            else :
                internal_node = self.add_internal_node(child_name, False, parent)
                self.__add_nodes_recursive(tree, internal_node, tree[child_name])
