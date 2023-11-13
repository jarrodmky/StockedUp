import typing
from graphlib import TopologicalSorter, CycleError

StringDict = typing.Dict[str, typing.List[str]]

class StringTree :

    def __init__(self, string_dict : StringDict, leaf_predicate : typing.Callable) :
        self.string_dict = string_dict
        #check for disconnected nodes and loops
        reachable_node_set = set()
        for children_keys in self.string_dict.values() :
            for child_key in children_keys :
                assert child_key not in reachable_node_set, f"Found duplicate child node \"{child_key}\""
                reachable_node_set.add(child_key)

        for subtree_key in list(self.string_dict.keys())[1:] :
            assert subtree_key in reachable_node_set, f"Found isolated node \"{subtree_key}\""
            
        #check no cycles
        try :
            test_sorter = TopologicalSorter(self.string_dict)
            test_sorter.prepare()
        except CycleError :
            assert False, "Cycle detected!"
        
        self.__verify_tree_recurse(self.string_dict[self.__get_root_node()], leaf_predicate)
    
    def topological_sort(self) -> typing.Iterable :
        return TopologicalSorter(self.string_dict).static_order()
    
    def __get_root_node(self) -> str :
        return list(self.string_dict.keys())[0]

    def __verify_tree_recurse(self, children : typing.List[str], leaf_predicate : typing.Callable) -> None :
        for child_name in children :
            if child_name in self.string_dict :
                self.__verify_tree_recurse(self.string_dict[child_name], leaf_predicate)
            else :
                assert leaf_predicate(child_name), f"Leaf node {child_name} is invalid!"

    def build_recursive_tree(self, build_root : typing.Callable, build_interior : typing.Callable, build_leaf : typing.Callable) :
        root_node = build_root()
        self.__add_tree_nodes_recursive(build_interior, build_leaf, root_node, self.string_dict[self.__get_root_node()])

    def __add_tree_nodes_recursive(self, build_interior : typing.Callable, build_leaf : typing.Callable, parent : typing.Any, children : typing.List[str]) -> None :
        for child_name in children :
            if child_name not in self.string_dict :
                build_leaf(child_name, parent)
            else :
                internal_node = build_interior(child_name, parent)
                self.__add_tree_nodes_recursive(build_interior, build_leaf, internal_node, self.string_dict[child_name])
