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
        
        self.__verify_tree_recurse(self.expand_root(), leaf_predicate)
    
    def topological_sort(self) -> typing.Iterable :
        return TopologicalSorter(self.string_dict).static_order()
    
    def __get_root_node(self) -> str :
        return list(self.string_dict.keys())[0]
    
    def expand_root(self) -> typing.List[str] :
        return self.expand_node(self.__get_root_node())
    
    def expand_node(self, string : str) -> typing.List[str] :
        return self.string_dict[string]

    def __verify_tree_recurse(self, children : typing.List[str], leaf_predicate : typing.Callable) -> None :
        for child_name in children :
            if child_name in self.string_dict :
                self.__verify_tree_recurse(self.string_dict[child_name], leaf_predicate)
            else :
                assert leaf_predicate(child_name), f"Leaf node {child_name} is invalid!"
