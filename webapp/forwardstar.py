"""
Provides a simple class which implements a forward star representation of any given tree structure.
The forward star is instantiated with the root element as a mandatory parameter. In this basic
implementation, the elements in the tree need to be unique.
"""

# ------------------
# Imports
# ------------------
from dataclasses import dataclass

from sqlalchemy import true

# Public symbols.
# __all__ = ["__version__", "__doc__"]

# Authoship information
__author__ = "Daniel Grass"
__copyright__ = "Copyright 2022, Daniel Grass"
__credits__ = "Rod Stephens"
__license__ = "GPL"
__version__ = "0.1.0"
__maintainer__ = "Daniel Grass"
__email__ = "dani.grass@bluewin.ch"
__status__ = "Development"


@dataclass
class ForwardStarData:

    # __slots__ = (
    #     "node_caption",
    #     "first_link",
    #     "to_node",
    #     "num_links",
    #     "num_nodes",
    #     "selected_node",
    # )

    node_caption: list
    first_link: list
    to_node: list
    num_links: int
    num_nodes: int
    selected_node: int


class ForwardStar:

    __slots__ = (
        "node_caption",
        "first_link",
        "to_node",
        "num_links",
        "num_nodes",
        "selected_node",
    )

    # ------------------
    # Standard Functions
    # ------------------
    def __init__(self, root_caption: str):
        self.node_caption = []
        self.first_link = []
        self.to_node = []
        self.num_links = 0
        self.num_nodes = 1
        self.selected_node = -1

        # root
        self.first_link.append(0)
        self.node_caption.append(root_caption)

        # sentinel
        self.first_link.append(0)

    # ------------------
    # Internal Functions
    # ------------------
    def _select_node_by_caption(self, node_caption: str) -> int:
        sel_node = [i for i, j in enumerate(self.node_caption) if j == node_caption]

        if len(sel_node) > 1:
            raise KeyError(
                f"parent node caption '{node_caption}' is not unique, unique node caption required"
            )
        else:
            return sel_node[0]

    def _add_link(self, from_node: int, to_node: int):
        # Create room for the new link
        self.num_links += 1
        self.to_node.append(-1)

        # Move the other links over to make room for the new one
        new_var_from = self.num_links - 1
        new_var_to = (
            self.first_link[from_node + 1] + 1 - 1
        )  # the last - 1 is needed as range works <from> inclusive to <to> exclusive
        for i in range(new_var_from, new_var_to, -1):
            self.to_node[i] = self.to_node[i - 1]

        # Insert the new link
        self.to_node[self.first_link[from_node + 1]] = to_node

        # Update the FirstLink entries
        var_from = from_node + 1
        var_to = (
            self.num_nodes + 1
        )  # + 1 is needed as range works <from> inclusive to <to> exclusive
        for i in range(var_from, var_to):
            self.first_link[i] = self.first_link[i] + 1

    def _new_node(self, caption: str) -> int:
        # new entry
        self.first_link.append(self.first_link[self.num_nodes])

        self.node_caption.append(caption)
        out = self.num_nodes
        self.num_nodes += 1
        return out

    # ------------------
    # Public Functions
    # ------------------
    def add_child(self, parent_node_caption: str, child_caption: str):

        # select the parent node
        self.selected_node = self._select_node_by_caption(parent_node_caption)
        # create the new node
        node = self._new_node(child_caption)
        # add the link from the parent to the new node
        self._add_link(self.selected_node, node)

    def find_parent_by_caption(self, node_caption: str):

        # get the node index
        node = self._select_node_by_caption(node_caption)
        return self.find_parent(node)

    def find_parent(self, node: int):

        # initialize parent and link as "we did not find a parent"
        parent = -1
        link = -1
        out = (parent, link)

        # Find the link into this node
        for parent in range(
            0, self.num_nodes
        ):  # the - 1 is non needed as range works <from> inclusive to <to> exclusive
            for link in range(
                self.first_link[parent], self.first_link[parent + 1]
            ):  # the - 1 is non needed as range works <from> inclusive to <to> exclusive
                if self.to_node[link] == node:
                    out = (parent, link)
                    break

        return out

    def find_node_by_caption(self, node_caption: str):
        # get the node index
        node = self._select_node_by_caption(node_caption)
        return node

    def get_tree(self) -> dict:
        tree_data = {}
        frist_link_caption = []
        to_node = []
        idx_bound = len(self.first_link) - 1

        for idx, elem in enumerate(self.first_link):
            if idx == idx_bound:
                frist_link_caption.append([idx, " ", self.first_link[idx]])
            else:
                frist_link_caption.append(
                    [idx, self.node_caption[idx], self.first_link[idx]]
                )
        tree_data["first_link_caption"] = frist_link_caption

        for idx, elem in enumerate(self.to_node):
            to_node.append([idx, elem])
        tree_data["to_node"] = to_node

        return tree_data

    def get_fstar_data(self) -> ForwardStarData:
        fstar_data = ForwardStarData(
            node_caption=self.node_caption,
            first_link=self.first_link,
            to_node=self.to_node,
            num_links=self.num_links,
            num_nodes=self.num_nodes,
            selected_node=self.selected_node,
        )

        return fstar_data

    def load_from_fstar_data(self, fstar_data: ForwardStarData):
        self.node_caption = fstar_data.node_caption
        self.first_link = fstar_data.first_link
        self.to_node = fstar_data.to_node
        self.num_links = fstar_data.num_links
        self.num_nodes = fstar_data.num_nodes
        self.selected_node = fstar_data.selected_node

    def debug_tree(self):
        print("Index", "Label", "FirstLink", sep="\t")
        idx_bound = len(self.first_link) - 1
        for idx, elem in enumerate(self.first_link):
            if idx == idx_bound:
                print(idx, " ", self.first_link[idx], sep="\t")
            else:
                print(idx, self.node_caption[idx], self.first_link[idx], sep="\t")

        print("")

        print("Index", "ToNode", sep="\t")
        for idx, elem in enumerate(self.to_node):
            print(idx, elem, sep="\t")

    def display_tree(self):
        self.display_node(0, 0)

    def display_node(self, node: int, parent: int, display_node_only=True):

        if node != parent:
            # display the node and parent
            print(f"{self.node_caption[parent]} -> {self.node_caption[node]}")
        else:
            if display_node_only:
                # display the node
                print(self.node_caption[node])

        # display the children
        for link in range(
            self.first_link[node], self.first_link[node + 1]
        ):  # -1 in <to> argument not needed as range works <from> inclusive to <to> exclusive
            self.display_node(self.to_node[link], node, display_node_only)

    def display_node_descendants(self, node_caption: str):
        node = self._select_node_by_caption(node_caption)
        self.display_node(node, node, False)

    def display_node_ancestors(self, node_caption: str):
        parent, link = self.find_parent_by_caption(node_caption=node_caption)
        if parent == -1:
            # print(f"{node_caption} is the root")
            pass
        else:
            # print(f"{self.node_caption[parent]} is the parent of {node_caption}")
            print(f"{self.node_caption[parent]} <- {node_caption}")
            self.display_node_ancestors(self.node_caption[parent])

    def visit_tree(self):
        yield from self.visit_node(0, 0)

    def visit_node_descendants(self, node_caption: str):
        node = self._select_node_by_caption(node_caption)
        yield from self.visit_node(node, node)

    def visit_node(self, node: int, parent: int):

        if node != parent:
            # return the node and parent
            yield (self.node_caption[parent], self.node_caption[node])

        # return the children
        for link in range(
            self.first_link[node], self.first_link[node + 1]
        ):  # -1 in <to> argument not needed as range works <from> inclusive to <to> exclusive
            yield from self.visit_node(self.to_node[link], node)

    def visit_node_ancestors(self, node_caption: str):
        parent, link = self.find_parent_by_caption(node_caption=node_caption)
        if parent == -1:
            # print(f"{node_caption} is the root")
            pass
        else:
            # print(f"{self.node_caption[parent]} is the parent of {node_caption}")
            yield (self.node_caption[parent], node_caption)
            yield from self.visit_node_ancestors(self.node_caption[parent])


def breadth_first():
    fstar = ForwardStar("A")

    fstar.add_child("A", "B")
    fstar.add_child("A", "C")

    fstar.add_child("B", "D")

    fstar.add_child("C", "E")
    fstar.add_child("C", "F")
    fstar.add_child("C", "G")
    fstar.add_child("C", "H")
    fstar.add_child("C", "I")
    fstar.add_child("C", "J")

    fstar.add_child("D", "K")
    fstar.add_child("D", "L")

    return fstar


def depth_first():
    fstar = ForwardStar("A")

    fstar.add_child("A", "B")
    fstar.add_child("B", "D")
    fstar.add_child("D", "K")

    fstar.add_child("K", "Y")
    fstar.add_child("K", "Z")

    fstar.add_child("D", "L")
    fstar.add_child("A", "C")
    fstar.add_child("C", "E")
    fstar.add_child("C", "F")
    fstar.add_child("C", "G")
    fstar.add_child("C", "H")
    fstar.add_child("C", "I")
    fstar.add_child("C", "J")

    return fstar


if __name__ == "__main__":

    print("breadth_first:")
    fwdstar = breadth_first()
    # fwdstar.display_tree()

    # print("** depth_first **")
    # fwdstar = depth_first()
    node_caption = "D"

    print(f"Descendants of {node_caption}:")
    fwdstar.display_node_descendants(node_caption)
    print(f"Descendants of {node_caption} from visit_node_descendants:")
    for descendant in fwdstar.visit_node_descendants(node_caption):
        print(descendant)

    print(f"Ancestors of {node_caption}:")
    fwdstar.display_node_ancestors(node_caption)
    print(f"Pedigree of {node_caption} from visit_node_ancestors:")
    pedigree = []
    for ancestor in fwdstar.visit_node_ancestors(node_caption):
        pedigree.append(ancestor)
    # pedigree = pedigree[::-1]
    for ancestor in pedigree:
        print(ancestor)

    print("Full tree from visit_tree:")
    for level in fwdstar.visit_tree():
        print(level)

    # fwdstar.display_tree()

    # children = ["A", "K", "D", "G", "C"]
    # for child in children:
    #     parent, link = fwdstar.find_parent_by_caption(child)
    #     if parent == -1:
    #         print(f"{child} is the root")
    #     else:
    #         print(f"{fwdstar.node_caption[parent]} is the parent of {child}")

    # fstar_data = fwdstar.get_tree()
    # print(fstar_data)

    # node = fwdstar.find_node_by_caption("B")
    # fwdstar.display_node(node, node)
    # fstar_data = fwdstar.get_fstar_data()
    # print(fstar_data)