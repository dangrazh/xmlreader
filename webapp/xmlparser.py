"""Provides an xml parser which 'flatens' a given xml document and returns a dictionary with all tags and values.
Alternatively, the parsed document can be printed as indented tree or flattened structure.
"""


# ------------------
# Imports
# ------------------
# import sys
from msilib.schema import Error
import os

# import errno
import datetime as dt
import tempfile
from typing import Any
from xmlrpc.client import Boolean

# import pathlib
# import getpass
# import re
import regex as re
import enum
from dataclasses import dataclass

# import pandas as pd

from bs4 import BeautifulSoup as bs

# import for usage in flask app
from webapp.forwardstar import ForwardStar, ForwardStarData

# import for standalone usage
# from forwardstar import ForwardStar

# from bs4.diagnose import diagnose


# Public symbols.
# __all__ = ["__version__", "__doc__"]

# Authoship information
__author__ = "Daniel Grass"
__copyright__ = "Copyright 2021, Daniel Grass"
# __credits__ = []
__license__ = "GPL"
__version__ = "1.1.0"
__maintainer__ = "Daniel Grass"
__email__ = "dani.grass@bluewin.ch"
__status__ = "Development"


# ------------------
# Classes
# ------------------


@dataclass
class Result:

    __slots__ = ("ok", "result")

    ok: Boolean
    result: Any


@dataclass
class TagAndValue:

    __slots__ = ("key", "value")

    key: str
    value: Any


class AttributeUsage(enum.Enum):
    add_to_tag_name = 1
    add_to_tag_value = 2
    add_separate_tag = 3
    ignore = 4


class TagType(int, enum.Enum):
    node: int = 0
    data_tag: int = 1


class XmlParser:

    __slots__ = (
        "doc_id",
        "doc_str",
        "soup",
        "tags",
        "top_node",
        "type",
        "source_no_of_tags",
        "soup_no_of_tags",
        "tag_id",
        "fstar",
    )

    # ------------------
    # Standard Functions
    # ------------------
    def __init__(
        self,
        document_string,
        top_node_tree_level=0,
        type_distance_to_top=1,
        document_id=0,
    ):

        # replace the ? in the XML Prolog as this caused
        # BeautifulSoup to return a completely broken object
        if "?" in document_string:
            re_pattern = re.compile(r"(<)(\?)(xml .*?)(\?)(>)", re.IGNORECASE)
            document_string = re_pattern.sub(r"\1\3\5", document_string)
            # document_string = document_string.replace(
            #     '<?xml version="1.0" encoding="ISO-8859-1"?>',
            #     '<xml version="1.0" encoding="ISO-8859-1">',
            # )

        # set the basic attributes
        self.doc_id = document_id
        self.doc_str = document_string
        self.soup = bs(document_string, "xml")
        self.tags = self._get_tags()
        self.top_node = self.soup(self.tags[top_node_tree_level])[0]
        try:
            doc_type_level = top_node_tree_level + type_distance_to_top
            self.type = self.soup(self.tags[doc_type_level])[0].name
            if str(self.type).lower() == "document":
                self.type = self.soup(self.tags[doc_type_level + 1])[0].name
        except IndexError as e:
            raise IndexError(
                f"Index out of range occurred while trying to access 'self.soup(self.tags[1])[0].name'\nInput string processed: {document_string}\nParsed result: {self.soup}\nTags identified: {self.tags}"
            )

        # do some basic checks post parsing
        self.source_no_of_tags = document_string.count("</")

        self.soup_no_of_tags = len(self.tags)
        if self.soup_no_of_tags < (0.5 * self.source_no_of_tags):
            raise ValueError(
                f"BeautifulSoup has recognized {self.soup_no_of_tags} tags whearas raw document contains {self.source_no_of_tags} tags"
            )

    # ------------------
    # Public Functions
    # ------------------
    def inspect_document_tree(self, attribute_usage=AttributeUsage.ignore):
        # starting_node = self.tags[0]
        self._traverse_document_tree(self.top_node, attribute_usage)

    def inspect_document_flat(self, attribute_usage=AttributeUsage.ignore):
        # starting_node = self.tags[0]
        # print(f'inspecting tag {starting_node.name} and desendants:')
        self._traverse_document_flat(self.top_node, attribute_usage)

    def get_tags_and_values(
        self, attribute_usage=AttributeUsage.add_to_tag_name, concat_on_key_error=False
    ):

        # # OLD: initialize the forward star - 0 is the static root element of the tree
        self.tag_id = 0
        # # parent_tag_id = 0
        # self.fstar = ForwardStar(self.tag_id)

        # NEW: initialize the forward star as None
        self.fstar = None

        doc_cont = {}
        try:
            self._process_document(
                self.top_node, doc_cont, attribute_usage, concat_on_key_error
            )
            return Result(ok=True, result=doc_cont)
        except Exception as e:
            return Result(ok=False, result=e)

        # return doc_cont

    # ------------------
    # Internal Functions
    # ------------------
    def _get_tags(self):
        soup_tags = [itm.name for itm in self.soup.descendants if itm.name]
        # for itm in self.soup.descendants:
        #     if itm.name:
        #         soup_tags.append(itm.name)

        return soup_tags

    def _traverse_document_tree(self, bs_elem, attribute_usage, level=0):

        level += 1
        spacing = "  " * level

        if str(type(bs_elem)) == "<class 'bs4.element.Tag'>":
            if bs_elem.name:
                if bs_elem.string and len(list(bs_elem.descendants)) == 1:
                    # Tag with value and no further descendants -> we are at the bottom of the tree, print both the tag and the value
                    if bs_elem.attrs:
                        print(
                            f"{level}{spacing}{bs_elem.name} ({bs_elem.attrs}) -> {bs_elem.string}"
                        )
                    else:
                        print(f"{level}{spacing}{bs_elem.name} -> {bs_elem.string}")
                else:
                    print(f"{level}{spacing}{bs_elem.name}")

                for child in bs_elem.children:
                    self._traverse_document_tree(child, attribute_usage, level)

    def _traverse_document_flat(self, bs_elem, attribute_usage, path="", level=0):

        if str(type(bs_elem)) == "<class 'bs4.element.Tag'>":
            if bs_elem.name:
                if bs_elem.string and len(list(bs_elem.descendants)) == 1:
                    # Tag with value and no further descendants -> we are at the bottom of the tree, print both the tag and the value

                    # old version expanding the attributes -> as this is to inspect the document, new version just displays
                    # the attributes as they are in the docuemnt
                    # attribs = ''
                    # if bs_elem.attrs:
                    #     for item in bs_elem.attrs:
                    #         attribs = attribs + '-' + bs_elem.attrs[item]
                    # print(f'{path}.{bs_elem.name}{attribs} -> {bs_elem.string}')

                    if bs_elem.attrs:
                        print(
                            f"{path}.{bs_elem.name} ({bs_elem.attrs}) -> {bs_elem.string}"
                        )
                    else:
                        print(f"{path}.{bs_elem.name} -> {bs_elem.string}")
                else:
                    if len(path) > 0:
                        path = path + "." + bs_elem.name
                    else:
                        path = bs_elem.name

                for child in bs_elem.children:
                    self._traverse_document_flat(child, attribute_usage, path, level)

    def _process_document(
        self,
        bs_elem,
        doc_cont,
        attribute_usage,
        concat_on_key_error,
        path="",
        level=0,
        parent_tag_id=0,
    ):
        level += 1
        if str(type(bs_elem)) == "<class 'bs4.element.Tag'>":
            if bs_elem.name:
                if bs_elem.string and len(list(bs_elem.descendants)) == 1:
                    # Tag with value and no further descendants -> we are at the bottom of the tree, print both the tag and the value
                    attrs = None
                    if bs_elem.attrs:
                        attrs = self._process_attrs(bs_elem, path, attribute_usage)
                        if isinstance(attrs, list):
                            # we have a list of attrs to turn into fields
                            for attr in attrs:
                                key = attr.key
                                value = attr.value
                                self._process_field(
                                    doc_cont,
                                    key,
                                    value,
                                    concat_on_key_error,
                                    level,
                                    parent_tag_id,
                                    TagType.data_tag,
                                )
                        else:
                            key = attrs.key
                            value = attrs.value
                            self._process_field(
                                doc_cont,
                                key,
                                value,
                                concat_on_key_error,
                                level,
                                parent_tag_id,
                                TagType.data_tag,
                            )
                    else:
                        key = f"{path}.{bs_elem.name}"
                        value = bs_elem.string
                        self._process_field(
                            doc_cont,
                            key,
                            value,
                            concat_on_key_error,
                            level,
                            parent_tag_id,
                            TagType.data_tag,
                        )
                else:
                    # Tag with further descendants
                    # TODO: check for attributes that would need to be processed

                    # build path
                    if len(path) > 0:
                        path = path + "." + bs_elem.name
                    else:
                        path = bs_elem.name

                    # add a "node only" field
                    key = path
                    value = "__node__"
                    self._process_field(
                        doc_cont,
                        key,
                        value,
                        concat_on_key_error,
                        level,
                        parent_tag_id,
                        TagType.node,
                    )

                    # disabled as already done in _process_field for "node only" field
                    # self.tag_id += 1
                    # self.fstar.add_child(parent_tag_id, self.tag_id)

                    # make the child the new parent
                    parent_tag_id = self.tag_id

                for child in bs_elem.children:
                    self._process_document(
                        child,
                        doc_cont,
                        attribute_usage,
                        concat_on_key_error,
                        path,
                        level,
                        parent_tag_id,
                    )

    def _process_attrs(self, bs_elem, path, attribute_usage):
        attribs = ""
        key = ""
        value = ""
        list_out = []
        ret_val = None

        if attribute_usage == AttributeUsage.add_to_tag_name:
            for item in bs_elem.attrs:
                attribs = attribs + "-" + bs_elem.attrs[item]
            key = f"{path}.{bs_elem.name}{attribs}"
            value = bs_elem.string
            ret_val = TagAndValue(key=key, value=value)

        elif attribute_usage == AttributeUsage.add_to_tag_value:
            for item in bs_elem.attrs:
                attribs = attribs + bs_elem.attrs[item] + "-"
            key = f"{path}.{bs_elem.name}"
            value = f"{attribs}{bs_elem.string}"
            ret_val = TagAndValue(key=key, value=value)

        elif attribute_usage == AttributeUsage.add_separate_tag:
            for item in bs_elem.attrs:
                # build a list of tags and values for each attribute
                key = f"{path}.{bs_elem.name}.{item}"
                value = bs_elem.attrs[item]
                list_out.append(TagAndValue(key=key, value=value))
            # and finally the tags value
            key = f"{path}.{bs_elem.name}.{bs_elem.name}"
            value = bs_elem.string
            list_out.append(TagAndValue(key=key, value=value))
            ret_val = list_out

        elif attribute_usage == AttributeUsage.ignore:
            key = f"{path}.{bs_elem.name}"
            value = bs_elem.string
            ret_val = TagAndValue(key=key, value=value)

        return ret_val

    def _process_field(
        self, doc_cont, key, value, concat_on_key_error, level, parent_tag_id, tag_type
    ):

        # icrease tag id and add tag to fstar
        self.tag_id += 1
        if self.fstar:
            self.fstar.add_child(parent_tag_id, self.tag_id)
        else:
            self.fstar = ForwardStar(self.tag_id)

        # process the key / value pair
        if key in doc_cont:
            if concat_on_key_error:
                old_value = doc_cont[key]
                # doc_cont[key] = old_value + " | " + value
                if isinstance(old_value, list):
                    # this creates a list of tag id, depth, value triplets in the following form: [(tagid, depth, value, tag_type), (tagid, depth, value, tag_type), (tagid, depth, value, tag_type)]
                    value_list = old_value
                    value_list.append((self.tag_id, level, value, tag_type))
                # elif isinstance(old_value, list) and len(old_value) == 1:
                #     # this is a single tag id / value pair (which are a list of 2 items)
                #     value_list = []
                #     value_list.append(old_value)
                #     value_list.append([self.tag_id, value])
                else:
                    print("I never pass here...")
                doc_cont[key] = value_list
            else:
                raise KeyError(
                    f"Trying to append the tag {key} which already exists in document! "
                    f"Current tag value: {value} "
                    f"Existing tag value: {doc_cont[key]}"
                )
        else:
            value_list = []
            # this creates a single tag id, depth, value triplet in the following form: [(tagid, depth, value)]
            value_list.append((self.tag_id, level, value, tag_type))
            doc_cont[key] = value_list


# -------------------------------------------------------------------------------
# The script entry point
# -------------------------------------------------------------------------------

if __name__ == "__main__":

    pass