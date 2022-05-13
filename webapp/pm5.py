"""Provides a data persistence object that stores the data extracted with a FileProcessor object. 
"""

# ------------------
# Imports
# ------------------
from ast import For
import os
import sqlite3
from copy import deepcopy
from typing import Optional, List, Set
import uuid
import json
from xmlrpc.client import Boolean

from numpy import isin, record

# USAGE = "standalone"
USAGE = "flask"

# import for usage in flask app
if USAGE == "flask":
    from webapp.xmlparser import XmlParser, AttributeUsage, TagType
    from webapp.forwardstar import ForwardStar, ForwardStarData
    from webapp.profiler import profile

# # import for standalone usage
# if USAGE == "standalone":
#     from xmlparser import XmlParser, AttributeUsage


# Public symbols.
# __all__ = ["__version__", "__doc__"]

# Authoship information
__author__ = "Daniel Grass"
__copyright__ = "Copyright 2021, Daniel Grass"
__credits__ = ()
__license__ = "GPL"
__version__ = "0.1.0"
__maintainer__ = "Daniel Grass"
__email__ = "dani.grass@bluewin.ch"
__status__ = "Development"


class IndexGroup:
    PROCESS_LOG = 0
    DOC_STORE = 1
    XML_STORE = 2


class LogLevel:
    INFO = 0
    WARNING = 1
    ERROR = 2
    ALL = 4


class DocValidity:
    VALID = 1
    INVALID = 0


class XmlAttribute:
    DocID = "DocID"
    Type = "Type"
    ParsedXml = "ParsedXml"
    SoupNoOfTags = "SoupNoOfTags"
    SourceNoOfTags = "SourceNoOfTags"
    Tags = "Tags"
    TagsAndValues = "TagsAndValues"
    TopNode = "TopNode"


class PersistenceManager:

    # ------------------
    # Standard Functions
    # ------------------
    def __init__(self, db_name=None, data_directory=None):
        self.conn = None
        # self.c = None

        if db_name:
            self.db_name = db_name
            self._connect_db()
        else:
            db_id = uuid.uuid1()
            if data_directory:
                self.db_file_path = data_directory
                self.db_name = os.path.join(data_directory, f"{db_id}.db")
            else:
                self.db_name = f"{db_id}.db"
            self._connect_db()
            self._create_tables()

        # initialize the cache
        self.cache = self._init_cache()

    def __del__(self):
        # make sure all data in commited before object is deleted
        self.commit_writes()

    def __str__(self):
        out_string = f"db_file: {self.db_name}\n" f"file_path: {self.file_path}"
        return out_string

    def __repr__(self):
        return self.__str__()

    # ------------------
    # Custom Pickling Functions
    # ------------------

    def __getstate__(self):
        # Copy the object's state from self.__dict__ which contains
        # all our instance attributes. Always use the dict.copy()
        # method to avoid modifying the original state.
        state = self.__dict__.copy()
        # Remove the unpicklable entries, i.e. the cursor 'c' and the
        # connection 'conn' objecgt
        # del state["c"]
        del state["conn"]
        return state

    def __setstate__(self, state):
        # Restore instance attributes (i.e., reconnect to the db).
        self.__dict__.update(state)
        # Restore the connection and cursor.
        self._connect_db()

    # ------------------
    # Public Functions
    # ------------------

    # Process log Management
    def log_error(self, doc_id: int, log_entry_text: str):
        self._add_to_cache("ProcessLog", (doc_id, LogLevel.ERROR, log_entry_text))

    def log_success(self, doc_id: int, log_entry_text: str):
        self._add_to_cache("ProcessLog", (doc_id, LogLevel.INFO, log_entry_text))

    def log_warning(self, doc_id: int, log_entry_text: str):
        self._add_to_cache("ProcessLog", (doc_id, LogLevel.WARNING, log_entry_text))

    def get_process_log(self, log_level: LogLevel, doc_id: int = None):
        if log_level == LogLevel.ALL:
            if doc_id:
                curr = self.conn.execute(
                    "SELECT * FROM ProcessLog where DocID=:doc_id",
                    {"doc_id": doc_id},
                )
                return curr.fetchone()
            else:
                curr = self.conn.execute("SELECT * FROM ProcessLog")
        else:
            if doc_id:
                curr = self.conn.execute(
                    "SELECT * FROM ProcessLog where LogLevel=:log_level and DocID=:doc_id",
                    {
                        "log_level": log_level,
                        "doc_id": doc_id,
                    },
                )
                return curr.fetchone()
            else:
                curr = self.conn.execute(
                    "SELECT * FROM ProcessLog where LogLevel=:log_level",
                    {"log_level": log_level},
                )
        for row in curr:
            yield row

    def get_process_log_count(self):
        curr = self.conn.execute("SELECT COUNT(*) AS NoOfLogs FROM ProcessLog")
        row = curr.fetchone()
        return row["NoOfLogs"]

    def truncate_process_log(self):
        with self.conn:
            self.conn.execute(
                "DELETE FROM ProcessLog",
            )

    # Document Management
    def store_doc(
        self,
        doc_id: int,
        doc_validity: DocValidity,
        doc_text: str,
        doc_invalid_reason: str = "",
    ):
        # "INSERT INTO DocList VALUES (:id, :type, :text, :reason)"
        self._add_to_cache(
            "DocList", (doc_id, doc_validity, doc_text, doc_invalid_reason)
        )

    def get_single_doc(self, doc_id: int):
        curr = self.conn.execute(
            "SELECT * FROM DocList where DocID=:id", {"id": doc_id}
        )
        return curr.fetchone()

    def get_all_docs(self, doc_validity: DocValidity = None):
        if doc_validity:
            curr = self.conn.execute(
                "SELECT * FROM DocList where DocValidity=:doc_validity",
                {"doc_validity": doc_validity},
            )
        else:
            curr = self.conn.execute("SELECT * FROM DocList")
        for row in curr:
            yield row

    def get_doc_count(self, doc_validity: DocValidity = None):
        if doc_validity:
            curr = self.conn.execute(
                "SELECT COUNT(*) AS NoOfDocs FROM DocList where DocValidity=:doc_validity",
                {"doc_validity": doc_validity},
            )
        else:
            curr = self.conn.execute("SELECT COUNT(*) AS NoOfDocs FROM DocList")
        row = curr.fetchone()
        return row["NoOfDocs"]

    def truncate_doc_store(self):
        with self.conn:
            self.conn.execute(
                "DELETE FROM DocList",
            )

    # Parsed XML Info Management
    def store_xml_parsed(self, doc_id: int, tags_and_values: dict, xml_obj: XmlParser):

        # get the meta data
        soup_no_of_tags = xml_obj.soup_no_of_tags
        source_no_of_tags = xml_obj.source_no_of_tags
        type = xml_obj.type
        # top_node = str(xml_obj.top_node)
        # tags = json.dumps(obj=xml_obj.tags)
        top_node = ""
        tags = ""
        tags_and_value = json.dumps(obj=tags_and_values)

        # cache the information
        # "INSERT INTO ParsedXmlStore VALUES (:DocID, :Type, :ParsedXml, :SoupNoOfTags, :SourceNoOfTags, :Tags, :TopNode)"
        self._add_to_cache(
            "ParsedXmlStore",
            (
                doc_id,
                type,
                tags_and_value,
                soup_no_of_tags,
                source_no_of_tags,
                tags,
                top_node,
            ),
        )

        # store the information
        # store all tag and value pairs
        tag_idx = 0
        for tag, value in tags_and_values.items():
            tag_idx += 1

            if isinstance(value, list) and len(value) > 1:
                # this is a list with tag id, depth, value triplets in the following form: [[tagid, depth, value, tag_type], [tagid, depth, value, tag_type], [tagid, depth, value, tag_type], ...]
                for idx, itm in enumerate(value):
                    tag_id = itm[0]
                    depth = itm[1]
                    tag_value = itm[2]
                    tag_type = itm[3]

                    # cache the information
                    # "INSERT INTO XmlTagsAndValues VALUES (:DocID, :Type, :TagOrder, :Tag, :TagType, :TagDepth, :TagID, :RepNo, :Value)"
                    self._add_to_cache(
                        "XmlTagsAndValues",
                        (
                            doc_id,
                            type,
                            tag_idx,
                            tag,
                            tag_type,
                            depth,
                            tag_id,
                            idx,
                            tag_value,
                        ),
                    )

            else:
                # this is a single tag id, depth, value triplet in the following form: [[tagid, depth, value, tag_type]]
                tag_id = value[0][0]
                depth = value[0][1]
                tag_value = value[0][2]
                tag_type = value[0][3]

                # cache the information
                # "INSERT INTO XmlTagsAndValues VALUES (:DocID, :Type, :TagOrder, :Tag, :TagType, :TagDepth, :TagID, :RepNo, :Value)"
                self._add_to_cache(
                    "XmlTagsAndValues",
                    (doc_id, type, tag_idx, tag, tag_type, depth, tag_id, 0, tag_value),
                )

        # store the forward star of the xml document
        self._store_fstar_data(doc_id, xml_obj)

    def get_xml_doc_parsed(
        self, doc_id: int, attribute: XmlAttribute, data_tags_only: Boolean = False
    ):

        str_select = f"SELECT {attribute} FROM ParsedXmlStore where DocID=:id"
        curr = self.conn.execute(str_select, {"id": doc_id})
        xml_parsed = curr.fetchone()
        xml_ret = xml_parsed[0]
        # print(xml_ret)

        if attribute == XmlAttribute.Tags:
            output = json.loads(xml_ret)
        elif attribute == XmlAttribute.ParsedXml:
            dict_to_process = json.loads(xml_ret)
            output = self._replace_lists(dict_to_process, data_tags_only)
        else:
            output = xml_ret

        return output

    def get_xml_types(self):
        curr = self.conn.execute(
            "SELECT DISTINCT Type FROM ParsedXmlStore ORDER BY DocID"
        )

        for row in curr:
            yield row["Type"]

    def get_xml_doc_id_by_type(self, doc_type: str):
        sql_sel = f"SELECT Type, DocID FROM ParsedXmlStore WHERE Type=:doc_type ORDER BY DocID"

        curr = self.conn.execute(
            sql_sel,
            {"doc_type": doc_type},
        )

        for row in curr:
            yield row

    def get_single_xml_type_stats(self, doc_type: str):

        sql_cre_tmp_tbl = f"CREATE TEMP TABLE temp_{doc_type}_xmlTypeStats AS SELECT TagOrder, Tag, DocID, max(TagDepth) as maxDepth, max(TagRepetition) AS maxRepetition FROM XmlTagsAndValues WHERE Type=:doc_type  AND TagType=:tag_type GROUP BY TagOrder, Tag, DocID"
        sql_sel_result = f"SELECT Tag, max(maxDepth) as maxDepth, max(maxRepetition) AS maxRep, min(maxRepetition) AS minRep, avg(maxRepetition) AS avgRep FROM temp_{doc_type}_xmlTypeStats GROUP BY TagOrder ORDER BY TagOrder"

        self.conn.execute(
            sql_cre_tmp_tbl,
            {"doc_type": doc_type, "tag_type": TagType.data_tag},
        )
        curr = self.conn.execute(sql_sel_result)

        for row in curr:
            yield row

    def get_xml_types_overview(self):
        curr = self.conn.execute(
            "SELECT Type, count(DocID) AS NoOfDocs FROM ParsedXmlStore GROUP BY Type"
        )
        for row in curr:
            yield row

    def get_xml_tags_by_type(self, doc_type: str):
        curr = self.conn.execute(
            "SELECT DISTINCT Tag FROM XmlTagsAndValues WHERE Type=:doc_type AND TagType=:tag_type ORDER BY TagOrder",
            {"doc_type": doc_type, "tag_type": TagType.data_tag},
        )
        for row in curr:
            yield row

    def get_xml_tag_stats_by_doc_id(self, doc_id: int):
        curr = self.conn.execute(
            "SELECT Tag, MAX(TagDepth), MAX(TagRepetition) FROM XmlTagsAndValues WHERE DocID=:doc_id GROUP BY Tag",
            {"doc_id": doc_id},
        )
        for row in curr:
            yield row

    def get_xml_tags_and_values_by_doc_id(self, doc_id: int):
        curr = self.conn.execute(
            "SELECT TagID, Tag, TagType, Value FROM XmlTagsAndValues WHERE DocID=:doc_id order by TagID;",
            {"doc_id": doc_id},
        )
        for row in curr:
            yield row

    def get_xml_tags_and_values_repetitive_by_doc_id(
        self, doc_id: int, create_record_on: Set[str]
    ):
        fields = [f"'{field}'" for field in create_record_on]
        str_fields = ", ".join(fields)
        sql = f"SELECT TagID, Tag, TagType, TagRepetition, Value FROM XmlTagsAndValues WHERE DocID=:doc_id AND Tag IN({str_fields}) order by TagRepetition, TagID;"
        curr = self.conn.execute(
            sql,
            {"doc_id": doc_id},
        )
        for row in curr:
            yield row

    def get_xml_tags_and_values_non_repetitive_by_doc_id(
        self, doc_id: int, create_record_on: Set[str]
    ):
        fields = [f"'{field}'" for field in create_record_on]
        str_fields = ", ".join(fields)
        sql = f"SELECT TagID, Tag, TagType, TagRepetition, Value FROM XmlTagsAndValues WHERE DocID=:doc_id AND Tag NOT IN({str_fields}) order by TagRepetition, TagID;"
        curr = self.conn.execute(
            sql,
            {"doc_id": doc_id},
        )
        for row in curr:
            yield row

    # @profile
    def create_output_by_xml_type(
        self, doc_type: str, create_record_on: Optional[Set[str]] = None
    ):
        # get the headers and create the fieldlist
        headers = [
            f"[{row['Tag']}] text, " for row in self.get_xml_tags_by_type(doc_type)
        ]
        s = ""
        fieldlist = s.join(headers)
        fieldlist = fieldlist[:-2]

        # create table with headers
        # print(f"drop if exists with subsequent create table for: {doc_type}FinalOutput")
        sql_del_tbl = f"DROP TABLE IF EXISTS {doc_type}FinalOutput"
        sql_cre_tbl = f"CREATE TABLE {doc_type}FinalOutput ({fieldlist})"
        # print(sql_cre_tbl)
        self.conn.execute(sql_del_tbl)
        self.conn.execute(sql_cre_tbl)

        # process the documents
        if not create_record_on:
            for type, id in self.get_xml_doc_id_by_type(doc_type):
                # simply process each document without taking care of any repetitive elements
                tags_n_values = self.get_xml_doc_parsed(
                    id, XmlAttribute.ParsedXml, True
                )
                # # replace the record structure with simple key/value pairs
                # for tag, record in tags_n_values.items():
                #     tags_n_values[tag] = record[2]
                self._create_single_record_output(
                    tags_n_values=tags_n_values, type=type
                )
        else:
            # create a new forward star object
            fstar = ForwardStar("empty")

            # process the documents
            for type, id in self.get_xml_doc_id_by_type(doc_type):
                # build the fstar
                fstar_data = self._get_fstar_data(doc_id=id)
                fstar.load_from_fstar_data(fstar_data)

                # build tags and value dict
                tags_and_values = {}
                for (
                    tag_id,
                    tag,
                    tag_type,
                    value,
                ) in self.get_xml_tags_and_values_by_doc_id(doc_id=id):
                    tags_and_values[tag_id] = (tag, tag_type, value)

                # build the repetitive tags table
                # TagID, Tag, TagRepetition, Value
                rep_tags_and_values = {}
                for (
                    r_tag_id,
                    r_tag,
                    r_tag_type,
                    r_tag_rep,
                    r_value,
                ) in self.get_xml_tags_and_values_repetitive_by_doc_id(
                    doc_id=id, create_record_on=create_record_on
                ):
                    rep_tags_and_values[r_tag_id] = (
                        r_tag,
                        r_tag_type,
                        r_tag_rep,
                        r_value,
                    )

                # build the non-repetitive tags table
                non_rep_tags_and_values = {}
                for (
                    nr_tag_id,
                    nr_tag,
                    nr_tag_type,
                    nr_tag_rep,
                    nr_value,
                ) in self.get_xml_tags_and_values_non_repetitive_by_doc_id(
                    doc_id=id, create_record_on=create_record_on
                ):
                    non_rep_tags_and_values[nr_tag_id] = (
                        nr_tag,
                        nr_tag_type,
                        nr_tag_rep,
                        nr_value,
                    )

                # build the empty target record structure
                target_record = {}
                tags = self.get_xml_doc_parsed(id, XmlAttribute.ParsedXml, True)
                for t_tag in tags:
                    target_record[t_tag] = ""

                # process each document while creating new records for fields provided
                # based on the list of repetitive tags
                r_tag_rep_prev = 0
                first_run = True
                records = []
                curr_record = deepcopy(target_record)

                # process the repetitive tags
                for r_tag_id, r_tag_record in rep_tags_and_values.items():

                    # record structure of r_tag_record: (tag, tag_type, tag_rep, value)
                    r_tag, _, r_tag_rep, r_value = r_tag_record

                    # Step 1 - enrich all the non repetitive tags based on the
                    # frist repetitive tag
                    if first_run:
                        first_run = False
                        curr_record = self._get_non_repetitive_tags(
                            record=curr_record,
                            fstar=fstar,
                            start_tag_id=r_tag_id,
                            tags_and_values=tags_and_values,
                            nr_tags_n_values=non_rep_tags_and_values,
                            r_tags_n_values=rep_tags_and_values,
                        )

                    # Step 2 - if we have moved to the next repetition,
                    # save the current record and initiate a new record
                    if r_tag_rep != r_tag_rep_prev:
                        records.append(curr_record)
                        r_tag_rep_prev = r_tag_rep
                        curr_record = deepcopy(target_record)
                        first_run = True

                    # Setp 3 - add the tag and it's value
                    curr_record[r_tag] = r_value

                # save the last record
                records.append(curr_record)

                # create the output for the records
                for curr_record in records:
                    self._create_single_record_output(
                        tags_n_values=curr_record, type=type
                    )

    def get_output_by_xml_type(self, doc_type: str):

        sql = f"SELECT * FROM {doc_type}FinalOutput"
        curr = self.conn.execute(sql)

        for row in curr:
            yield row

    # general database Management
    def remove_db_file(self):
        os.remove(self.db_name)

    def commit_writes(self):

        for table_name, values in self.cache.items():
            # only process lists that actually do contain data
            if len(values["Data"]) > 0:
                # get the number of elements in the 1st tuple
                field_list = ", ".join(values["Fields"])
                no_elems = len(values["Data"][0])
                params_string = ", ".join("?" for _ in range(0, no_elems))
                sql_ins = (
                    f"INSERT INTO {table_name} ({field_list}) VALUES ({params_string})"
                )
                with self.conn:
                    self.conn.executemany(sql_ins, values["Data"])

        self._clear_chache()

    def truncate_doc_store(self):
        with self.conn:
            self.conn.execute(
                "DELETE FROM DocList",
            )

    def truncate_xml_store(self):
        with self.conn:
            self.conn.execute(
                "DELETE FROM ParsedXmlStore",
            )
            self.conn.execute(
                "DELETE FROM XmlTagsAndValues",
            )
            # self.conn.execute(
            #     "DELETE FROM XmlFStarFirstLink",
            # )
            # self.conn.execute(
            #     "DELETE FROM XmlFStarToNode",
            # )
            # self.conn.execute(
            #     "DELETE FROM XmlFStarNodeCaption",
            # )
            self.conn.execute(
                "DELETE FROM XmlFStarAttributes",
            )

    def create_indices(self, index_group: IndexGroup):

        if index_group == IndexGroup.PROCESS_LOG:
            # the ProcessLog index
            with self.conn:
                self.conn.execute(
                    """CREATE INDEX IdxProcessLogDocID
                    ON ProcessLog (DocID)"""
                )

                self.conn.execute(
                    """CREATE INDEX IdxProcessLogLogLevel 
                    ON ProcessLog (LogLevel)"""
                )

        if index_group == IndexGroup.DOC_STORE:
            # the DocList indices
            with self.conn:
                self.conn.execute(
                    """CREATE UNIQUE INDEX IdxDocListDocID
                    ON DocList (DocID)"""
                )

                self.conn.execute(
                    """CREATE INDEX IdxDocListDocValidity
                    ON DocList (DocValidity)"""
                )

        if index_group == IndexGroup.XML_STORE:
            # the ParsedXmlStore indices
            with self.conn:
                self.conn.execute(
                    """CREATE UNIQUE INDEX IdxParsedXmlStoreDocID
                    ON ParsedXmlStore (DocID)"""
                )

                self.conn.execute(
                    """CREATE INDEX IdxParsedXmlStoreType
                    ON ParsedXmlStore (Type)"""
                )

                # the XmlTagsAndValues indices
                self.conn.execute(
                    """CREATE INDEX IdxXmlTagsAndValuesDocID
                    ON XmlTagsAndValues (DocID)"""
                )

                self.conn.execute(
                    """CREATE INDEX IdxXmlTagsAndValuesTag
                    ON XmlTagsAndValues (Tag)"""
                )

                self.conn.execute(
                    """CREATE INDEX IdxXmlTagsAndValuesType
                    ON XmlTagsAndValues (Type)"""
                )

                # the XmlFStarAttributes index
                self.conn.execute(
                    """CREATE INDEX IdxXmlFStarAttributes
                    ON XmlFStarAttributes (DocID)"""
                )

    def drop_indices(self, index_group: IndexGroup):

        if index_group == IndexGroup.PROCESS_LOG:
            # the ProcessLog index
            with self.conn:
                self.conn.execute("""DROP INDEX IF EXISTS IdxProcessLogDocID""")

                self.conn.execute("""DROP INDEX IF EXISTS IdxProcessLogLogLevel""")

        if index_group == IndexGroup.DOC_STORE:
            # the DocList indices
            with self.conn:
                self.conn.execute("""DROP INDEX IF EXISTS IdxDocListDocID""")

                self.conn.execute("""DROP INDEX IF EXISTS IdxDocListDocValidity""")

        if index_group == IndexGroup.XML_STORE:
            # the ParsedXmlStore indices
            with self.conn:
                self.conn.execute("""DROP INDEX IF EXISTS IdxParsedXmlStoreDocID""")

                self.conn.execute("""DROP INDEX IF EXISTS IdxParsedXmlStoreType""")

                # the XmlTagsAndValues indices
                self.conn.execute("""DROP INDEX IF EXISTS IdxXmlTagsAndValuesDocID""")

                self.conn.execute("""DROP INDEX IF EXISTS IdxXmlTagsAndValuesTag""")

                self.conn.execute("""DROP INDEX IF EXISTS IdxXmlTagsAndValuesType""")

                # the XmlFStarAttributes index
                self.conn.execute("""DROP INDEX IF EXISTS IdxXmlFStarAttributes""")

    # ------------------
    # Internal Functions
    # ------------------

    def _connect_db(self):
        self.conn = sqlite3.connect(self.db_name, isolation_level="DEFERRED")
        self.conn.execute("PRAGMA synchronous = OFF")
        self.conn.execute("PRAGMA journal_mode = OFF")
        self.conn.row_factory = sqlite3.Row

        # self.c = self.conn.cursor()

    def _disconnect_db(self):
        # self.c = None
        self.conn.close()
        self.conn = None

    def _init_cache(self):
        cache = {}
        cache["ProcessLog"] = {
            "Data": [],
            "Fields": ["DocID", "LogLevel", "LogEntry"],
        }
        cache["DocList"] = {
            "Data": [],
            "Fields": ["DocID", "DocValidity", "DocText", "DocInvalidReason"],
        }
        cache["ParsedXmlStore"] = {
            "Data": [],
            "Fields": [
                "DocID",
                "Type",
                "ParsedXml",
                "SoupNoOfTags",
                "SourceNoOfTags",
                "Tags",
                "TopNode",
            ],
        }
        cache["XmlTagsAndValues"] = {
            "Data": [],
            "Fields": [
                "DocID",
                "Type",
                "TagOrder",
                "Tag",
                "TagType",
                "TagDepth",
                "TagID",
                "TagRepetition",
                "Value",
            ],
        }
        cache["XmlFStarFirstLink"] = {
            "Data": [],
            "Fields": ["DocID", "FSIndex", "FristLink"],
        }
        cache["XmlFStarToNode"] = {
            "Data": [],
            "Fields": ["DocID", "FSIndex", "ToNode"],
        }
        cache["XmlFStarNodeCaption"] = {
            "Data": [],
            "Fields": ["DocID", "FSIndex", "NodeCaptionTagID"],
        }
        cache["XmlFStarAttributes"] = {
            "Data": [],
            "Fields": [
                "DocID",
                "NumLinks",
                "NumNodes",
                "SelectedNode",
                "FristLink",
                "ToNode",
                "NodeCaption",
            ],
        }

        return cache

    def _create_tables(self):
        with self.conn:
            # the ProcessLog table
            self.conn.execute(
                """CREATE TABLE ProcessLog (
                            DocID integer,
                            LogLevel integer,
                            LogEntry text
                            )"""
            )

            # the DocList table
            self.conn.execute(
                """CREATE TABLE DocList (
                            DocID integer,
                            DocValidity integer,
                            DocText text,
                            DocInvalidReason text
                            )"""
            )

            # the ParsedXmlStore table
            self.conn.execute(
                """CREATE TABLE ParsedXmlStore (
                            DocID integer,
                            Type text,
                            ParsedXml text,
                            SoupNoOfTags integer,
                            SourceNoOfTags integer,
                            Tags text,
                            TopNode text
                            )"""
            )

            # the XmlTagsAndValues table
            self.conn.execute(
                """CREATE TABLE XmlTagsAndValues (
                            DocID integer,
                            Type text,
                            TagOrder integer,
                            Tag text,
                            TagType integer,
                            TagDepth integer,
                            TagID integer,
                            TagRepetition integer,
                            Value text
                            )"""
            )

            # the XmlFStarAttributes table
            self.conn.execute(
                """CREATE TABLE XmlFStarAttributes (
                            DocID integer,
                            NumLinks integer,
                            NumNodes integer,
                            SelectedNode integer,
                            FristLink text,
                            ToNode text,
                            NodeCaption text
                            )"""
            )

    def _replace_lists(self, dict_in: dict, data_tags_only: Boolean = False):

        separator = " | "
        dict_tmp = {}
        for key in dict_in:
            # check if the item is a node
            if isinstance(dict_in[key], list):
                if isinstance(dict_in[key][0], list):
                    # this is a list containing tag id, depth, value, tag_type triplets
                    # get the value which is at position 2 (the 3rd position)
                    if dict_in[key][0][3] == TagType.node:
                        if data_tags_only:
                            # this is a list of nodes - don't transfer to temp dict
                            pass
                        else:
                            # this is list of nodes - keep it but blank out value
                            # dict_in[key] = ""
                            dict_tmp[key] = ""
                    else:
                        # lst_tmp = []
                        # for pair in dict_in[key]:
                        #     lst_tmp.append(pair[2])
                        lst_tmp = [pair[2] for pair in dict_in[key]]
                        # dict_in[key] = separator.join(lst_tmp)
                        dict_tmp[key] = separator.join(lst_tmp)
                else:
                    # this is a single value containing a tag id, depth, value, tag_type triplet
                    # get the value which is at position 2 (the 3rd position)
                    if dict_in[key][3] == TagType.node:
                        # this is a node - blank out value
                        # dict_in[key] = ""
                        dict_tmp[key] = ""
                    else:
                        # dict_in[key] = dict_in[key][2]
                        dict_tmp[key] = dict_in[key][2]
        # return dict_in
        return dict_tmp

    def _store_fstar_data(self, doc_id: int, xml_obj: XmlParser):

        # get the fstar from xml_obj
        fstar_data = xml_obj.fstar.get_fstar_data()

        # the frist link list
        first_link = json.dumps(obj=fstar_data.first_link)

        # the to node list
        to_node = json.dumps(obj=fstar_data.to_node)

        # the node caption list
        node_caption = json.dumps(obj=fstar_data.node_caption)

        # store the all lists and attributes in the attributes table
        self._add_to_cache(
            "XmlFStarAttributes",
            (
                doc_id,
                fstar_data.num_links,
                fstar_data.num_nodes,
                fstar_data.selected_node,
                first_link,
                to_node,
                node_caption,
            ),
        )

    def _get_fstar_data(self, doc_id: int) -> ForwardStarData:

        # node_caption = []
        # first_link = []
        # to_node = []
        num_links = 0
        num_nodes = 0
        selected_node = -1

        # get the attributes
        curr = self.conn.execute(
            "SELECT NumLinks, NumNodes, SelectedNode, FristLink, ToNode, NodeCaption FROM XmlFStarAttributes WHERE DocID=:doc_id",
            {"doc_id": doc_id},
        )
        row = curr.fetchone()
        first_link = json.loads(row["FristLink"])
        to_node = json.loads(row["ToNode"])
        node_caption = json.loads(row["NodeCaption"])
        num_links = row["NumLinks"]
        num_nodes = row["NumNodes"]
        selected_node = row["SelectedNode"]

        fstar_data = ForwardStarData(
            node_caption=node_caption,
            first_link=first_link,
            to_node=to_node,
            num_links=num_links,
            num_nodes=num_nodes,
            selected_node=selected_node,
        )

        return fstar_data

    def _get_non_repetitive_tags(
        self,
        record: dict,
        fstar: ForwardStar,
        start_tag_id: int,
        tags_and_values: dict,
        nr_tags_n_values: dict,
        r_tags_n_values: dict,
    ) -> dict:
        # structure of tags_and_values: tags_and_values[tag_id] = (tag, tag_type, value)

        # set with tag_id's which have been processed
        tags_processed = {key for key in r_tags_n_values.keys()}

        # Step 1 - get all the child values down to the bottom
        # and store the tag_id's to prevent override of the values
        # in step 2
        for _, child in fstar.visit_node_descendants(start_tag_id):
            if (
                tags_and_values[child][1] == TagType.data_tag
                and child not in tags_processed
            ):
                record[tags_and_values[child][0]] = tags_and_values[child][2]
                tags_processed.add(child)

        # Step 2 - get all the parent values up to the root
        # and process the child values, i.e. repeat Step 1
        for parent, _ in fstar.visit_node_ancestors(start_tag_id):
            # Step 1 (2nd iteration) - get all the child values down to the bottom
            # and store the tag_id's to prevent override of the values in a
            # later visit
            for _, child in fstar.visit_node_descendants(parent):
                if (
                    tags_and_values[child][1] == TagType.data_tag
                    and child not in tags_processed
                ):
                    record[tags_and_values[child][0]] = tags_and_values[child][2]
                    tags_processed.add(child)
                # elif child == start_tag_id:
                #     break

        # return the enriched record
        return record

    def _create_single_record_output(self, tags_n_values: dict, type: str):
        fields = []
        values = []
        for tag, value in tags_n_values.items():
            fields.append(f"[{tag}]")
            values.append(f"'{value}'")
        field_str = ", ".join(fields)
        value_str = ", ".join(values)
        sql_ins = f"INSERT INTO {type}FinalOutput ({field_str}) VALUES ({value_str})"
        try:
            self.conn.execute(sql_ins)
        except sqlite3.OperationalError as e:
            print(
                f"DocID: {id} with statement:\n{sql_ins}\nled to the following error: {e}"
            )

    def _add_to_cache(self, cache_name: str, record: tuple):
        self.cache[cache_name]["Data"].append(record)

    def _clear_chache(self):
        for key in self.cache:
            self.cache[key]["Data"] = []


# -------------------------------------------------------------------------------
# The script entry point
# -------------------------------------------------------------------------------
if __name__ == "__main__":

    pm = PersistenceManager()

    xmls = (
        """<?xml version="1.0" encoding="UTF-8"?><Document xmlns="urn:iso:std:iso:20022:tech:xsd:pain.008.001.02" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><CstmrDrctDbtInitn><GrpHdr><MsgId>yd5oBwTm19W2rZG3</MsgId><CreDtTm>2013-10-08T12:57:52</CreDtTm><NbOfTxs>2</NbOfTxs><CtrlSum>56465384.0</CtrlSum><InitgPty><Nm>PILOTFORETAG B</Nm><Id><OrgId><Othr><Id>7158637412</Id><SchmeNm><Cd>BANK</Cd></SchmeNm></Othr></OrgId></Id></InitgPty></GrpHdr><PmtInf><PmtInfId>SEND PAYMENT VER 009</PmtInfId><PmtMtd>DD</PmtMtd><BtchBookg>true</BtchBookg><NbOfTxs>2</NbOfTxs><CtrlSum>56465384.0</CtrlSum><PmtTpInf><SvcLvl><Cd>SEPA</Cd></SvcLvl><LclInstrm><Cd>B2B</Cd></LclInstrm><SeqTp>RCUR</SeqTp></PmtTpInf><ReqdColltnDt>2013-11-08</ReqdColltnDt><Cdtr><Nm>PILOTFORETAG B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr></Cdtr><CdtrAcct><Id><IBAN>CH23885378935554937471</IBAN></Id></CdtrAcct><CdtrAgt><FinInstnId><BIC>HANDNL2A</BIC></FinInstnId></CdtrAgt><CdtrSchmeId><Id><PrvtId><Othr><Id>CH13546501204560291467</Id><SchmeNm><Prtry>SEPA</Prtry></SchmeNm></Othr></PrvtId></Id></CdtrSchmeId><DrctDbtTxInf><PmtId><EndToEndId>BMO1 SEND PROD VER 10 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">49975405.0</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER8</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>Pilot B</Nm><PstlAdr><Ctry>NL</Ctry></PstlAdr><Id><OrgId><Othr><Id>5497683033</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>CH89549400409945581319</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 1</Ustrd></RmtInf></DrctDbtTxInf><DrctDbtTxInf><PmtId><EndToEndId>BMO2 SEND PROD VER 11 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">6489979.0</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER9</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>PILOT B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr><Id><OrgId><Othr><Id>7159672956</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>CH89549400409945581319</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 2</Ustrd></RmtInf></DrctDbtTxInf></PmtInf></CstmrDrctDbtInitn></Document>""",
        """<?xml version="1.0" encoding="UTF-8"?><Document xmlns="urn:iso:std:iso:20022:tech:xsd:pain.008.001.02" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><CstmrDrctDbtInitn><GrpHdr><MsgId>RtFuXggs5NRROfX2</MsgId><CreDtTm>2013-10-08T12:57:52</CreDtTm><NbOfTxs>2</NbOfTxs><CtrlSum>79043066.0</CtrlSum><InitgPty><Nm>PILOTFORETAG B</Nm><Id><OrgId><Othr><Id>5821844973</Id><SchmeNm><Cd>BANK</Cd></SchmeNm></Othr></OrgId></Id></InitgPty></GrpHdr><PmtInf><PmtInfId>SEND PAYMENT VER 009</PmtInfId><PmtMtd>DD</PmtMtd><BtchBookg>true</BtchBookg><NbOfTxs>2</NbOfTxs><CtrlSum>79043066.0</CtrlSum><PmtTpInf><SvcLvl><Cd>SEPA</Cd></SvcLvl><LclInstrm><Cd>B2B</Cd></LclInstrm><SeqTp>RCUR</SeqTp></PmtTpInf><ReqdColltnDt>2013-11-08</ReqdColltnDt><Cdtr><Nm>PILOTFORETAG B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr></Cdtr><CdtrAcct><Id><IBAN>CH10527077502227108975</IBAN></Id></CdtrAcct><CdtrAgt><FinInstnId><BIC>HANDNL2A</BIC></FinInstnId></CdtrAgt><CdtrSchmeId><Id><PrvtId><Othr><Id>CH12539863068515968380</Id><SchmeNm><Prtry>SEPA</Prtry></SchmeNm></Othr></PrvtId></Id></CdtrSchmeId><DrctDbtTxInf><PmtId><EndToEndId>BMO1 SEND PROD VER 10 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">61969404.0</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER8</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>Pilot B</Nm><PstlAdr><Ctry>NL</Ctry></PstlAdr><Id><OrgId><Othr><Id>2166468186</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>CH82081682298522704984</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 1</Ustrd></RmtInf></DrctDbtTxInf><DrctDbtTxInf><PmtId><EndToEndId>BMO2 SEND PROD VER 11 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">17073662.0</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER9</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>PILOT B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr><Id><OrgId><Othr><Id>2841910625</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>CH82081682298522704984</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 2</Ustrd></RmtInf></DrctDbtTxInf></PmtInf></CstmrDrctDbtInitn></Document>""",
        """<?xml version="1.0" encoding="UTF-8"?><Document xmlns="urn:iso:std:iso:20022:tech:xsd:pain.008.001.02" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><CstmrDrctDbtInitn><GrpHdr><MsgId>D2H7gplMSNHq8KXN</MsgId><CreDtTm>2013-10-08T12:57:52</CreDtTm><NbOfTxs>2</NbOfTxs><CtrlSum>117887441.0</CtrlSum><InitgPty><Nm>PILOTFORETAG B</Nm><Id><OrgId><Othr><Id>3409640347</Id><SchmeNm><Cd>BANK</Cd></SchmeNm></Othr></OrgId></Id></InitgPty></GrpHdr><PmtInf><PmtInfId>SEND PAYMENT VER 009</PmtInfId><PmtMtd>DD</PmtMtd><BtchBookg>true</BtchBookg><NbOfTxs>2</NbOfTxs><CtrlSum>117887441.0</CtrlSum><PmtTpInf><SvcLvl><Cd>SEPA</Cd></SvcLvl><LclInstrm><Cd>B2B</Cd></LclInstrm><SeqTp>RCUR</SeqTp></PmtTpInf><ReqdColltnDt>2013-11-08</ReqdColltnDt><Cdtr><Nm>PILOTFORETAG B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr></Cdtr><CdtrAcct><Id><IBAN>CH45976017636798651373</IBAN></Id></CdtrAcct><CdtrAgt><FinInstnId><BIC>HANDNL2A</BIC></FinInstnId></CdtrAgt><CdtrSchmeId><Id><PrvtId><Othr><Id>CH86356313299109188981</Id><SchmeNm><Prtry>SEPA</Prtry></SchmeNm></Othr></PrvtId></Id></CdtrSchmeId><DrctDbtTxInf><PmtId><EndToEndId>BMO1 SEND PROD VER 10 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">42427088.0</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER8</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>Pilot B</Nm><PstlAdr><Ctry>NL</Ctry></PstlAdr><Id><OrgId><Othr><Id>4150268815</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>CH81485718864258214724</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 1</Ustrd></RmtInf></DrctDbtTxInf><DrctDbtTxInf><PmtId><EndToEndId>BMO2 SEND PROD VER 11 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">75460353.0</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER9</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>PILOT B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr><Id><OrgId><Othr><Id>8724729036</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>CH81485718864258214724</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 2</Ustrd></RmtInf></DrctDbtTxInf></PmtInf></CstmrDrctDbtInitn></Document>""",
        """<?xml version="1.0" encoding="UTF-8"?><Document xmlns="urn:iso:std:iso:20022:tech:xsd:pain.008.001.02" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><CstmrDrctDbtInitn><GrpHdr><MsgId>VJOqmeb4nO9iIyhD</MsgId><CreDtTm>2013-10-08T12:57:52</CreDtTm><NbOfTxs>2</NbOfTxs><CtrlSum>85669535.0</CtrlSum><InitgPty><Nm>PILOTFORETAG B</Nm><Id><OrgId><Othr><Id>4961732353</Id><SchmeNm><Cd>BANK</Cd></SchmeNm></Othr></OrgId></Id></InitgPty></GrpHdr><PmtInf><PmtInfId>SEND PAYMENT VER 009</PmtInfId><PmtMtd>DD</PmtMtd><BtchBookg>true</BtchBookg><NbOfTxs>2</NbOfTxs><CtrlSum>85669535.0</CtrlSum><PmtTpInf><SvcLvl><Cd>SEPA</Cd></SvcLvl><LclInstrm><Cd>B2B</Cd></LclInstrm><SeqTp>RCUR</SeqTp></PmtTpInf><ReqdColltnDt>2013-11-08</ReqdColltnDt><Cdtr><Nm>PILOTFORETAG B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr></Cdtr><CdtrAcct><Id><IBAN>CH39639238214833260967</IBAN></Id></CdtrAcct><CdtrAgt><FinInstnId><BIC>HANDNL2A</BIC></FinInstnId></CdtrAgt><CdtrSchmeId><Id><PrvtId><Othr><Id>CH42355129508755716551</Id><SchmeNm><Prtry>SEPA</Prtry></SchmeNm></Othr></PrvtId></Id></CdtrSchmeId><DrctDbtTxInf><PmtId><EndToEndId>BMO1 SEND PROD VER 10 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">46944258.0</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER8</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>Pilot B</Nm><PstlAdr><Ctry>NL</Ctry></PstlAdr><Id><OrgId><Othr><Id>6944371483</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>CH86920263896891261155</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 1</Ustrd></RmtInf></DrctDbtTxInf><DrctDbtTxInf><PmtId><EndToEndId>BMO2 SEND PROD VER 11 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">38725277.0</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER9</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>PILOT B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr><Id><OrgId><Othr><Id>6942238282</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>CH86920263896891261155</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 2</Ustrd></RmtInf></DrctDbtTxInf></PmtInf></CstmrDrctDbtInitn></Document>""",
        """<?xml version="1.0" encoding="UTF-8"?><Document xmlns="urn:iso:std:iso:20022:tech:xsd:pain.008.001.02" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><CstmrDrctDbtInitn><GrpHdr><MsgId>jaKQCkNtBmat45kN</MsgId><CreDtTm>2013-10-08T12:57:52</CreDtTm><NbOfTxs>2</NbOfTxs><CtrlSum>41366911.0</CtrlSum><InitgPty><Nm>PILOTFORETAG B</Nm><Id><OrgId><Othr><Id>1515650071</Id><SchmeNm><Cd>BANK</Cd></SchmeNm></Othr></OrgId></Id></InitgPty></GrpHdr><PmtInf><PmtInfId>SEND PAYMENT VER 009</PmtInfId><PmtMtd>DD</PmtMtd><BtchBookg>true</BtchBookg><NbOfTxs>2</NbOfTxs><CtrlSum>41366911.0</CtrlSum><PmtTpInf><SvcLvl><Cd>SEPA</Cd></SvcLvl><LclInstrm><Cd>B2B</Cd></LclInstrm><SeqTp>RCUR</SeqTp></PmtTpInf><ReqdColltnDt>2013-11-08</ReqdColltnDt><Cdtr><Nm>PILOTFORETAG B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr></Cdtr><CdtrAcct><Id><IBAN>CH76304291163027618599</IBAN></Id></CdtrAcct><CdtrAgt><FinInstnId><BIC>HANDNL2A</BIC></FinInstnId></CdtrAgt><CdtrSchmeId><Id><PrvtId><Othr><Id>CH87289282047289592462</Id><SchmeNm><Prtry>SEPA</Prtry></SchmeNm></Othr></PrvtId></Id></CdtrSchmeId><DrctDbtTxInf><PmtId><EndToEndId>BMO1 SEND PROD VER 10 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">21463099.0</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER8</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>Pilot B</Nm><PstlAdr><Ctry>NL</Ctry></PstlAdr><Id><OrgId><Othr><Id>7585435117</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>CH49619510590676729084</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 1</Ustrd></RmtInf></DrctDbtTxInf><DrctDbtTxInf><PmtId><EndToEndId>BMO2 SEND PROD VER 11 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">19903812.0</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER9</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>PILOT B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr><Id><OrgId><Othr><Id>3373277665</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>CH49619510590676729084</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 2</Ustrd></RmtInf></DrctDbtTxInf></PmtInf></CstmrDrctDbtInitn></Document>""",
    )

    for id, xml in enumerate(xmls, start=1):
        xml_parsed = XmlParser(xml)
        xml_tag_and_value = xml_parsed.get_tags_and_values(
            attribute_usage=AttributeUsage.add_separate_tag, concat_on_key_error=True
        )

        pm.store_doc(id, DocValidity.VALID, xml)
        pm.store_xml_parsed(id, xml_tag_and_value, xml_parsed)
        pm.log_success(id, f"Message {id} succressfully processed")

    pm.create_indices(IndexGroup.DOC_STORE)
    pm.create_indices(IndexGroup.XML_STORE)
    pm.create_indices(IndexGroup.PROCESS_LOG)

    # # get all documents
    # for doc_row in pm.get_all_docs(DocValidity.VALID):
    #     print(f"ID: {doc_row[0]} | validity: {doc_row[1]} | text: {doc_row[2]}")

    # get a single document
    print(f"SINGLE DOC: \n{pm.get_single_doc(5)}")

    # get a single pared document
    id = 4
    t = pm.get_xml_doc_parsed(id, XmlAttribute.Tags)
    t_n_v = pm.get_xml_doc_parsed(id, XmlAttribute.ParsedXml)

    print(f"TAGS: \n{t}")
    print(type(t))
    print(f"TAGS_N_VALS: \n{t_n_v}")
    print(type(t_n_v))

    no_of_docs = pm.get_doc_count()
    print(no_of_docs)