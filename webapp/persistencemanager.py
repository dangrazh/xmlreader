"""Provides a data persistence object that stores the data extracted with a FileProcessor object. 
"""

# ------------------
# Imports
# ------------------
import os
import sqlite3
from typing import Optional, List

# from typing_extensions import ParamSpecArgs
import uuid
import json

# USAGE = "standalone"
USAGE = "flask"

# import for usage in flask app
if USAGE == "flask":
    from webapp.xmlparser import XmlParser, AttributeUsage

# from webapp.xmlvalidator import XmlValidator  # , ValidationResult
# from webapp.profiler import profile, StopWatch

# import for standalone usage
# if USAGE == "standalone":
#     from xmlparser import XmlParser, AttributeUsage

# from xmlvalidator import XmlValidator  # , ValidationResult
# from profiler import profile


# Public symbols.
# __all__ = ["__version__", "__doc__"]

# Authoship information
__author__ = "Daniel Grass"
__copyright__ = "Copyright 2021, Daniel Grass"
# __credits__ = []
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

        # self.conn = sqlite3.connect(":memory:")

    def __del__(self):
        print("PersistenceManager is being deleted")
        self._disconnect_db()

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
        with self.conn:
            self.conn.execute(
                "INSERT INTO ProcessLog VALUES (:id, :result, :text)",
                {"id": doc_id, "result": LogLevel.ERROR, "text": log_entry_text},
            )

    def log_success(self, doc_id: int, log_entry_text: str):
        with self.conn:
            self.conn.execute(
                "INSERT INTO ProcessLog VALUES (:id, :result, :text)",
                {"id": doc_id, "result": LogLevel.INFO, "text": log_entry_text},
            )

    def log_warning(self, doc_id: int, log_entry_text: str):
        with self.conn:
            self.conn.execute(
                "INSERT INTO ProcessLog VALUES (:id, :result, :text)",
                {"id": doc_id, "result": LogLevel.WARNING, "text": log_entry_text},
            )

    def get_process_log(self, log_level: LogLevel):
        if log_level == LogLevel.ALL:
            curr = self.conn.execute("SELECT * FROM ProcessLog")
        else:
            curr = self.conn.execute(
                "SELECT * FROM ProcessLog where LogLevel=:log_level",
                {"log_level": log_level},
            )
        for row in curr:
            yield row

    def truncate_process_log(self):
        with self.conn:
            self.conn.execute(
                "DELETE FROM ProcessLog",
            )
            # self.conn.execute(
        #     "VACUUM",
        # )

    # Document Management
    def store_doc(
        self,
        doc_id: int,
        doc_validity: DocValidity,
        doc_text: str,
        doc_invalid_reason: str = "",
    ):
        with self.conn:
            self.conn.execute(
                "INSERT INTO DocList VALUES (:id, :type, :text, :reason)",
                {
                    "id": doc_id,
                    "type": doc_validity,
                    "text": doc_text,
                    "reason": doc_invalid_reason,
                },
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
        top_node = str(xml_obj.top_node)
        tags = json.dumps(obj=xml_obj.tags)
        tags_and_value = json.dumps(obj=tags_and_values)

        # store the information
        with self.conn:
            self.conn.execute(
                "INSERT INTO ParsedXmlStore VALUES (:DocID, :Type, :ParsedXml, :SoupNoOfTags, :SourceNoOfTags, :Tags, :TopNode)",
                {
                    "DocID": doc_id,
                    "Type": type,
                    "ParsedXml": tags_and_value,
                    "SoupNoOfTags": soup_no_of_tags,
                    "SourceNoOfTags": source_no_of_tags,
                    "Tags": tags,
                    "TopNode": top_node,
                },
            )

            # store all tag and value pairs
            tag_idx = 0
            for tag, value in tags_and_values.items():
                tag_idx += 1
                if isinstance(value, list):
                    for idx, itm in enumerate(value):
                        self.conn.execute(
                            "INSERT INTO XmlTagsAndValues VALUES (:DocID, :Type, :TagOrder, :Tag, :RepNo, :Value)",
                            {
                                "DocID": doc_id,
                                "Type": type,
                                "TagOrder": tag_idx,
                                "Tag": tag,
                                "RepNo": idx,
                                "Value": itm,
                            },
                        )

                else:
                    self.conn.execute(
                        "INSERT INTO XmlTagsAndValues VALUES (:DocID, :Type, :TagOrder, :Tag, :RepNo, :Value)",
                        {
                            "DocID": doc_id,
                            "Type": type,
                            "TagOrder": tag_idx,
                            "Tag": tag,
                            "RepNo": 0,
                            "Value": value,
                        },
                    )

    def get_xml_doc_parsed(self, doc_id: int, attribute: XmlAttribute):

        str_select = f"SELECT {attribute} FROM ParsedXmlStore where DocID=:id"
        curr = self.conn.execute(str_select, {"id": doc_id})
        xml_parsed = curr.fetchone()
        xml_ret = xml_parsed[0]
        # print(xml_ret)

        if attribute == XmlAttribute.Tags:
            output = json.loads(xml_ret)
        elif attribute == XmlAttribute.ParsedXml:
            dict_to_process = json.loads(xml_ret)
            output = self._replace_lists(dict_to_process)
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

        sql_cre_tmp_tbl = f"CREATE TEMP TABLE temp_{doc_type}_xmlTypeStats AS SELECT TagOrder, Tag, DocID, max(TagRepetition) AS maxRepetition FROM XmlTagsAndValues WHERE Type=:doc_type GROUP BY TagOrder, Tag, DocID"
        sql_sel_result = f"SELECT Tag, max(maxRepetition) AS maxRep, min(maxRepetition) AS minRep, avg(maxRepetition) AS avgRep FROM temp_{doc_type}_xmlTypeStats GROUP BY TagOrder ORDER BY TagOrder"

        self.conn.execute(
            sql_cre_tmp_tbl,
            {"doc_type": doc_type},
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
            "SELECT DISTINCT Tag FROM XmlTagsAndValues WHERE Type=:doc_type ORDER BY TagOrder",
            {"doc_type": doc_type},
        )
        for row in curr:
            yield row

    def get_xml_tag_stats_by_doc_id(self, doc_id: int):
        curr = self.conn.execute(
            "SELECT Tag, MAX(TagRepetition) FROM XmlTagsAndValues WHERE DocID=:doc_id GROUP BY Tag",
            {"doc_id": doc_id},
        )
        for row in curr:
            yield row

    def create_output_by_xml_type(
        self, doc_type: str, create_record_on: Optional[List[str]] = None
    ):
        # get the headers and create the fieldlist
        headers = [
            f"[{row['Tag']}] text, " for row in self.get_xml_tags_by_type(doc_type)
        ]
        s = ""
        fieldlist = s.join(headers)
        fieldlist = fieldlist[:-2]

        # create table with headers
        sql_del_tbl = f"DROP TABLE IF EXISTS {doc_type}FinalOutput"
        sql_cre_tbl = f"CREATE TABLE {doc_type}FinalOutput ({fieldlist})"
        # print(sql_cre_tbl)
        self.conn.execute(sql_del_tbl)
        self.conn.execute(sql_cre_tbl)

        # process the documents
        if not create_record_on:
            for type, id in self.get_xml_doc_id_by_type(doc_type):
                # print(f"Creating output table for ID {id}: {type}")
                # simply process each document without taking care of any repetitive elements
                tags_n_values = self.get_xml_doc_parsed(id, XmlAttribute.ParsedXml)
                fields = []
                values = []
                for tag, value in tags_n_values.items():
                    fields.append(f"[{tag}]")
                    values.append(f"'{value}'")
                field_str = ", ".join(fields)
                value_str = ", ".join(values)
                sql_ins = (
                    f"INSERT INTO {type}FinalOutput ({field_str}) VALUES ({value_str})"
                )
                try:
                    self.conn.execute(sql_ins)
                except sqlite3.OperationalError as e:
                    print(
                        f"DocID: {id} with statement:\n{sql_ins}\nled to the following error: {e}"
                    )
        else:
            # process each document while creating new recoreds for fields provided
            for type, id in self.get_xml_doc_id_by_type(doc_type):
                pass

    def get_output_by_xml_type(self, doc_type: str):

        sql = f"SELECT * FROM {doc_type}FinalOutput"
        curr = self.conn.execute(sql)

        for row in curr:
            yield row

    def truncate_xml_store(self):
        with self.conn:
            self.conn.execute(
                "DELETE FROM DocList",
            )
            self.conn.execute(
                "DELETE FROM XmlTagsAndValues",
            )

    # general database Management
    def remove_db_file(self):
        os.remove(self.db_name)

    def create_indices(self, index_group: IndexGroup):

        if index_group == IndexGroup.PROCESS_LOG:
            # the ProcessLog index
            with self.conn:
                self.conn.execute(
                    """CREATE UNIQUE INDEX IdxProcessLogDocID
                    ON ProcessLog (DocID)"""
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

    # ------------------
    # Internal Functions
    # ------------------

    def _connect_db(self):
        self.conn = sqlite3.connect(self.db_name, isolation_level=None)
        self.conn.execute("PRAGMA synchronous = OFF")
        self.conn.execute("PRAGMA journal_mode = OFF")
        self.conn.row_factory = sqlite3.Row

        # self.c = self.conn.cursor()

    def _disconnect_db(self):
        # self.c = None
        self.conn.close()
        self.conn = None

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
                            TagRepetition integer,
                            value text
                            )"""
            )

    def _replace_lists(self, dict_in: dict):

        separator = " | "
        for key in dict_in:
            if isinstance(dict_in[key], list):
                dict_in[key] = separator.join(dict_in[key])

        return dict_in


# -------------------------------------------------------------------------------
# The script entry point
# -------------------------------------------------------------------------------
if __name__ == "__main__":

    pm = PersistenceManager()

    xmls = [
        """<?xml version="1.0" encoding="UTF-8"?><Document xmlns="urn:iso:std:iso:20022:tech:xsd:pain.008.001.02" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><CstmrDrctDbtInitn><GrpHdr><MsgId>yd5oBwTm19W2rZG3</MsgId><CreDtTm>2013-10-08T12:57:52</CreDtTm><NbOfTxs>2</NbOfTxs><CtrlSum>56465384.0</CtrlSum><InitgPty><Nm>PILOTFORETAG B</Nm><Id><OrgId><Othr><Id>7158637412</Id><SchmeNm><Cd>BANK</Cd></SchmeNm></Othr></OrgId></Id></InitgPty></GrpHdr><PmtInf><PmtInfId>SEND PAYMENT VER 009</PmtInfId><PmtMtd>DD</PmtMtd><BtchBookg>true</BtchBookg><NbOfTxs>2</NbOfTxs><CtrlSum>56465384.0</CtrlSum><PmtTpInf><SvcLvl><Cd>SEPA</Cd></SvcLvl><LclInstrm><Cd>B2B</Cd></LclInstrm><SeqTp>RCUR</SeqTp></PmtTpInf><ReqdColltnDt>2013-11-08</ReqdColltnDt><Cdtr><Nm>PILOTFORETAG B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr></Cdtr><CdtrAcct><Id><IBAN>CH23885378935554937471</IBAN></Id></CdtrAcct><CdtrAgt><FinInstnId><BIC>HANDNL2A</BIC></FinInstnId></CdtrAgt><CdtrSchmeId><Id><PrvtId><Othr><Id>CH13546501204560291467</Id><SchmeNm><Prtry>SEPA</Prtry></SchmeNm></Othr></PrvtId></Id></CdtrSchmeId><DrctDbtTxInf><PmtId><EndToEndId>BMO1 SEND PROD VER 10 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">49975405.0</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER8</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>Pilot B</Nm><PstlAdr><Ctry>NL</Ctry></PstlAdr><Id><OrgId><Othr><Id>5497683033</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>CH89549400409945581319</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 1</Ustrd></RmtInf></DrctDbtTxInf><DrctDbtTxInf><PmtId><EndToEndId>BMO2 SEND PROD VER 11 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">6489979.0</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER9</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>PILOT B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr><Id><OrgId><Othr><Id>7159672956</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>CH89549400409945581319</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 2</Ustrd></RmtInf></DrctDbtTxInf></PmtInf></CstmrDrctDbtInitn></Document>""",
        """<?xml version="1.0" encoding="UTF-8"?><Document xmlns="urn:iso:std:iso:20022:tech:xsd:pain.008.001.02" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><CstmrDrctDbtInitn><GrpHdr><MsgId>RtFuXggs5NRROfX2</MsgId><CreDtTm>2013-10-08T12:57:52</CreDtTm><NbOfTxs>2</NbOfTxs><CtrlSum>79043066.0</CtrlSum><InitgPty><Nm>PILOTFORETAG B</Nm><Id><OrgId><Othr><Id>5821844973</Id><SchmeNm><Cd>BANK</Cd></SchmeNm></Othr></OrgId></Id></InitgPty></GrpHdr><PmtInf><PmtInfId>SEND PAYMENT VER 009</PmtInfId><PmtMtd>DD</PmtMtd><BtchBookg>true</BtchBookg><NbOfTxs>2</NbOfTxs><CtrlSum>79043066.0</CtrlSum><PmtTpInf><SvcLvl><Cd>SEPA</Cd></SvcLvl><LclInstrm><Cd>B2B</Cd></LclInstrm><SeqTp>RCUR</SeqTp></PmtTpInf><ReqdColltnDt>2013-11-08</ReqdColltnDt><Cdtr><Nm>PILOTFORETAG B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr></Cdtr><CdtrAcct><Id><IBAN>CH10527077502227108975</IBAN></Id></CdtrAcct><CdtrAgt><FinInstnId><BIC>HANDNL2A</BIC></FinInstnId></CdtrAgt><CdtrSchmeId><Id><PrvtId><Othr><Id>CH12539863068515968380</Id><SchmeNm><Prtry>SEPA</Prtry></SchmeNm></Othr></PrvtId></Id></CdtrSchmeId><DrctDbtTxInf><PmtId><EndToEndId>BMO1 SEND PROD VER 10 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">61969404.0</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER8</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>Pilot B</Nm><PstlAdr><Ctry>NL</Ctry></PstlAdr><Id><OrgId><Othr><Id>2166468186</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>CH82081682298522704984</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 1</Ustrd></RmtInf></DrctDbtTxInf><DrctDbtTxInf><PmtId><EndToEndId>BMO2 SEND PROD VER 11 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">17073662.0</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER9</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>PILOT B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr><Id><OrgId><Othr><Id>2841910625</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>CH82081682298522704984</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 2</Ustrd></RmtInf></DrctDbtTxInf></PmtInf></CstmrDrctDbtInitn></Document>""",
        """<?xml version="1.0" encoding="UTF-8"?><Document xmlns="urn:iso:std:iso:20022:tech:xsd:pain.008.001.02" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><CstmrDrctDbtInitn><GrpHdr><MsgId>D2H7gplMSNHq8KXN</MsgId><CreDtTm>2013-10-08T12:57:52</CreDtTm><NbOfTxs>2</NbOfTxs><CtrlSum>117887441.0</CtrlSum><InitgPty><Nm>PILOTFORETAG B</Nm><Id><OrgId><Othr><Id>3409640347</Id><SchmeNm><Cd>BANK</Cd></SchmeNm></Othr></OrgId></Id></InitgPty></GrpHdr><PmtInf><PmtInfId>SEND PAYMENT VER 009</PmtInfId><PmtMtd>DD</PmtMtd><BtchBookg>true</BtchBookg><NbOfTxs>2</NbOfTxs><CtrlSum>117887441.0</CtrlSum><PmtTpInf><SvcLvl><Cd>SEPA</Cd></SvcLvl><LclInstrm><Cd>B2B</Cd></LclInstrm><SeqTp>RCUR</SeqTp></PmtTpInf><ReqdColltnDt>2013-11-08</ReqdColltnDt><Cdtr><Nm>PILOTFORETAG B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr></Cdtr><CdtrAcct><Id><IBAN>CH45976017636798651373</IBAN></Id></CdtrAcct><CdtrAgt><FinInstnId><BIC>HANDNL2A</BIC></FinInstnId></CdtrAgt><CdtrSchmeId><Id><PrvtId><Othr><Id>CH86356313299109188981</Id><SchmeNm><Prtry>SEPA</Prtry></SchmeNm></Othr></PrvtId></Id></CdtrSchmeId><DrctDbtTxInf><PmtId><EndToEndId>BMO1 SEND PROD VER 10 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">42427088.0</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER8</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>Pilot B</Nm><PstlAdr><Ctry>NL</Ctry></PstlAdr><Id><OrgId><Othr><Id>4150268815</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>CH81485718864258214724</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 1</Ustrd></RmtInf></DrctDbtTxInf><DrctDbtTxInf><PmtId><EndToEndId>BMO2 SEND PROD VER 11 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">75460353.0</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER9</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>PILOT B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr><Id><OrgId><Othr><Id>8724729036</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>CH81485718864258214724</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 2</Ustrd></RmtInf></DrctDbtTxInf></PmtInf></CstmrDrctDbtInitn></Document>""",
        """<?xml version="1.0" encoding="UTF-8"?><Document xmlns="urn:iso:std:iso:20022:tech:xsd:pain.008.001.02" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><CstmrDrctDbtInitn><GrpHdr><MsgId>VJOqmeb4nO9iIyhD</MsgId><CreDtTm>2013-10-08T12:57:52</CreDtTm><NbOfTxs>2</NbOfTxs><CtrlSum>85669535.0</CtrlSum><InitgPty><Nm>PILOTFORETAG B</Nm><Id><OrgId><Othr><Id>4961732353</Id><SchmeNm><Cd>BANK</Cd></SchmeNm></Othr></OrgId></Id></InitgPty></GrpHdr><PmtInf><PmtInfId>SEND PAYMENT VER 009</PmtInfId><PmtMtd>DD</PmtMtd><BtchBookg>true</BtchBookg><NbOfTxs>2</NbOfTxs><CtrlSum>85669535.0</CtrlSum><PmtTpInf><SvcLvl><Cd>SEPA</Cd></SvcLvl><LclInstrm><Cd>B2B</Cd></LclInstrm><SeqTp>RCUR</SeqTp></PmtTpInf><ReqdColltnDt>2013-11-08</ReqdColltnDt><Cdtr><Nm>PILOTFORETAG B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr></Cdtr><CdtrAcct><Id><IBAN>CH39639238214833260967</IBAN></Id></CdtrAcct><CdtrAgt><FinInstnId><BIC>HANDNL2A</BIC></FinInstnId></CdtrAgt><CdtrSchmeId><Id><PrvtId><Othr><Id>CH42355129508755716551</Id><SchmeNm><Prtry>SEPA</Prtry></SchmeNm></Othr></PrvtId></Id></CdtrSchmeId><DrctDbtTxInf><PmtId><EndToEndId>BMO1 SEND PROD VER 10 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">46944258.0</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER8</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>Pilot B</Nm><PstlAdr><Ctry>NL</Ctry></PstlAdr><Id><OrgId><Othr><Id>6944371483</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>CH86920263896891261155</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 1</Ustrd></RmtInf></DrctDbtTxInf><DrctDbtTxInf><PmtId><EndToEndId>BMO2 SEND PROD VER 11 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">38725277.0</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER9</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>PILOT B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr><Id><OrgId><Othr><Id>6942238282</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>CH86920263896891261155</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 2</Ustrd></RmtInf></DrctDbtTxInf></PmtInf></CstmrDrctDbtInitn></Document>""",
        """<?xml version="1.0" encoding="UTF-8"?><Document xmlns="urn:iso:std:iso:20022:tech:xsd:pain.008.001.02" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><CstmrDrctDbtInitn><GrpHdr><MsgId>jaKQCkNtBmat45kN</MsgId><CreDtTm>2013-10-08T12:57:52</CreDtTm><NbOfTxs>2</NbOfTxs><CtrlSum>41366911.0</CtrlSum><InitgPty><Nm>PILOTFORETAG B</Nm><Id><OrgId><Othr><Id>1515650071</Id><SchmeNm><Cd>BANK</Cd></SchmeNm></Othr></OrgId></Id></InitgPty></GrpHdr><PmtInf><PmtInfId>SEND PAYMENT VER 009</PmtInfId><PmtMtd>DD</PmtMtd><BtchBookg>true</BtchBookg><NbOfTxs>2</NbOfTxs><CtrlSum>41366911.0</CtrlSum><PmtTpInf><SvcLvl><Cd>SEPA</Cd></SvcLvl><LclInstrm><Cd>B2B</Cd></LclInstrm><SeqTp>RCUR</SeqTp></PmtTpInf><ReqdColltnDt>2013-11-08</ReqdColltnDt><Cdtr><Nm>PILOTFORETAG B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr></Cdtr><CdtrAcct><Id><IBAN>CH76304291163027618599</IBAN></Id></CdtrAcct><CdtrAgt><FinInstnId><BIC>HANDNL2A</BIC></FinInstnId></CdtrAgt><CdtrSchmeId><Id><PrvtId><Othr><Id>CH87289282047289592462</Id><SchmeNm><Prtry>SEPA</Prtry></SchmeNm></Othr></PrvtId></Id></CdtrSchmeId><DrctDbtTxInf><PmtId><EndToEndId>BMO1 SEND PROD VER 10 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">21463099.0</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER8</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>Pilot B</Nm><PstlAdr><Ctry>NL</Ctry></PstlAdr><Id><OrgId><Othr><Id>7585435117</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>CH49619510590676729084</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 1</Ustrd></RmtInf></DrctDbtTxInf><DrctDbtTxInf><PmtId><EndToEndId>BMO2 SEND PROD VER 11 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">19903812.0</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER9</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>PILOT B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr><Id><OrgId><Othr><Id>3373277665</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>CH49619510590676729084</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 2</Ustrd></RmtInf></DrctDbtTxInf></PmtInf></CstmrDrctDbtInitn></Document>""",
    ]

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