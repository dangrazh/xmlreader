"""Provides a file processing object that splits a given input file into single validated xml documents.
These docuemnts are then processed leveraging the xmlparser module and finally can be exported to MS Excel.
"""

# ------------------
# Imports
# ------------------
import os
import datetime as dt
from re import I
import tempfile

# import re
import regex as re
import xml.sax
import xlsxwriter

# import pandas as pd

# import for usage in flask app
from webapp.xmlparser import XmlParser, AttributeUsage
from webapp.xmlvalidator import XmlValidator  # , ValidationResult
from webapp.persistencemanager import (
    PersistenceManager,
    DocValidity,
    XmlAttribute,
    IndexGroup,
    LogLevel,
)

# imports for profiling
from webapp.profiler import profile, StopWatch

# from memory_profiler import profile
# from webapp.profiler import get_actualsizeof

# import for standalone usage
# from xmlparser import XmlParser, AttributeUsage
# from xmlvalidator import XmlValidator  # , ValidationResult
# from persistencemanager import PersistenceManager, DocValidity, XmlAttribute
# from profiler import profile, StopWatch
# from persistencemanager import (
#     PersistenceManager,
#     DocValidity,
#     XmlAttribute,
#     IndexGroup,
#     LogLevel,
# )

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


class FileProcessor:

    # ------------------
    # Standard Functions
    # ------------------
    def __init__(self, source_file, data_directory=None):

        with open(source_file) as file_handle:

            # basic attributes
            self.file_content = file_handle.read()
            self.file_full_name = file_handle.name
            self.file_path = os.path.abspath(file_handle.name)
            self.file_name = os.path.basename(file_handle.name)
            self.file_name_root = os.path.splitext(self.file_name)[0]
            self.line_count = self.file_content.count("\n")

        # get the current temporary directory
        self._temp_path = tempfile.gettempdir()

        # create the persistence manager
        self.pm = PersistenceManager(data_directory=self._temp_path)
        self.db_name = self.pm.db_name

        # define the identifiers of a xml document
        self.xml_identifiers = {}

        # define the regex to split
        # 1st xml prolog + document tag
        # 2nd xml prolog not followed by document tag [ regex with negative lookahead -> (?!<Document .*?>) ]
        # 3rd document tag not preceeded by xml prolog [ regex with negative lookbehind -> (?<!<\?xml .*?>) ]
        #     (the negative lookbehind with non fixed width requires the pyhton regex module to wokr as the
        #     standard python re module only supports negative lookbehind with fixed width)
        # Important: every regex has to be a single capturing group i.e. enclosed with () and cannot contain
        #            subcapturing groups, i.e. other parts enclosed in () -> exception: the lookbehind and
        #            lookahead groups that need to be enclosed in () but are not considered as capturing groups
        #            by the regex engine - this is important as the number of groups is used in logic in the
        #            _split_into_documents() function.
        # Performance improvements from: https://www.loggly.com/blog/five-invaluable-techniques-to-improve-regex-performance/
        self.re_split = [
            r"(<\?xml .*?><Document .*?>)",
            r"(<\?xml .*?>(?!<Document .*?>))",
            r"((?<!<\?xml .*?>)<Document .*?>)",
        ]

        # Final regex to split using a positive lookahead (?=...), where (?={}) will be replaced
        # with the delimiters defined in the list above, joined with | to get an "or" match
        # in the regex split function. The (?!$) matches only a location that is not
        # immediately followed with the end of string - removed this to test performance impact
        # print(r"(?={})(?!$)".format("|".join(self.re_split)))
        self.comp_re_split = re.compile(
            r"(?={})".format("|".join(self.re_split)), re.IGNORECASE
        )

        # the delimiters and process log stores
        self.doc_delimiters = []
        # self.process_log = []

        # flag if file has already been processed
        self.file_processed = False

        # define comments to ignore
        # TODO: include in process file, resp. in pre-processing cleanup
        comment_str_open = "<!--"
        comment_str_close = "-->"

        # define tags to ignore
        # TODO: include in process file, resp. in pre-processing cleanup
        self.tags_to_ignore = {"script"}

        # read the file into a list of single xml documents
        # self.xml_source = []
        # self.xml_source_invalid = []
        self.xml_split_noerror = self._split_into_documents()
        # self.no_of_docs_in_file = len(self.xml_source)
        # self.no_of_invalid_docs_in_file = len(self.xml_source_invalid)
        self.no_of_docs_in_file = self.pm.get_doc_count(DocValidity.VALID)
        self.no_of_invalid_docs_in_file = self.pm.get_doc_count(DocValidity.INVALID)

        # the raw file content is no longer needed, once the _split_into_documents() has been executed.
        # so release that memory by deleting the variable
        del self.file_content

        # initiate the internal store for the the output
        self.doc_types = {}

    def __str__(self):
        out_string = (
            f"file_full_name: {self.file_full_name}\n"
            f"file_path: {self.file_path}\n"
            f"file_name: {self.file_name}\n"
            f"file_name_root: {self.file_name_root}\n"
            f"no_of_docs_in_file: {self.no_of_docs_in_file}\n"
            f"temp_path: {self._temp_path}\n"
        )
        return out_string

    def __repr__(self):
        return self.__str__()

    # ------------------
    # Public Functions
    # ------------------
    # @profile
    def process_file(
        self,
        attribute_usage=AttributeUsage.add_separate_tag,
        concat_on_key_error=True,
        top_node_tree_level=0,
        type_distance_to_top=1,
    ):

        # initialize the internal store for the the output
        self.doc_types = {}
        # self.process_log = []
        self.pm.truncate_process_log()

        # initialize the return value
        out = "success"

        print(f"No of docs to process: {self.pm.get_doc_count(DocValidity.VALID)}")

        # for index, doc_validity, xml_doc, doc_invalid_reason in self.pm.get_all_docs(
        #     DocValidity.VALID
        # ):
        #     print(index, doc_validity, xml_doc[:100])

        # process the data
        for index, doc_validity, xml_doc, doc_invalid_reason in self.pm.get_all_docs(
            DocValidity.VALID
        ):
            # print(f"START: Processing document #{index}...")
            if index % 10000 == 0:
                print(f"START: Processing document #{index}...")
            # the actual processing
            try:
                xml_parsed = XmlParser(
                    xml_doc, top_node_tree_level, type_distance_to_top
                )
            except ValueError as e:
                # print(
                #     f"ERROR: skipping document #{index} due to the following error: {e}"
                # )

                self.pm.log_error(
                    index,
                    f"ERROR: skipping document #{index} due to the following error: {e}",
                )
                # self.process_log.append(
                #     f"ERROR: skipping document #{index} due to the following error: {e}"
                # )
                out = "error"
                xml_parsed = None
            except IndexError as e:

                self.pm.log_error(
                    index,
                    f"ERROR: parsinng document #{index} failed due to the following error: {e}",
                )
                # self.process_log.append(
                #     f"ERROR: parsinng document #{index} failed due to the following error: {e}"
                # )
                out = "error"
                xml_parsed = None
                # return out
            else:
                # print(f"INFO: document #{index} successfully loaded")
                self.pm.log_success(
                    index, f"INFO: document #{index} successfully loaded"
                )
                # self.process_log.append(f"INFO: document #{index} successfully loaded")
                # check the document type and either get the list of records
                # if already existing or create a new list if new document type
                # if xml_parsed.type in self.doc_types:
                #     doc_data = self.doc_types[xml_parsed.type]
                # else:
                #     doc_data = []

                # read the tags and values from the parsed xml document
                # the error handling is actually not needed as the option
                # concat_on_key_error=True will not raise any KeyErrors
                try:
                    tags_n_values = xml_parsed.get_tags_and_values(
                        attribute_usage, concat_on_key_error
                    )
                except KeyError as e:
                    # print(
                    #     f"ERROR: skipping document #{index} due to the following error: {e}"
                    # )
                    self.pm.log_error(
                        index,
                        f"ERROR: skipping document #{index} due to the following error: {e}",
                    )
                    # self.process_log.append(
                    #     f"ERROR: skipping document #{index} due to the following error: {e}"
                    # )
                    out = "error"
                else:
                    # append the new record to the list for the current document type
                    # and store/update the new list in the dict holding all document type lists
                    # doc_data.append(tags_n_values)
                    # self.doc_types[xml_parsed.type] = doc_data

                    # safe the parsed document to the database
                    self.pm.store_xml_parsed(index, tags_n_values, xml_parsed)

        # print(f"Size of doc store in MiB: {get_actualsizeof(self.doc_types)}")

        # once all data is processed and stored, create the indices
        self.pm.create_indices(IndexGroup.PROCESS_LOG)
        self.pm.create_indices(IndexGroup.XML_STORE)

        self.file_processed = True
        return out

    def inspect_samples(self):
        # create the empty dict holding the output
        doc_type_samples = {}

        # loop through the full store of document types and data
        # and return the 1st data element of each as a sample
        # for type in self.doc_types:
        #     doc_type_samples[type] = self.doc_types[type][0]

        for type in self.pm.get_xml_types():
            for doc_row in self.pm.get_xml_doc_id_by_type(type):
                doc_id = doc_row["DocID"]
                break
            doc_type_samples[type] = self.pm.get_xml_doc_parsed(
                doc_id, XmlAttribute.ParsedXml
            )

        return doc_type_samples

    def get_processed_file_overview(self):

        doc_types = {}

        for xml_type, no_of_docs in self.pm.get_xml_types_overview():
            doc_type_stats = []
            for (
                tag,
                max_repets,
                min_repets,
                avg_Repets,
            ) in self.pm.get_single_xml_type_stats(xml_type):
                doc_type_stats.append([tag, max_repets, min_repets, avg_Repets])
            doc_types[xml_type] = {
                "no_of_docs": no_of_docs,
                "doc_type_stats": doc_type_stats,
            }

        return doc_types

    def get_processed_file_overview_with_samples(self):

        doc_data = {}
        doc_samples = self.inspect_samples()
        file_stats = self.get_processed_file_overview()

        for doc_type in file_stats:
            doc_data[doc_type] = {
                "sample_data": doc_samples[doc_type],
                "no_of_docs": file_stats[doc_type]["no_of_docs"],
                "tag_stats": file_stats[doc_type]["doc_type_stats"],
            }

        return doc_data

    def info(self):
        out = [
            f"File name: {self.file_name_root}",
            f"No of lines in file: {self.line_count}",
            f"No of valid documents in file: {self.no_of_docs_in_file}",
        ]
        return out

    def debug_info(self):
        out = {
            "No of valid documents in file": self.no_of_docs_in_file,
            "Tags to ignore": self.tags_to_ignore,
            "Document delimiters found": self.doc_delimiters,
            # "Documents in file": self.xml_source,
            # "Parsed file content": self.doc_types,
        }

        return out

    def get_process_log(self, log_level: LogLevel = None):

        log_lvl = {
            0: "INFO",
            1: "WARNING",
            2: "ERROR",
        }

        if not log_level:
            log_level = LogLevel.ALL

        for row in self.pm.get_process_log(log_level):
            doc_id = row["DocID"]
            log_lev = log_lvl[row["LogLevel"]]
            log_entry = row["LogEntry"]
            row_out = (doc_id, log_lev, log_entry)
            yield row_out

    def get_xml_docs_valid(self):

        for row in self.pm.get_all_docs(DocValidity.VALID):
            row_out = (row["DocID"], row["DocText"])
            yield row_out

    # @profile
    def to_excel(self, out_path=None):
        # create the output
        cnt_files = 0

        dt_now = dt.datetime.now()
        timestamp = dt_now.strftime("%Y%m%d_%H%M%S")

        if out_path:
            file_out_path = out_path + "/"
        else:
            file_out_path = self._temp_path + "/"

        file_out_name = f"{self.file_name_root}_{timestamp}.xlsx"
        file_out = file_out_path + file_out_name

        # if we have output to produce, loop through the dict holding all document type lists
        # and create an Excel file for each output type
        # if len(self.doc_types) > 0:
        #     # create the excel writer object
        #     with pd.ExcelWriter(
        #         file_out, engine="xlsxwriter"
        #     ) as writer:  # pylint: disable=abstract-class-instantiated
        #         for item in self.doc_types:
        #             df = pd.DataFrame(self.doc_types[item])
        #             df.to_excel(writer, index=False, sheet_name=item)
        #             cnt_files += 1
        #     # return f"{cnt_files} data tabs created in output file!"
        #     return file_out_name
        # else:
        #     return "No output data extracted - Excel file not created!"

        # Create file
        workbook = xlsxwriter.Workbook(file_out)
        for doc_type in self.pm.get_xml_types():
            try:
                # Sheet names in excel can have up to 31 chars
                worksheet = workbook.add_worksheet(name=doc_type[0:31])
            except:
                pass
            # create the output table for each document type
            self.pm.create_output_by_xml_type(doc_type)
            # write the header to the output sheet
            header = (row["Tag"] for row in self.pm.get_xml_tags_by_type(doc_type))
            worksheet.write_row(0, 0, header)
            # write the data row by row to the output sheet
            for row_number, row in enumerate(
                self.pm.get_output_by_xml_type(doc_type), start=1
            ):
                worksheet.write_row(row_number, 0, tuple(row))

        # Saves the new document
        workbook.close()
        return file_out_name

    # ------------------
    # Internal Functions
    # ------------------
    def _split_into_documents(self):
        out = True

        patts = set()
        idx_start = 0

        # every regex has to be a single capturing group
        num_groups = len(self.re_split) + 1

        # regex.split returns a list containing the resulting substrings.
        # If capturing parentheses are used in pattern, then the text of all groups in the pattern are also returned as part of the resulting list.
        # These groups are returned as elements *before* the actual resulting substring. I.e. if 4 capturing groups have been defined,
        # 4 items representing matches of these groups will be returned and the actual substring itself will be returned as the 5th item in the list.
        # the list can start with an empty string.

        list_raw = self.comp_re_split.split(self.file_content)

        if list_raw[0] is not None:
            if len(list_raw[0]) == 0:
                # ignore 1st emptyp string
                idx_start = 1

        doc_idx = 0
        for idx, item in enumerate(list_raw[idx_start:], start=1):
            if item:
                if idx % num_groups == 0:
                    # this is the actual content we want
                    validation_result = self._validate_document(item)
                    doc_idx += 1
                    if validation_result.valid:
                        # store the valid item
                        # self.xml_source.append(item.strip())
                        self.pm.store_doc(doc_idx, DocValidity.VALID, item.strip())
                    else:
                        out = False
                        # store the invalid item
                        # self.xml_source_invalid.append(
                        #     {
                        #         "Document": item.strip(),
                        #         "Validation Result": validation_result.output,
                        #     }
                        # )
                        self.pm.store_doc(
                            doc_idx,
                            DocValidity.INVALID,
                            item.strip(),
                            validation_result.output,
                        )
                else:
                    patts.add(item)

        self.doc_delimiters = list(patts)

        # once all data is processed and stored, create the indices
        self.pm.create_indices(IndexGroup.DOC_STORE)

        # print(self.xml_source_invalid)
        return out

    # @profile
    def _validate_document(self, xml_document):

        # initiate the validator object
        validator = XmlValidator()

        # validating the basic well-formedness of the xml document
        validation_result = validator.validate_doc(xml_document)

        return validation_result


# -------------------------------------------------------------------------------
# General Service Functions
# ------------------------------------------------------------------------------


# -------------------------------------------------------------------------------
# The script entry point
# -------------------------------------------------------------------------------

if __name__ == "__main__":

    from pprint import pprint

    path_name = "P:/Programming/Python/xml_examples/"
    # file_name = "pain.008.sepa.duplicates_and_invalid.xml"
    file_name = "mixed.pain.camt.xml"
    # file_name = "camt.054_sweden.xml"
    # file_name = "pain.008.sepa.xml"
    # file_name = "xml_test_data_small_2k.xml"
    # file_name = "xml_test_data_large.xml"
    # file_name = "books.xml"
    file_in = path_name + file_name

    db_path = "C:/Users/Dani/AppData/Local/Temp"
    # db_path = "P:/Programming/Python"

    # load the file into the file processor
    stopwatch = StopWatch(run_label="processing file")
    fp = FileProcessor(source_file=file_in, data_directory=db_path)
    print(stopwatch.time_run())

    # get the basic information for the file
    # print(f"FileProcessor object:\n{fp}")

    # get the full debug information
    # print(f"Debug Info:\n {fp.debug_info()}\n")

    # parse the documents within the file
    # this is where the actual XML processing happens
    stopwatch = StopWatch(run_label="parsing documents")
    fp.process_file()
    print(stopwatch.time_run())

    # print(f"pf doc types:\n{fp.doc_types}")

    # get the samples - this returns a dict with
    # the 1st document of each document tpye in the file
    # print(f"Inspect Sample:\n{fp.inspect_samples()}")

    # export the parsed documents to the excel file
    # stopwatch = StopWatch(run_label="creating excel")
    # fp.to_excel()
    # print(stopwatch.time_run())

    # get the document stats
    # file_stats = fp.get_processed_file_overview()
    # # print(file_stats)
    # for doc_type in file_stats:
    #     no_of_docs = file_stats[doc_type]["no_of_docs"]
    #     print(f"No of docs in type '{doc_type}': {no_of_docs}")
    #     for type_stat in file_stats[doc_type]["doc_type_stats"]:
    #         print(f"  {type_stat[0]}: {type_stat[1:]}")

    # get the document stats and samples
    doc_data = fp.get_processed_file_overview_with_samples()
    pprint(doc_data)