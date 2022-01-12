# ------------------
# Imports
# ------------------
import xml.sax
from dataclasses import dataclass

# ------------------
# Classes
# ------------------


@dataclass
class ValidationResult:
    valid: bool
    output: str


class DummyHandler(xml.sax.ContentHandler):
    def __init__(self):
        pass

    def startElement(self, tag, attributes):
        pass

    def endElement(self, tag):
        pass

    def characters(self, content):
        pass


class XmlValidator:
    def __init__(self):

        self.dummy_handler = DummyHandler()

    def validate_doc(self, string_to_validate):

        ret_val = ValidationResult(valid=True, output="Valid xml document!")

        try:
            xml.sax.parseString(string_to_validate, self.dummy_handler)
        except Exception as e:
            ret_val = ValidationResult(valid=False, output=f"Invalid xml document! The following error occured: {e}")
            
        return ret_val


if __name__ == "__main__":

    xml_doc = """<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pain.008.001.02" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">"""
    xml_doc_2 = """<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pain.008.001.02" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><CstmrDrctDbtInitn><GrpHdr><MsgId>Message 9 200131106</MsgId><CreDtTm>2013-10-08T12:57:52</CreDtTm><NbOfTxs>2</NbOfTxs><CtrlSum>0.30</CtrlSum><InitgPty><Nm>PILOTFORETAG B</Nm><Id><OrgId><Othr><Id>3321251633</Id><SchmeNm><Cd>BANK</Cd></SchmeNm></Othr></OrgId></Id></InitgPty></GrpHdr><PmtInf><PmtInfId>SEND PAYMENT VER 009</PmtInfId><PmtMtd>DD</PmtMtd><BtchBookg>true</BtchBookg><NbOfTxs>2</NbOfTxs><CtrlSum>0.30</CtrlSum><PmtTpInf><SvcLvl><Cd>SEPA</Cd></SvcLvl><LclInstrm><Cd>B2B</Cd></LclInstrm><SeqTp>RCUR</SeqTp></PmtTpInf><ReqdColltnDt>2013-11-08</ReqdColltnDt><Cdtr><Nm>PILOTFORETAG B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr></Cdtr><CdtrAcct><Id><IBAN>NL58HAND0718128982</IBAN></Id></CdtrAcct><CdtrAgt><FinInstnId><BIC>HANDNL2A</BIC></FinInstnId></CdtrAgt><CdtrSchmeId><Id><PrvtId><Othr><Id>SE92ZZZ3321251633</Id><SchmeNm><Prtry>SEPA</Prtry></SchmeNm></Othr></PrvtId></Id></CdtrSchmeId><DrctDbtTxInf><PmtId><EndToEndId>BMO1 SEND PROD VER 10 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">0.20</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER8</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>Pilot B</Nm><PstlAdr><Ctry>NL</Ctry></PstlAdr><Id><OrgId><Othr><Id>3321251633</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>DE50514206000011350044</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 1</Ustrd></RmtInf></DrctDbtTxInf><DrctDbtTxInf><PmtId><EndToEndId>BMO2 SEND PROD VER 11 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">0.10</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER9</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>PILOT B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr><Id><OrgId><Othr><Id>5567578793</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>DE84514206000011350014</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 2</Ustrd></RmtInf></DrctDbtTxInf></PmtInf></CstmrDrctDbtInitn></Document>"""

    validator = XmlValidator()

    validation_result = validator.validate_doc(xml_doc)
    validation_result_2 = validator.validate_doc(xml_doc_2)
    validator = None
    print(f"Validation result doc 1: {validation_result}")
    print(f"Validation result doc 2: {validation_result_2}")
