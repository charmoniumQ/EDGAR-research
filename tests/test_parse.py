import datetime
import edgar_code.parse as parse


# fields can be in a different order.
# fields can be dilimited by spaces or pipes
# see https://www.sec.gov/Archives/edgar/full-index/2016/QTR1/master.idx

def test_parse_index1() -> None:
    #pylint: disable=line-too-long
    raw_indexes = [
        (1995, 1, b'''Description:           Master Index of EDGAR Dissemination Feed by Form Type
Last Data Received:    March 31, 1995
Comments:              webmaster@sec.gov
Anonymous FTP:         ftp://ftp.sec.gov/edgar/




Form Type   Company Name                                                  CIK         Date Filed  File Name
---------------------------------------------------------------------------------------------------------------------------------------------
10-12B      COMPREHENSIVE CARE CORP                                       22872       1995-01-13  edgar/data/22872/0000022872-95-000004.txt           
10-12B/A    COMPUTER SCIENCES CORP                                        23082       1995-02-07  edgar/data/23082/0000912057-95-000343.txt           '''),
        (2016, 1, b'''Description:           Master Index of EDGAR Dissemination Feed
Last Data Received:    March 31, 2016
Comments:              webmaster@sec.gov
Anonymous FTP:         ftp://ftp.sec.gov/edgar/
Cloud HTTP:            https://www.sec.gov/Archives/




CIK|Company Name|Form Type|Date Filed|Filename
--------------------------------------------------------------------------------
1000032|BINCH JAMES G|4|2016-03-02|edgar/data/1000032/0001209191-16-104477.txt
1000032|BINCH JAMES G|4|2016-03-11|edgar/data/1000032/0001209191-16-107917.txt'''),
    ]

    parsed_indexes = [
        [
            parse.Index(
                year=1995,
                qtr=1,
                form_type='10-12B',
                company_name='COMPREHENSIVE CARE CORP',
                cik=22872,
                date_filed=datetime.date(1995, 1, 13),
                url='https://www.sec.gov/Archives/edgar/data/22872/0000022872-95-000004.txt',
            ),
            parse.Index(
                year=1995,
                qtr=1,
                form_type='10-12B/A',
                company_name='COMPUTER SCIENCES CORP',
                cik=23082,
                date_filed=datetime.date(1995, 2, 7),
                url='https://www.sec.gov/Archives/edgar/data/23082/0000912057-95-000343.txt'
            ),
        ], [
            parse.Index(
                year=2016,
                qtr=1,
                form_type='4',
                company_name='BINCH JAMES G',
                cik=1000032,
                date_filed=datetime.date(2016, 3, 2),
                url='https://www.sec.gov/Archives/edgar/data/1000032/0001209191-16-104477.txt',
            ),
            parse.Index(
                year=2016,
                qtr=1,
                form_type='4',
                company_name='BINCH JAMES G',
                cik=1000032,
                date_filed=datetime.date(2016, 3, 11),
                url='https://www.sec.gov/Archives/edgar/data/1000032/0001209191-16-107917.txt',
            ),
        ],
    ]
    for (year, qtr, raw_index), parsed_index in zip(raw_indexes, parsed_indexes):
        lines = iter(raw_index.split(b'\n'))
        header = parse.parse_header(lines)
        indexes = list(parse.parse_body(year, qtr, lines, header))
        assert indexes[:2] == parsed_index


def test_parse_sgml() -> None:
    raw_sgml1 = b'''<SEC-DOCUMENT>0001209191-16-107917.txt : 20160311
<SEC-HEADER>0001209191-16-107917.hdr.sgml : 20160311
<ACCEPTANCE-DATETIME>20160311172333
</SEC-HEADER>
<DOCUMENT>
<TYPE>not 4
<SEQUENCE>1
<FILENAME>attachment.xml
<DESCRIPTION>attachment or something
<TEXT>
this is an attachment
</TEXT>
</DOCUMENT>
<DOCUMENT>
<TYPE>4
<SEQUENCE>2
<FILENAME>doc4.xml
<DESCRIPTION>FORM 4 SUBMISSION
<TEXT>
this is where the doc should be
</TEXT>
</DOCUMENT>
</SEC-DOCUMENT>
    '''
    fileinfos = parse.sgml2fileinfos(raw_sgml1)
    assert parse.find_form(fileinfos, '4') == b'\nthis is where the doc should be\n'


def test_parse_html():
    #pylint: disable=line-too-long
    raw_html1 = b'''
<!-- ignores comments -->
<!-- has arbitrarily many attrs -->
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:ref="http://www.xbrl.org/2006/ref" xmlns:us-gaap="http://fasb.org/us-gaap/2017-01-31" xmlns:xbrldi="http://xbrl.org/2006/xbrldi" xmlns:naics="http://xbrl.sec.gov/naics/2017-01-31" xmlns:xbrli="http://www.xbrl.org/2003/instance">
<meta http-equiv="Content-Type" content="text/html; charset=windows-1252">
		<!-- random whitespace -->
<title>Document</title>
</head>
<body style="font-family:Times New Roman;font-size:10pt;">
<div style="line-height:120%;padding-top:4px;font-size:9pt;">
<span style="font-family:Arial;font-size:9pt;">
Hello
world.
</span>
</div>
<div style="line-height:120%;padding-top:4px;font-size:9pt;">
<span style="font-family:Arial;font-size:9pt;">
This is a paragraph.
</span>
</div>
</body>
</html>
'''


    expected_paragraphs = [
        'Document',
        'Hello world.',
        'This is a paragraph.',
    ]
    assert parse.is_html(b'<p>even one tiny </p> html tag')
    assert not parse.is_html(b'3 < 4 and 6 > 5')
    paragraphs = parse.html2paragraphs(raw_html1)
    real_paragraphs = filter(bool, map(lambda string: string.strip(), paragraphs))
    for paragraph, expected_paragraphs in zip(real_paragraphs, expected_paragraphs):
        assert paragraph.strip() == expected_paragraphs


def test_parse_text() -> None:
    text1 = b'''
<PAGE>

               UNITED STATES SECURITIES AND EXCHANGE COMMISSION
                            Washington, D.C. 20549

                            McDONALD'S CORPORATION
              (Exact name of registrant as specified in its charter)

Indicate by check mark whether the registrant (1) has filed all reports required
to be filed by Section 13 or 15(d) of the Securities Exchange Act of 1934 during
the preceding 12 months (or for such shorter period that the registrant was
required to file such reports), and (2) has been subject to such filing
requirements for the past 90 days. Yes [X]  No [ ]

<PAGE>

Part I

Item 1.  Business

McDonald's Corporation, the registrant, together with its subsidiaries, is
referred to herein as the "Company".

                                               9
'''
    expected_paragraph_starts = [
        'UNITED STATES',
        'McDONALD\'S CORPORATION',
        # eliminates word wrap
        'Indicate by check mark whether the registrant (1) has filed all reports required to',
        # eliminates <PAGE> markings
        'Part I',
        # eliminates double spaces
        'Item 1.  Business',
        'McDonald\'s Corporation, the registrant, together with its subsidiaries, is referred',
        '9',
    ]

    paragraphs = parse.text2paragraphs(text1)
    real_paragraphs = filter(bool, map(lambda string: string.strip(), paragraphs))
    for paragraph, expected_paragraph_start in zip(real_paragraphs, expected_paragraph_starts):
        assert paragraph.startswith(expected_paragraph_start)


def test_text2paragraphs() -> None:
    #pylint: disable=line-too-long
    paragraphs = [
        '               UNITED STATES SECURITIES AND EXCHANGE COMMISSION                            Washington, D.C. 20549',
        "                            McDONALD'S CORPORATION              (Exact name of registrant as specified in its charter)",
        '  Indicate by check mark whether the registrant (1) has filed all reports required to be filed by Section 13 or 15(d) of the',
        'Item 1.	Business',
        'Item 1. Business',
        ' ',
        '3',
        'McDonald’s Corporation, the registrant, together with its subsidiaries, is referred to herein as the ”Company”.',
        '(table of contents)'
        ]
    expected_paragraphs = [
        # gets rid of redundant whitespace
        'UNITED STATES SECURITIES AND EXCHANGE COMMISSION Washington, D.C. 20549',
        'McDONALD\'S CORPORATION (Exact name of registrant as specified in its charter)',
        # strips spaces (start and end)
        'Indicate by check mark whether the registrant (1) has filed all reports required to be filed by Section 13 or 15(d) of the',
        # eliminates whitespace lines
        # eliminates page numbers, so no '3',
        # gets rid of tabs
        'Item 1. Business',
        # gets rid of nonbreaking space
        'Item 1. Business',
        # fixes non-ascii quotes
        'McDonald\'s Corporation, the registrant, together with its subsidiaries, is referred to herein as the "Company".',
        # removes recurring link to table of contents
    ]
    paragraphs = parse.clean_paragraphs(paragraphs)
    for paragraph, expected_paragraph in zip(paragraphs, expected_paragraphs):
        assert paragraph == expected_paragraph


def test_remove_header() -> None:
    test_cases = [
        (2, [
            'Part I',
            'Item 1. Business',
        ]),
        (2, [
            'Table of contents',
            'Part I',
            'Reference to Part I before it occurs',
            'unrelated paragraph.',
            'Part I',
            'Item 1. Business',
        ]),
    ]
    for num, paragraphs in test_cases:
        assert parse.remove_header(paragraphs) == paragraphs[-num:]


def test_paragraphs2rf() -> None:
    #pylint: disable=line-too-long
    test_cases_pre_2006 = [
        # pre 2006
        [
            'Fixed charges consist of interest expense and the estimated interest component of rent expense.',
            # case doesn't matter
            'ITEM 7. MANAGEMENT\'S DISCUSSION AND ANALYSIS OF FINANCIAL CONDITION AND RESULTS OF OPERATIONS',
            'this is data',
            'ITEM 8. FINANCIAL STATEMENTS AND SUPPLEMENTARY DATA',
        ], [
            # doesn't include passing references to item 7
            'in Part II, Item 7, pages 7 through 17 of this Form 10-K.',
            'Item 7. Management\'s discussion and analysis of financial condition and results of operations',
            'this is data',
            'Item 8. Financial statements and supplementary data',
        ],
    ]
    test_cases_post_2006 = [
        # pre 2006
        [
            # doesn't include passing references
            'ITEM 1A., which is of interest to people.',
            # but still allows full title
            'ITEM 1A. RISK FACTORS AND CAUTIONARY STATEMENT REGARDING FORWARD-LOOKING STATEMENTS',
            'this is data',
            'ITEM 2. PROPERTIES',
        ], [
            # but doesn't require title
            'Item 1A.',
            'this is data',
            'Item 2.',
        ],
    ]
    for test_case in test_cases_pre_2006:
        assert parse.paragraphs2rf(test_case, True) == ['this is data']
    for test_case in test_cases_post_2006:
        assert parse.paragraphs2rf(test_case, False) == ['this is data']
