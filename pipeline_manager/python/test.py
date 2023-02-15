import tempfile
import os

from dbsp import DBSPConnection
from dbsp import DBSPPipelineConfig
from dbsp import InputEndpointConfig
from dbsp import OutputEndpointConfig
from dbsp import TransportConfig
from dbsp import FormatConfig
from dbsp import FileInputConfig
from dbsp import FileOutputConfig
from dbsp import CsvParserConfig
from dbsp import CsvEncoderConfig

demographics = """4218196001337,172,1,866,131,34,12043,42.684,-74.4939,8509,4,1968-10-03
4351161559407816183,107,0,319,34,34,13027,43.162,-76.3237,31637,357,1948-06-07
4192832764832,237,0,61,355,18,70605,30.1693,-93.2218,124276,260,1989-05-30
4238849696532874,27,0,276,478,6,6513,41.3072,-72.8654,128884,420,1950-12-26
4514627048281480,215,0,810,43,43,76021,32.8536,-97.1358,46885,292,1972-05-22
3517182278248964,344,1,471,169,18,70706,30.6085,-90.9038,71335,102,1984-06-18
213193010310510,190,0,831,573,45,23146,37.7337,-77.7,3093,341,1969-03-06
4065133387262473,90,1,118,206,31,8318,39.5691,-75.163,12702,380,1944-12-27
630447468723,69,0,808,498,43,79766,31.7827,-102.3449,137390,398,1927-08-23
2222913619399092,24,0,365,711,2,72774,35.9082,-94.2304,6121,7,1971-01-26
4251354278496,351,1,840,599,4,95122,37.3293,-121.8339,973849,440,1998-02-26
4874016400529,231,1,696,146,43,78416,27.7536,-97.4347,306433,464,1998-08-02
345331586923222,303,0,355,280,34,14075,42.7334,-78.8389,41937,282,2000-08-16
6541458685014295,333,0,657,33,4,93308,35.4244,-119.0433,520197,98,1977-09-29
3508835615951480,111,1,489,136,5,80918,38.9129,-104.7734,525713,324,1945-08-14
6577738721489511,285,0,329,179,10,30096,33.9845,-84.1529,103980,162,1975-01-06
4163287083172781,343,0,872,179,10,30096,33.9845,-84.1529,103980,228,1994-01-23
4143455812236231660,125,1,682,108,14,60645,42.0086,-87.6947,2680484,244,1948-09-26
3573467065627293,176,1,631,591,20,21804,38.3508,-75.5338,68541,358,1959-04-04
4708053100330923275,240,1,465,563,38,15851,41.0629,-78.8961,6671,257,1962-11-24
6011193149190586,154,1,688,545,37,97230,45.5472,-122.5001,841711,429,1990-11-15
4314737996507527356,268,0,92,438,3,85203,33.437,-111.8057,478404,387,1930-10-26
6011495788568554,241,0,148,353,15,47905,40.4001,-86.8602,98078,475,1975-08-02
4082400842710944,169,1,479,418,19,2048,42.0212,-71.2178,23184,45,1978-08-09
2706999386774968,111,1,353,592,44,84123,40.6596,-111.9193,594043,1,1959-02-24
4471349361831,235,1,27,479,1,35761,34.9,-86.4487,11630,252,1970-03-15
30190659401395,210,0,32,562,13,83440,43.81,-111.789,35470,280,2002-04-06
377468071545168,296,0,884,177,17,41035,38.7049,-84.6237,10785,497,1969-12-03
4824771093241,199,0,174,297,31,7030,40.7445,-74.0329,50005,332,1934-04-12
"""

transactions = """
2008-01-01 01:11:21,4251354278496,548,2,45.74,7f8b297e3e990c368ce365cc6f8d2bef,1199178681,38.226336,-120.947107,0
2008-01-01 05:27:47,4251354278496,605,2,80.8,29532183df62ac0b5b09798b3e419c0e,1199194067,36.704567,-122.520603,0
2008-01-01 07:34:11,4251354278496,474,2,75.01,5d708c8a59e61765a9a8fd75cd273895,1199201651,38.293734,-121.480201,0
2008-01-01 05:01:11,2222913619399092,497,2,7.15,880b191586e2d55b38b353bea0003b20,1199192471,35.069558,-94.182218,0
2008-01-01 05:07:11,2222913619399092,522,2,46.3,61efa4eb230a57e25bf7e4ac36afe117,1199192831,35.64587,-94.888836,0
2008-01-02 09:43:16,4824771093241,117,2,1.25,0a203757f41995b5191a2f5f4ba37225,1199295796,40.999857,-73.948364,0
2008-01-02 11:08:47,4824771093241,332,2,3.74,c7e55ca7b11bc647e1d9e515df5ae244,1199300927,41.702109,-74.728905,0
2008-01-02 11:09:05,4211457758471,117,2,58.49,7b8631718c9006e9cd21718e24b67a35,1199300945,36.541407,-122.872482,0
2008-01-02 11:10:21,4521746535378012,208,2,38.05,ee1158a1d18b2bff3ccbcb7f240af2ec,1199301021,41.406169,-88.699498,0
2008-01-02 11:10:26,3547625380966414,208,2,160.59,7cb077bb10b79c257194775bc7162be0,1199301026,44.57281,-69.605028,0
2008-01-02 11:10:37,4824771093241,107,2,193.13,4ea3c5e44e2bdf601489bb1fb49f7991,1199301037,40.862917,-73.170228,0
2008-01-03 05:38:14,4082400842710944,231,2,17.04,abb639d206ea30d766b0fae771cb92e3,1199367494,41.13911,-71.434731,0
2008-01-04 00:58:43,4082400842710944,208,2,37.78,775c2e16e71852ce2f0f98c14d2aa2cd,1199437123,41.952015,-70.514457,0
2008-01-04 04:23:04,4082400842710944,381,2,89.9,c0c039baa4d0e62e4b16e0690ac54313,1199449384,42.331448,-70.495846,0
"""

sql_code = """
CREATE TABLE demographics (
    cc_num FLOAT64 NOT NULL,
    first STRING,
    gender STRING,
    street STRING,
    city STRING,
    state STRING,
    zip INTEGER,
    lat FLOAT64,
    long FLOAT64,
    city_pop INTEGER,
    job STRING,
    dob STRING
);

CREATE TABLE transactions (
    trans_date_trans_time TIMESTAMP NOT NULL,
    cc_num FLOAT64 NOT NULL,
    merchant STRING,
    category STRING,
    amt FLOAT64,
    trans_num STRING,
    unix_time INTEGER,
    merch_lat FLOAT64,
    merch_long FLOAT64,
    is_fraud INTEGER
);

CREATE VIEW transactions_with_demographics as
    SELECT
        transactions.trans_date_trans_time,
        transactions.cc_num,
        demographics.first,
        demographics.city
    FROM
        transactions JOIN demographics
        ON transactions.cc_num = demographics.cc_num;"""

def main():
    demfd, dempath = tempfile.mkstemp(suffix='.csv', prefix='demographics', text=True)
    with os.fdopen(demfd, 'w') as f:
        f.write(demographics)
    print("Demographics data written to '" + dempath + "'")

    transfd, transpath = tempfile.mkstemp(suffix='.csv', prefix='transactions', text=True)
    with os.fdopen(transfd, 'w') as f:
        f.write(transactions)

    outfd, outpath = tempfile.mkstemp(suffix='.csv', prefix='output', text=True)
    os.close(outfd)

    dbsp = DBSPConnection()
    print("Connection established")

    project = dbsp.new_project(name = "foo", sql_code = sql_code)
    print("Project created")

    status = project.status()
    print("Project status: " + status)

    config = DBSPPipelineConfig(project, 6)
    config.add_input(
            "DEMOGRAPHICS",
            InputEndpointConfig(
                transport = TransportConfig(
                    name = "file",
                    config = FileInputConfig.from_dict({ 'path': dempath })),
                format_ = FormatConfig(
                    name = "csv",
                    config = CsvParserConfig(input_stream = 'DEMOGRAPHICS'))))
    config.add_input(
            "TRANSACTIONS",
            InputEndpointConfig(
                transport = TransportConfig(
                    name = "file",
                    config = FileInputConfig.from_dict({ 'path': transpath })),
                format_ = FormatConfig(
                    name = "csv",
                    config = CsvParserConfig(input_stream = 'TRANSACTIONS'))))

    config.add_output(
            "TRANSACTIONS_WITH_DEMOGRAPHICS",
            OutputEndpointConfig(
                stream = 'TRANSACTIONS_WITH_DEMOGRAPHICS',
                transport = TransportConfig(
                    name = "file",
                    config = FileOutputConfig.from_dict({ 'path': outpath })),
                format_ = FormatConfig(
                    name = "csv",
                    config = CsvEncoderConfig(buffer_size_records = 1000000))))

    project.compile()
    print("Project compiled")

    status = project.status()
    print("Project status: " + status)

    pipeline = config.run()
    print("Pipeline is running")

    print("Pipeline status: " + str(pipeline.status()))
    print("Pipeline metadata: " + str(pipeline.metadata()))

    pipeline.pause()
    print("Pipeline paused")

    # pipeline.shutdown()
    # print("Pipeline terminated")

    pipeline.delete()
    print("Pipeline deleted")

    with open(outpath, 'r') as outfile:
        output = outfile.read()

    print("Output read from '" + outpath + "':")
    print(output)


if __name__ == "__main__":
    main()
