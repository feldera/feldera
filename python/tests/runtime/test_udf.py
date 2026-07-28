from datetime import date, datetime, time
from decimal import Decimal
import json
import unittest

from feldera import PipelineBuilder
from tests import TEST_CLIENT
from tests.platform.helper import PipelineTestCase
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_WORKERS, FELDERA_TEST_NUM_HOSTS


class TestUDF(PipelineTestCase):
    def test_local(self):
        sql = """
CREATE TYPE my_struct AS (
   i INT,
   s VARCHAR
);

CREATE TABLE t (
    i INT,
    ti TINYINT,
    si SMALLINT,
    bi BIGINT,
    r REAL,
    d DOUBLE,
    bin VARBINARY,
    dt DATE,
    t TIME,
    ts TIMESTAMP,
    a INT ARRAY,
    m MAP<VARCHAR, VARCHAR>,
    v VARIANT,
    b BOOLEAN,
    dc DECIMAL(7,2),
    s VARCHAR,
    ms MY_STRUCT
) with ('materialized' = 'true');

CREATE FUNCTION bool2bool(i BOOLEAN) RETURNS BOOLEAN;
CREATE FUNCTION nbool2nbool(i BOOLEAN NOT NULL) RETURNS BOOLEAN NOT NULL;

CREATE FUNCTION i2i(i INT) RETURNS INT;
CREATE FUNCTION ni2ni(i INT NOT NULL) RETURNS INT NOT NULL;

CREATE FUNCTION ti2ti(i TINYINT) RETURNS TINYINT;
CREATE FUNCTION nti2nti(i TINYINT NOT NULL) RETURNS TINYINT NOT NULL;

CREATE FUNCTION si2si(i SMALLINT) RETURNS SMALLINT;
CREATE FUNCTION nsi2nsi(i SMALLINT NOT NULL) RETURNS SMALLINT NOT NULL;

CREATE FUNCTION bi2bi(i BIGINT) RETURNS BIGINT;
CREATE FUNCTION nbi2nbi(i BIGINT NOT NULL) RETURNS BIGINT NOT NULL;

CREATE FUNCTION r2r(i REAL) RETURNS REAL;
CREATE FUNCTION nr2nr(i REAL NOT NULL) RETURNS REAL NOT NULL;

CREATE FUNCTION d2d(i DOUBLE) RETURNS DOUBLE;
CREATE FUNCTION nd2nd(i DOUBLE NOT NULL) RETURNS DOUBLE NOT NULL;

CREATE FUNCTION bin2bin(i VARBINARY) RETURNS VARBINARY;
CREATE FUNCTION nbin2nbin(i VARBINARY NOT NULL) RETURNS VARBINARY NOT NULL;

CREATE FUNCTION date2date(i DATE) RETURNS DATE;
CREATE FUNCTION ndate2ndate(i DATE NOT NULL) RETURNS DATE NOT NULL;

CREATE FUNCTION ts2ts(i TIMESTAMP) RETURNS TIMESTAMP;
CREATE FUNCTION nts2nts(i TIMESTAMP NOT NULL) RETURNS TIMESTAMP NOT NULL;

CREATE FUNCTION t2t(i TIME) RETURNS TIME;
CREATE FUNCTION nt2nt(i TIME NOT NULL) RETURNS TIME NOT NULL;

CREATE FUNCTION arr2arr(i INT ARRAY) RETURNS INT ARRAY;
CREATE FUNCTION narr2narr(i INT ARRAY NOT NULL) RETURNS INT ARRAY NOT NULL;

CREATE FUNCTION map2map(i MAP<VARCHAR, VARCHAR>) RETURNS MAP<VARCHAR, VARCHAR>;
CREATE FUNCTION nmap2nmap(i MAP<VARCHAR, VARCHAR> NOT NULL) RETURNS MAP<VARCHAR, VARCHAR> NOT NULL;

CREATE FUNCTION var2var(i VARIANT) RETURNS VARIANT;
CREATE FUNCTION nvar2nvar(i VARIANT NOT NULL) RETURNS VARIANT NOT NULL;

CREATE FUNCTION dec2dec(i DECIMAL(7, 2)) RETURNS DECIMAL(7, 2);
CREATE FUNCTION ndec2ndec(i DECIMAL(7, 2) NOT NULL) RETURNS DECIMAL(7, 2) NOT NULL;

CREATE FUNCTION str2str(i VARCHAR) RETURNS VARCHAR;
CREATE FUNCTION nstr2nstr(i VARCHAR NOT NULL) RETURNS VARCHAR NOT NULL;

CREATE FUNCTION struct2struct(i my_struct) RETURNS my_struct;
CREATE FUNCTION nstruct2nstruct(i my_struct NOT NULL) RETURNS my_struct NOT NULL;

CREATE MATERIALIZED VIEW v AS
SELECT
    bool2bool(b),
    nbool2nbool(COALESCE(b, FALSE)),
    i2i(i),
    ni2ni(COALESCE(i, 0)),
    ti2ti(ti),
    nti2nti(COALESCE(ti, 0)),
    si2si(si),
    nsi2nsi(COALESCE(si, 0)),
    bi2bi(bi),
    nbi2nbi(COALESCE(bi, 0)),
    r2r(r),
    nr2nr(COALESCE(r, 0.0)),
    d2d(d),
    nd2nd(COALESCE(d, 0.0)),
    bin2bin(bin),
    nbin2nbin(COALESCE(bin, x'')),
    date2date(dt),
    ndate2ndate(COALESCE(dt, DATE '2023-01-01')),
    ts2ts(ts),
    nts2nts(COALESCE(ts, TIMESTAMP '2023-01-01 00:00:00')),
    t2t(t),
    nt2nt(COALESCE(t, TIME '00:00:00')),
    arr2arr(a),
    narr2narr(a),
    map2map(m),
    nmap2nmap(m),
    var2var(v),
    nvar2nvar(COALESCE(v, VARIANTNULL())),
    dec2dec(dc),
    ndec2ndec(COALESCE(dc, 0)),
    str2str(s),
    nstr2nstr(COALESCE(s, ''))
FROM
    t;
        """

        udfs = """
use feldera_sqllib::F32;
use crate::*;

pub fn bool2bool(i: Option<bool>) -> Result<Option<bool>, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn nbool2nbool(i: bool) -> Result<bool, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn i2i(i: Option<i32>) -> Result<Option<i32>, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn ni2ni(i: i32) -> Result<i32, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn ti2ti(i: Option<i8>) -> Result<Option<i8>, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn nti2nti(i: i8) -> Result<i8, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn si2si(i: Option<i16>) -> Result<Option<i16>, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn nsi2nsi(i: i16) -> Result<i16, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn bi2bi(i: Option<i64>) -> Result<Option<i64>, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn nbi2nbi(i: i64) -> Result<i64, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn r2r(i: Option<F32>) -> Result<Option<F32>, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn nr2nr(i: F32) -> Result<F32, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn d2d(i: Option<F64>) -> Result<Option<F64>, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn nd2nd(i: F64) -> Result<F64, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn bin2bin(i: Option<ByteArray>) -> Result<Option<ByteArray>, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn nbin2nbin(i: ByteArray) -> Result<ByteArray, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn date2date(i: Option<Date>) -> Result<Option<Date>, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn ndate2ndate(i: Date) -> Result<Date, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn ts2ts(i: Option<Timestamp>) -> Result<Option<Timestamp>, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn nts2nts(i: Timestamp) -> Result<Timestamp, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn t2t(i: Option<Time>) -> Result<Option<Time>, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn nt2nt(i: Time) -> Result<Time, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn arr2arr(i: Option<Array<Option<i32>>>) -> Result<Option<Array<Option<i32>>>, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn narr2narr(i: Array<Option<i32>>) -> Result<Array<Option<i32>>, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn map2map(i: Option<Map<SqlString, Option<SqlString>>>) -> Result<Option<Map<SqlString, Option<SqlString>>>, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn nmap2nmap(i: Map<SqlString, Option<SqlString>>) -> Result<Map<SqlString, Option<SqlString>>, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn var2var(i: Option<Variant>) -> Result<Option<Variant>, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn nvar2nvar(i: Variant) -> Result<Variant, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn dec2dec(i: Option<SqlDecimal<7, 2>>) -> Result<Option<SqlDecimal<7, 2>>, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn ndec2ndec(i: SqlDecimal<7, 2>) -> Result<SqlDecimal<7, 2>, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn str2str(i: Option<SqlString>) -> Result<Option<SqlString>, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn nstr2nstr(i: SqlString) -> Result<SqlString, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn struct2struct(i: Option<Tup2<Option<i32>, Option<SqlString>>>) -> Result<Option<Tup2<Option<i32>, Option<SqlString>>>, Box<dyn std::error::Error>> {
    Ok(i)
}
pub fn nstruct2nstruct(i: Tup2<Option<i32>, Option<SqlString>>) -> Result<Tup2<Option<i32>, Option<SqlString>>, Box<dyn std::error::Error>> {
    Ok(i)
}
        """

        # Under the FlatVariant representation (FELDERA_FLAT_VARIANT), UDFs receive
        # and return FlatVariant; the identity UDF bodies only need the type
        # name swapped.
        import os
        import re

        if os.environ.get("FELDERA_FLAT_VARIANT", "").lower() in ("1", "on", "true"):
            udfs = re.sub(r"\bVariant\b", "FlatVariant", udfs)

        pipeline = PipelineBuilder(
            TEST_CLIENT,
            name=self.register_for_cleanup("test_udfs"),
            sql=sql,
            udf_rust=udfs,
            runtime_config=RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
            ),
        ).create_or_replace()

        pipeline.start()

        # A VARIANT holding a JSON string, as opposed to a JSON object. The
        # distinction decides how the ad-hoc layer renders it below.
        variant = '{"foo": "bar"}'

        pipeline.input_json(
            "t",
            [
                {
                    "i": 1,
                    "ti": -2,
                    "si": 3,
                    "bi": -4,
                    "r": 0.5,
                    "d": 1e-5,
                    "bin": [],
                    "dt": "2024-09-25",
                    "t": "13:05:00",
                    "ts": "2024-09-25 13:05:00",
                    "a": [1, 2, 3, 4, 5],
                    "m": {"foo": "bar"},
                    "v": variant,
                    "b": True,
                    "dc": "123.45",
                    "s": "foobar",
                }
            ],
            wait=True,
        )

        # `wait=True` above establishes that the circuit processed the input.
        # Arrow IPC rather than JSON, because JSON cannot carry VARBINARY or DECIMAL
        # faithfully (https://github.com/feldera/feldera/issues/4219).
        #
        # Columns are unnamed in the view, so they arrive as EXPR$0..EXPR$31 in
        # SELECT-list order. Every UDF is the identity, hence each pair of
        # columns repeats one input value.
        rows = list(pipeline.query_arrow_dicts("SELECT * FROM v"))
        assert rows == [
            {
                # bool2bool, nbool2nbool
                "EXPR$0": True,
                "EXPR$1": True,
                # i2i, ni2ni
                "EXPR$2": 1,
                "EXPR$3": 1,
                # ti2ti, nti2nti
                "EXPR$4": -2,
                "EXPR$5": -2,
                # si2si, nsi2nsi
                "EXPR$6": 3,
                "EXPR$7": 3,
                # bi2bi, nbi2nbi
                "EXPR$8": -4,
                "EXPR$9": -4,
                # r2r, nr2nr
                "EXPR$10": 0.5,
                "EXPR$11": 0.5,
                # d2d, nd2nd
                "EXPR$12": 1e-05,
                "EXPR$13": 1e-05,
                # bin2bin, nbin2nbin
                "EXPR$14": b"",
                "EXPR$15": b"",
                # date2date, ndate2ndate
                "EXPR$16": date(2024, 9, 25),
                "EXPR$17": date(2024, 9, 25),
                # ts2ts, nts2nts
                "EXPR$18": datetime(2024, 9, 25, 13, 5),
                "EXPR$19": datetime(2024, 9, 25, 13, 5),
                # t2t, nt2nt
                "EXPR$20": time(13, 5),
                "EXPR$21": time(13, 5),
                # arr2arr, narr2narr
                "EXPR$22": [1, 2, 3, 4, 5],
                "EXPR$23": [1, 2, 3, 4, 5],
                # map2map, nmap2nmap. Arrow preserves MAP order, so a map
                # arrives as a list of key-value tuples.
                "EXPR$24": [("foo", "bar")],
                "EXPR$25": [("foo", "bar")],
                # var2var, nvar2nvar. The ad-hoc layer renders a VARIANT as its
                # JSON text; the text of a JSON string is that string quoted.
                "EXPR$26": json.dumps(variant),
                "EXPR$27": json.dumps(variant),
                # dec2dec, ndec2ndec
                "EXPR$28": Decimal("123.45"),
                "EXPR$29": Decimal("123.45"),
                # str2str, nstr2nstr
                "EXPR$30": "foobar",
                "EXPR$31": "foobar",
            }
        ]

        pipeline.stop(force=True)


if __name__ == "__main__":
    unittest.main()
