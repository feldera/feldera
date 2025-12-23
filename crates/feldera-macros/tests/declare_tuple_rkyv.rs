use feldera_sqllib::SqlString;

feldera_macros::declare_tuple! { Tup1<T1> }
feldera_macros::declare_tuple! { Tup2<T1, T2> }
feldera_macros::declare_tuple! { Tup12<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12> }

type OptStr = Option<SqlString>;
type Tup12Opt = Tup12<
    OptStr,
    OptStr,
    OptStr,
    OptStr,
    OptStr,
    OptStr,
    OptStr,
    OptStr,
    OptStr,
    OptStr,
    OptStr,
    OptStr,
>;

#[test]
fn rkyv_roundtrip_with_option_sqlstring() {
    let tup1_some = Tup1::new(Some(SqlString::from_ref("hello")));
    assert_eq!(tup1_some.get_0().as_ref().cloned().unwrap().str(), "hello");

    let bytes1_some = dbsp::storage::file::to_bytes(&tup1_some).unwrap();
    let restored1_some: Tup1<Option<SqlString>> =
        dbsp::trace::unaligned_deserialize(&bytes1_some[..]);
    assert_eq!(restored1_some, tup1_some);

    let tup1_none: Tup1<Option<SqlString>> = Tup1::new(None);
    let bytes1_none = dbsp::storage::file::to_bytes(&tup1_none).unwrap();
    let restored1_none: Tup1<Option<SqlString>> =
        dbsp::trace::unaligned_deserialize(&bytes1_none[..]);
    assert_eq!(restored1_none, tup1_none);
    assert_eq!(
        bytes1_none.len(),
        core::mem::size_of::<ArchivedTup1<Option<SqlString>>>()
    );
    assert!(bytes1_none.len() < bytes1_some.len());

    let tup2_some = Tup2::new(
        Some(SqlString::from_ref("hello")),
        Some(SqlString::from_ref("world")),
    );
    assert_eq!(tup2_some.get_0().as_ref().cloned().unwrap().str(), "hello");
    assert_eq!(tup2_some.get_1().as_ref().cloned().unwrap().str(), "world");

    let bytes2_some = dbsp::storage::file::to_bytes(&tup2_some).unwrap();
    let restored2_some: Tup2<Option<SqlString>, Option<SqlString>> =
        dbsp::trace::unaligned_deserialize(&bytes2_some[..]);
    assert_eq!(restored2_some, tup2_some);

    let tup2_none: Tup2<Option<SqlString>, Option<SqlString>> = Tup2::new(None, None);
    let bytes2_none = dbsp::storage::file::to_bytes(&tup2_none).unwrap();
    let restored2_none: Tup2<Option<SqlString>, Option<SqlString>> =
        dbsp::trace::unaligned_deserialize(&bytes2_none[..]);
    assert_eq!(restored2_none, tup2_none);
    assert_eq!(
        bytes2_none.len(),
        core::mem::size_of::<ArchivedTup2<Option<SqlString>, Option<SqlString>>>()
    );
    assert!(bytes2_none.len() < bytes2_some.len());
}

#[test]
fn rkyv_archived_getters() {
    let tup1_none: Tup1<Option<SqlString>> = Tup1::new(None);
    let bytes1_none = dbsp::storage::file::to_bytes(&tup1_none).unwrap();
    let archived1_none =
        unsafe { rkyv::archived_root::<Tup1<Option<SqlString>>>(&bytes1_none[..]) };
    let t0_none = archived1_none.get_t0();
    assert!(t0_none.is_none());

    let tup2_10: Tup2<Option<SqlString>, Option<i32>> =
        Tup2::new(Some(SqlString::from_ref("hi")), None);
    let bytes2_10 = dbsp::storage::file::to_bytes(&tup2_10).unwrap();
    let archived2_10 =
        unsafe { rkyv::archived_root::<Tup2<Option<SqlString>, Option<i32>>>(&bytes2_10[..]) };
    let t0_10 = archived2_10.get_t0();
    let t1_10 = archived2_10.get_t1();
    assert!(t0_10.is_some());
    assert!(t1_10.is_none());
    assert_eq!(t0_10.as_ref().unwrap().as_str(), "hi");
}

#[test]
fn rkyv_tup12_all_none_size() {
    let tup: Tup12Opt = Tup12Opt {
        11: Some(SqlString::from("12345678")),
        ..Default::default()
    };
    let bytes = dbsp::storage::file::to_bytes(&tup).unwrap();
    assert_eq!(
        bytes.len(),
        56,
        "expected compact encoding, got {} bytes",
        bytes.len()
    );
    let tup_archived = unsafe { rkyv::archived_root::<Tup12Opt>(&bytes[..]) };
    assert_eq!(
        tup_archived.get_t11().as_ref().unwrap().as_str(),
        "12345678"
    );
}
