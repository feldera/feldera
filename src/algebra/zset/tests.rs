use crate::algebra::{
    finite_map::FiniteMap,
    zset::{ZSet, ZSetHashMap},
    AddAssignByRef, AddByRef, CheckedInt, HasZero, NegByRef,
};
use std::{
    cmp::Ordering,
    fmt::{Display, Error, Formatter},
};

type CheckedI64 = CheckedInt<i64>;

#[test]
fn zset_integer_tests() {
    let mut z = ZSetHashMap::<i64, i64>::with_capacity(5);
    assert_eq!(0, z.support_size());
    assert_eq!("{}", z.to_string());
    assert_eq!(0, z.lookup(&0)); // not present -> weight 0
    assert_eq!(z, ZSetHashMap::<i64, i64>::zero());
    assert!(z.is_zero());
    let z2 = ZSetHashMap::<i64, i64>::new();
    assert_eq!(z, z2);

    z.increment(&0, 1);
    assert_eq!(1, z.support_size());
    assert_eq!("{0=>1}", z.to_string());
    assert_eq!(1, z.lookup(&0));
    assert_eq!(0, z.lookup(&1));
    assert_ne!(z, ZSetHashMap::<i64, i64>::zero());
    assert!(!z.is_zero());

    z.increment(&2, 0);
    assert_eq!(1, z.support_size());
    assert_eq!("{0=>1}", z.to_string());

    z.increment(&1, -1);
    assert_eq!(2, z.support_size());
    assert_eq!("{0=>1,1=>-1}", z.to_string());

    z.increment(&-1, 1);
    assert_eq!(3, z.support_size());
    assert_eq!("{-1=>1,0=>1,1=>-1}", z.to_string());

    let d = z.distinct();
    assert_eq!(2, d.support_size());
    assert_eq!("{-1=>1,0=>1}", d.to_string());

    let d = z.neg_by_ref();
    assert_eq!(3, d.support_size());
    assert_eq!("{-1=>-1,0=>-1,1=>1}", d.to_string());
    assert_ne!(d, z);

    z.increment(&1, 1);
    assert_eq!(2, z.support_size());
    assert_eq!("{-1=>1,0=>1}", z.to_string());

    let mut z2 = z.clone().add_by_ref(&z);
    assert_eq!(2, z2.support_size());
    assert_eq!("{-1=>2,0=>2}", z2.to_string());

    z2.add_assign_by_ref(&z);
    assert_eq!(2, z2.support_size());
    assert_eq!("{-1=>3,0=>3}", z2.to_string());
}

#[test]
fn checked_zset_integer_weights_tests() {
    let mut z = ZSetHashMap::<i64, CheckedI64>::with_capacity(5);
    assert_eq!(0, z.support_size());
    assert_eq!("{}", z.to_string());
    assert_eq!(CheckedI64::from(0), z.lookup(&0)); // not present -> weight 0
    assert_eq!(z, ZSetHashMap::<i64, CheckedI64>::zero());
    assert!(z.is_zero());
    let z2 = ZSetHashMap::<i64, CheckedI64>::new();
    assert_eq!(z, z2);

    z.increment(&0, CheckedI64::from(1));
    assert_eq!(1, z.support_size());
    assert_eq!("{0=>1}", z.to_string());
    assert_eq!(CheckedI64::from(1), z.lookup(&0));
    assert_eq!(CheckedI64::from(0), z.lookup(&1));
    assert_ne!(z, ZSetHashMap::<i64, CheckedI64>::zero());
    assert!(!z.is_zero());

    z.increment(&2, CheckedI64::from(0));
    assert_eq!(1, z.support_size());
    assert_eq!("{0=>1}", z.to_string());

    z.increment(&1, CheckedI64::from(-1));
    assert_eq!(2, z.support_size());
    assert_eq!("{0=>1,1=>-1}", z.to_string());

    z.increment(&-1, CheckedI64::from(1));
    assert_eq!(3, z.support_size());
    assert_eq!("{-1=>1,0=>1,1=>-1}", z.to_string());

    let d = z.distinct();
    assert_eq!(2, d.support_size());
    assert_eq!("{-1=>1,0=>1}", d.to_string());

    let d = z.neg_by_ref();
    assert_eq!(3, d.support_size());
    assert_eq!("{-1=>-1,0=>-1,1=>1}", d.to_string());
    assert_ne!(d, z);

    z.increment(&1, CheckedI64::from(1));
    assert_eq!(2, z.support_size());
    assert_eq!("{-1=>1,0=>1}", z.to_string());

    let mut z2 = z.clone().add_by_ref(&z);
    assert_eq!(2, z2.support_size());
    assert_eq!("{-1=>2,0=>2}", z2.to_string());

    z2.add_assign_by_ref(&z);
    assert_eq!(2, z2.support_size());
    assert_eq!("{-1=>3,0=>3}", z2.to_string());
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub(crate) struct TestTuple {
    pub(crate) left: i64,
    pub(crate) right: i64,
}

impl TestTuple {
    pub fn new(left: i64, right: i64) -> Self {
        Self { left, right }
    }
}

impl Display for TestTuple {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "({},{})", self.left, self.right)
    }
}

impl PartialOrd for TestTuple {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let lo = self.left.cmp(&other.left);
        if lo != Ordering::Equal {
            return Some(lo);
        }
        Some(self.right.cmp(&other.right))
    }
}

impl Ord for TestTuple {
    fn cmp(&self, other: &Self) -> Ordering {
        let lo = self.left.cmp(&other.left);
        if lo != Ordering::Equal {
            return lo;
        }
        self.right.cmp(&other.right)
    }
}

#[test]
fn zset_tuple_tests() {
    let mut z = ZSetHashMap::<TestTuple, CheckedI64>::with_capacity(5);
    assert_eq!(0, z.support_size());
    assert_eq!("{}", z.to_string());
    assert_eq!(CheckedI64::from(0), z.lookup(&TestTuple::new(0, 0))); // not present -> weight 0
    assert_eq!(z, ZSetHashMap::<TestTuple, CheckedI64>::zero());
    assert!(z.is_zero());
    let z2 = ZSetHashMap::<TestTuple, CheckedI64>::new();
    assert_eq!(z, z2);

    z.increment(&TestTuple::new(0, 0), CheckedI64::from(1));
    assert_eq!(1, z.support_size());
    assert_eq!("{(0,0)=>1}", z.to_string());
    assert_eq!(CheckedI64::from(1), z.lookup(&TestTuple::new(0, 0)));
    assert_eq!(CheckedI64::from(0), z.lookup(&TestTuple::new(0, 1)));
    assert_ne!(z, ZSetHashMap::<TestTuple, CheckedI64>::zero());
    assert!(!z.is_zero());

    z.increment(&TestTuple::new(2, 0), CheckedI64::from(0));
    assert_eq!(1, z.support_size());
    assert_eq!("{(0,0)=>1}", z.to_string());

    z.increment(&TestTuple::new(1, 0), CheckedI64::from(-1));
    assert_eq!(2, z.support_size());
    assert_eq!("{(0,0)=>1,(1,0)=>-1}", z.to_string());

    z.increment(&TestTuple::new(-1, 0), CheckedI64::from(1));
    assert_eq!(3, z.support_size());
    assert_eq!("{(-1,0)=>1,(0,0)=>1,(1,0)=>-1}", z.to_string());

    let d = z.distinct();
    assert_eq!(2, d.support_size());
    assert_eq!("{(-1,0)=>1,(0,0)=>1}", d.to_string());

    let d = z.neg_by_ref();
    assert_eq!(3, d.support_size());
    assert_eq!("{(-1,0)=>-1,(0,0)=>-1,(1,0)=>1}", d.to_string());
    assert_ne!(d, z);

    z.increment(&TestTuple::new(1, 0), CheckedI64::from(1));
    assert_eq!(2, z.support_size());
    assert_eq!("{(-1,0)=>1,(0,0)=>1}", z.to_string());

    let mut z2 = z.clone().add_by_ref(&z);
    assert_eq!(2, z2.support_size());
    let z2str = z2.to_string();
    assert_eq!("{(-1,0)=>2,(0,0)=>2}", z2str);

    z2.add_assign_by_ref(&z);
    assert_eq!(2, z2.support_size());
    assert_eq!("{(-1,0)=>3,(0,0)=>3}", z2.to_string());

    let prod = z2.cartesian(&z2, &|t, t2| {
        TestTuple::new(t.left + t2.left, t.right - t2.right)
    });
    assert_eq!("{(-2,0)=>9,(-1,0)=>18,(0,0)=>9}", prod.to_string());

    let j = z2.join(&z2, &|s| s.left, &|s| s.left, |a, _| a.clone());
    assert_eq!("{(-1,0)=>9,(0,0)=>9}", j.to_string());
}

#[test]
pub fn indexed_zset_tests() {
    let mut z = ZSetHashMap::<TestTuple, CheckedI64>::with_capacity(5);
    assert_eq!(0, z.support_size());
    z.increment(&TestTuple::new(0, 0), CheckedI64::from(1));
    z.increment(&TestTuple::new(2, 0), CheckedI64::from(2));
    z.increment(&TestTuple::new(1, 0), CheckedI64::from(-1));
    z.increment(&TestTuple::new(-1, 0), CheckedI64::from(1));
    let ps = z.clone().partition(&|t: &TestTuple| t.left.abs() % 2);
    let s = ps.to_string();
    assert_eq!("{0=>{(0,0)=>1,(2,0)=>2},1=>{(-1,0)=>1,(1,0)=>-1}}", s);
    let z2 = ps.sum();
    assert_eq!(z, z2);
}
