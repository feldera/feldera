use crate::{Tup1, Tup2};
use feldera_sqllib::*;
pub fn f(x: Option<Tup1<Option<i32>>>) -> Result<Option<Tup1<Option<i32>>>, Box<dyn std::error::Error>> {
   match x {
      None => Ok(None),
      Some(x) => match x.0 {
         None => Ok(Some(Tup1::new(None))),
         Some(x) => Ok(Some(Tup1::new(Some(x + 1)))),
      }
   }
}

pub fn g(x: i32) -> Result<Tup2<i32, i32>, Box<dyn std::error::Error>> {
   Ok(Tup2::new(x-1, x+1))
}
