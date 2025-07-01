use rkyv::Fallible;

use crate::Fixed;

impl<const P: usize, const S: usize> rkyv::Archive for Fixed<P, S> {
    type Archived = Self;
    type Resolver = ();

    #[inline]
    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, out: *mut Self::Archived) {
        out.write(*self);
    }
}

impl<Ser: Fallible + ?Sized, const P: usize, const S: usize> rkyv::Serialize<Ser> for Fixed<P, S> {
    #[inline]
    fn serialize(&self, _serializer: &mut Ser) -> Result<Self::Resolver, Ser::Error> {
        Ok(())
    }
}

impl<D: Fallible + ?Sized, const P: usize, const S: usize> rkyv::Deserialize<Fixed<P, S>, D>
    for Fixed<P, S>
{
    #[inline]
    fn deserialize(&self, _: &mut D) -> Result<Self, D::Error> {
        Ok(*self)
    }
}

impl<C: Fallible, const P: usize, const S: usize> rkyv::CheckBytes<C> for Fixed<P, S> {
    type Error = core::convert::Infallible;
    unsafe fn check_bytes<'a>(value: *const Self, _ctx: &mut C) -> Result<&'a Self, Self::Error> {
        Ok(&*value)
    }
}
