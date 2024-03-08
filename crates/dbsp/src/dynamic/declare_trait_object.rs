#[macro_export]
macro_rules! declare_trait_object {
    ($type_alias:ident <$($generic:ident),* > = dyn $typedef:path
    where
        $($bound:tt)*
    ) =>
    {
        $crate::declare_trait_object_with_archived!($type_alias <$($generic),*> = dyn $typedef
        { type Archived = dyn $crate::dynamic::DeserializeDyn<$type_alias<$($generic),*>> }
        where
            $($bound)*
        );
    }
}

#[macro_export]
macro_rules! declare_trait_object_with_archived {
    ($type_alias:ident <$($generic:ident),* > = dyn $typedef:path
    { type Archived = dyn $archived_type:path }
    where
        $($bound:tt)*
    ) =>
    {
        pub type $type_alias<$($generic),*> = dyn $typedef;

        $crate::derive_comparison_traits!($type_alias<$($generic),*> where $($bound)*);
        $crate::derive_erase!(<$($generic),*> $typedef
        { type Archived = dyn $archived_type }
        where $($bound)*);

        impl<$($generic),*> $crate::dynamic::DowncastTrait for $type_alias<$($generic),*>
        where
            $($bound)*
        {
        }

        impl<$($generic),*> Clone for Box<$type_alias<$($generic),*>>
        where
            $($bound)*
        {
            fn clone(&self) -> Self {
                dyn_clone::clone_box(self.as_ref())
            }
        }

        impl<$($generic),*> rkyv::Deserialize<Box<$type_alias<$($generic),*>>, $crate::trace::Deserializer> for () {
            fn deserialize(
                &self,
                _deserializer: &mut $crate::trace::Deserializer,
            ) -> Result<Box<$type_alias<$($generic),*>>, <$crate::trace::Deserializer as rkyv::Fallible>::Error> {
                todo!()
            }
        }

        impl<$($generic),*> rkyv::Archive for Box<$type_alias<$($generic),*>> {
            type Archived = ();
            type Resolver = ();

            unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
                todo!()
            }
        }

        impl<$($generic),*> rkyv::Serialize<$crate::trace::Serializer> for Box<$type_alias<$($generic),*>> {
            fn serialize(
                &self,
                _serializer: &mut $crate::trace::Serializer,
            ) -> Result<Self::Resolver, <$crate::trace::Serializer as rkyv::Fallible>::Error> {
                todo!()
            }
        }

        impl<$($generic),*> $crate::dynamic::ArchiveTrait for $type_alias<$($generic),*>
        where
            $($bound)*
        {
            type Archived = dyn $archived_type;
        }
    };
}

#[macro_export]
macro_rules! declare_typed_trait_object {
    ($type_alias:ident <$($generic:ident),* > = dyn $typedef:ty
    [$inner_type:ty]
    where
        $($bound:tt)*
    ) =>
    {
        $crate::declare_trait_object!($type_alias<$($generic),*> = dyn $typedef where $($bound)*);

        impl<$($generic),*> std::ops::Deref for $type_alias<$($generic),*>
        where
            $($bound)*
        {
            type Target = $inner_type;

            fn deref(&self) -> &Self::Target {
                unsafe { &*(self as *const _ as *const Self::Target) }
            }
        }

        impl<$($generic),*> std::ops::DerefMut for $type_alias<$($generic),*>
        where
            $($bound)*
        {
            fn deref_mut(&mut self) -> &mut Self::Target {
                unsafe { &mut *(self as *mut _ as *mut $inner_type) }
            }
        }
    };
}

#[cfg(test)]
mod test {
    use rkyv::{Archive, Deserialize, Serialize};
    use size_of::SizeOf;

    use crate::{
        declare_trait_object,
        dynamic::{Data, DataTrait, DataTraitTyped, Erase, WeightTrait, WeightTraitTyped},
        DBData, DBWeight,
    };

    #[derive(
        Clone,
        Default,
        Debug,
        PartialOrd,
        Ord,
        PartialEq,
        Eq,
        Hash,
        SizeOf,
        Archive,
        Serialize,
        Deserialize,
    )]
    #[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
    #[archive(compare(PartialEq, PartialOrd))]
    struct Foo<T1: DBData, T2: DBData> {
        x: T1,
        y: T2,
    }

    pub trait Bar<T1: DataTrait + ?Sized, T2: WeightTrait + ?Sized>: Data {
        fn f1(&self) -> &T1;
        fn f2(&self) -> &T2;
    }

    impl<T1: DataTraitTyped + ?Sized, T2: WeightTraitTyped + ?Sized> Bar<T1, T2>
        for Foo<T1::Type, T2::Type>
    where
        T1::Type: DBData + Erase<T1>,
        T2::Type: DBWeight + Erase<T2>,
    {
        fn f1(&self) -> &T1 {
            self.x.erase()
        }

        fn f2(&self) -> &T2 {
            self.y.erase()
        }
    }

    declare_trait_object!(DynBar<T1, T2> = dyn Bar<T1, T2>
    where
        T1: DataTrait + ?Sized,
        T2: WeightTrait + ?Sized
    );

    #[test]
    fn declare_trait_object_test() {}
}
