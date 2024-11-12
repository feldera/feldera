use crate::{
    dynamic::{erase::Erase, ArchiveTrait},
    DBData,
};
use hashbrown::HashSet;
use once_cell::sync::Lazy;
use rkyv::archived_value;
use std::{
    fs::{self, File},
    hash::{DefaultHasher, Hash, Hasher},
    io::{self, Read, Write},
    marker::PhantomData,
    mem,
    path::Path,
    sync::Mutex,
};
use typemap::ShareMap;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct RequiredFactory {
    val_trait: String,
    val_type: String,
}

// TODO: move to utils
fn write_file_if_changed(path: &Path, content: &str) -> io::Result<()> {
    println!("writing {}", path.display());
    // Create all parent directories if they don't exist
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    // Check if the file exists and has the same content
    if path.exists() {
        let mut existing_content = String::new();
        fs::File::open(path)?.read_to_string(&mut existing_content)?;

        // If the content is the same, do nothing
        if existing_content == content {
            return Ok(());
        }
    }

    // Write the new content to the file
    let mut file = fs::File::create(path)?;
    file.write_all(content.as_bytes())?;

    Ok(())
}

impl RequiredFactory {
    pub fn new(val_trait: &str, val_type: &str) -> Self {
        Self {
            val_trait: val_trait.to_string(),
            val_type: val_type.to_string(),
        }
    }

    pub fn crate_name(&self) -> String {
        // TODO: use crypto hash to avoid conflicts.
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        format!("factory_{}", hasher.finish())
    }

    pub fn generate_factory_crate(&self, dbsp_path: &Path, project_path: &Path) -> io::Result<()> {
        let crate_name = self.crate_name();
        println!("generating {crate_name}");

        let toml_path = project_path.join("..").join(&crate_name).join("Cargo.toml");
        let lib_path = project_path
            .join("..")
            .join(&crate_name)
            .join("src")
            .join("lib.rs");

        let toml_code = format!(
            r#"[package]
name = "{crate_name}"
edition = "2021"

[dependencies]
dbsp = {{ path = "{}" }}
feldera-sqllib = {{ path = "{}" }}
"#,
            dbsp_path.display(),
            dbsp_path.parent().unwrap().join("sqllib").display()
        );

        let rust_code = format!(
            r#"pub static factory: &'static dyn dbsp::dynamic::Factory<{}> = dbsp::dynamic::factory::FactoryImpl::<{}, {}>::new();"#,
            self.val_trait, self.val_type, self.val_trait
        ).replace("alloc::string::String", "String");

        let dependency = format!(
            r#"[dependencies.{crate_name}]
path = "../{crate_name}"
"#
        );

        write_file_if_changed(&toml_path, &toml_code)?;
        write_file_if_changed(&lib_path, &rust_code)?;

        let mut project_cargo_file = File::options()
            .append(true)
            .create(false)
            .open(project_path.join("Cargo.toml"))?;
        writeln!(project_cargo_file, "\n{}", &dependency)?;

        Ok(())
    }
}

static REQUIRED_FACTORIES: Lazy<Mutex<HashSet<RequiredFactory>>> =
    Lazy::new(|| Mutex::new(HashSet::new()));

pub fn register_required_factory(val_trait: &str, val_type: &str) {
    REQUIRED_FACTORIES
        .lock()
        .unwrap()
        .insert(RequiredFactory::new(val_trait, val_type));
}

pub fn generate_factory_crates(dbsp_path: &Path, project_path: &Path) -> io::Result<()> {
    let required_factories = REQUIRED_FACTORIES.lock().unwrap().clone();
    let mut register_factories = Vec::with_capacity(required_factories.len());

    for required_factory in required_factories.into_iter() {
        required_factory.generate_factory_crate(dbsp_path, project_path)?;

        let crate_name = required_factory.crate_name();
        register_factories.push(format!(
            "dbsp::dynamic::factory::register_factory::<{}, {}>({crate_name}::factory);",
            &required_factory.val_type, &required_factory.val_trait
        ));
    }

    let factories_path = project_path.join("src").join("factories.rs");
    let factories_code = format!(
        r#"pub fn register_factories() {{
    {}
}}"#,
        register_factories.join("\n    ")
    );

    let factories_code = factories_code.replace("alloc::string::String", "String");

    write_file_if_changed(&factories_path, &factories_code)?;

    Ok(())
}

#[derive(PartialEq, Eq)]
pub struct FactoryMapKey<VType, VTrait: ?Sized> {
    phantom: PhantomData<dyn Fn(&VType, &VTrait)>,
}

impl<VType, VTrait> typemap::Key for FactoryMapKey<VType, VTrait>
where
    VType: 'static,
    VTrait: ?Sized + 'static,
{
    type Value = &'static dyn Factory<VTrait>;
}

pub static FACTORIES: Lazy<Mutex<ShareMap>> = Lazy::new(|| Mutex::new(ShareMap::custom()));

pub fn register_factory<VType, VTrait>(factory: &'static dyn Factory<VTrait>)
where
    VType: 'static,
    VTrait: ?Sized + 'static,
{
    FACTORIES
        .lock()
        .unwrap()
        .insert::<FactoryMapKey<VType, VTrait>>(factory);
}

pub fn find_factory<VType, VTrait>() -> Option<&'static dyn Factory<VTrait>>
where
    VType: 'static,
    VTrait: ?Sized + 'static,
{
    let factory = FACTORIES
        .lock()
        .unwrap()
        .get::<FactoryMapKey<VType, VTrait>>()
        .map(|factory| *factory);

    if factory.is_none() {
        println!(
            "Factory not found. Known factories: {}",
            FACTORIES.lock().unwrap().len()
        );
    } else {
        println!("Factory found");
    }

    factory
}

#[macro_export]
macro_rules! factory {
    ($vtype:ty, $vtrait:ty) => {{
        use dbsp::{dynamic::*, trace::WeightedItem};
        //println!("factory({}, {})", stringify!($vtype), stringify!($vtrait));
        // println!(
        //     "factory({}, {})",
        //     std::any::type_name::<$vtype>(),
        //     std::any::type_name::<$vtrait>(),
        // );
        $crate::dynamic::WithFactory::<$vtype>::FACTORY
    }};
}

/// Create instances of a concrete type wrapped in a trait object.
pub trait Factory<Trait: ArchiveTrait + ?Sized>: Send + Sync {
    /// Size of the underlying concrete type.
    fn size_of(&self) -> usize;

    /// True iff the underlying concrete type is a zero-sized type.
    fn is_zst(&self) -> bool {
        self.size_of() == 0
    }

    /// Create an instance of the underlying concrete type with the default value on the heap.
    fn default_box(&self) -> Box<Trait>;

    /// Creates an instance of the underlying concrete type with the default value on the stack
    /// and passes it as an argument to the provided closure.
    fn with(&self, f: &mut dyn FnMut(&mut Trait));

    /// Casts an archived value from the given byte slice at the given position.
    ///
    /// # Safety
    ///
    /// The specified offset must contain an archived instance of the concrete
    /// type that this factory manages.
    unsafe fn archived_value<'a>(&self, bytes: &'a [u8], pos: usize) -> &'a Trait::Archived;
}

pub struct FactoryImpl<T, Trait: ?Sized> {
    phantom: PhantomData<fn(&T, &Trait)>,
}

impl<T, Trait: ?Sized> FactoryImpl<T, Trait> {
    pub const fn new() -> &'static Self {
        &Self {
            phantom: PhantomData,
        }
    }
}

/// Trait for trait objects that can be created from instances of a concrete type `T`.
pub trait WithFactory<T>: 'static {
    /// A factory that creates trait objects of type `Self`, backed by concrete values
    /// of type `T`.
    fn factory() -> &'static dyn Factory<Self>;

    // /// A factory that creates trait objects of type `Self`, backed by concrete values
    // /// of type `T`.
    // const FACTORY: &'static dyn Factory<Self>;
}

impl<T, Trait> Factory<Trait> for FactoryImpl<T, Trait>
where
    T: DBData + Erase<Trait> + 'static,
    Trait: ArchiveTrait + ?Sized,
{
    fn default_box(&self) -> Box<Trait> {
        Box::<T>::default().erase_box()
    }

    fn size_of(&self) -> usize {
        mem::size_of::<T>()
    }

    fn with(&self, f: &mut dyn FnMut(&mut Trait)) {
        f(T::default().erase_mut())
    }

    unsafe fn archived_value<'a>(&self, bytes: &'a [u8], pos: usize) -> &'a Trait::Archived {
        let archived: &T::Archived = archived_value::<T>(bytes, pos);
        <T as Erase<Trait>>::erase_archived(archived)
    }
}

impl<T, Trait> WithFactory<T> for Trait
where
    Trait: ArchiveTrait + ?Sized + 'static,
    T: DBData + Erase<Trait>,
{
    // const FACTORY: &'static dyn Factory<Self> = &FactoryImpl::<T, Self> {
    //     phantom: PhantomData,
    // };

    fn factory() -> &'static dyn Factory<Self> {
        // println!(
        //     "factory({}, {})",
        //     std::any::type_name::<T>(),
        //     std::any::type_name::<Self>(),
        // );
        register_required_factory(std::any::type_name::<Self>(), std::any::type_name::<T>());
        find_factory::<T, Self>().unwrap()

        //FactoryImpl::<T, Self>::new()
    }
}
