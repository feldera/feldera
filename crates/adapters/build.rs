use change_detection::ChangeDetection;
use static_files::resource_dir;

fn main() -> std::io::Result<()> {
    ChangeDetection::path("static").path("build.rs").generate();

    resource_dir("./static").build()
}
