# Install nightly build of Roc language
# execute with `source cli-roc/install_nightly_roc.sh`
# https://github.com/roc-lang/roc/blob/main/getting_started/linux_x86_64.md
# Alternative one-liner:
export ROC_OS=linux_x86_64 ROC_OUTPUT_LOCATION="roc_nightly-$ROC_OS" && mkdir -p $ROC_OUTPUT_LOCATION && curl --fail --location "https://github.com/roc-lang/roc/releases/download/nightly/roc_nightly-$ROC_OS-latest.tar.gz" | TAR_OPTIONS=--wildcards tar --directory $ROC_OUTPUT_LOCATION --extract --gzip --strip-components 1 --file - 'roc_nightly-*/roc' 'roc_nightly-*/roc_language_server' && export PATH=$PATH:$(realpath ".")/roc_nightly-$ROC_OS

# # Download and export nightly build into a dir with a unique name
# platform="linux_x86_64"
#
# roc_dir_prefix="roc_nightly-$platform-"
#
# # Remove previous version
# version_dir=$(find . -type d -name "${roc_dir_prefix}*")
# bin_path_old=$(realpath "$version_dir")
# rm -r "$version_dir"
#
# # Download latest nightly
# archive_file="roc_nightly-$platform-latest.tar.gz"
# curl -OL https://github.com/roc-lang/roc/releases/download/nightly/$archive_file
# tar -xf $archive_file
# rm $archive_file
#
# # Get the version from the archive filename
# version_dir=$(find . -type d -name "${roc_dir_prefix}*")
# bin_path_new=$(realpath "$version_dir")
#
# # Add or update path to Roc binary to PATH
# if [[ ":$PATH:" == *":$bin_path_old:"* ]]; then
#     # Replace the old path with the new path
#     export PATH=${PATH//$bin_path_old/$bin_path_new}
#     echo "Successfully updated Roc in PATH."
# else
#     export PATH=$PATH:$bin_path_new
#     echo $bin_path_new
#     echo "Successfully appended Roc to PATH."
# fi
