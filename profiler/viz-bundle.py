#!/usr/bin/env python3

# This script takes a support bundle file as argument:
# - it unpacks the bundle
# - extracts sources and json profile data
# - saves the json profile data in the data/ directory
# - invokes the SQL compiler on the sources to generate the dataflow graph
# - saves the dataflow graph in the data/ directory
# - updates the typescript file which loads these files
# - compiles the typescript into javascript
# - starts an html page in a web browser with the profile visualization

import argparse
import zipfile
import tempfile
import sys
import gzip
import json
import re
import subprocess
from pathlib import Path

parser = argparse.ArgumentParser(
    description="Visualize profile data from a support bundle"
)
parser.add_argument("filename", help="Input file name")
parser.add_argument(
    "-v", "--verbose", action="store_true", help="Enable verbose output"
)

args = parser.parse_args()
verbose = args.verbose
file = Path(args.filename)
if verbose:
    print(f"Unpacking file {file}")

if not file.exists():
    print("File not found: ", file)
    sys.exit(1)

name = "temp"

# Create a temporary directory
index_file = None
with tempfile.TemporaryDirectory() as tmpdir:
    with zipfile.ZipFile(file, "r") as zip_ref:
        try:
            # List zipfile content
            files = zip_ref.infolist()
            # Find file matching pattern "*_circuit_profile.json"
            profile_files = [
                f for f in files if f.filename.endswith("_json_circuit_profile.zip")
            ]
            if not profile_files:
                print("No json profile file found in the bundle")
                sys.exit(1)
            # Choose the last one in sorted order
            profile_file = sorted(profile_files, key=lambda f: f.filename)[-1]
            if verbose:
                print(f"Found profile file: {profile_file.filename}")
            zip_ref.extract(profile_file, "data")
            if verbose:
                print(f"Extracted zipped json profile file: {profile_file.filename}")
            # This file is another zip file.
            with zipfile.ZipFile("data/" + profile_file.filename, "r") as zip_ref2:
                # Extract the inner json profile file
                inner_files = zip_ref2.infolist()
                if len(inner_files) != 1:
                    print(
                        "Expected a single file in the zip file ", profile_file.filename
                    )
                    sys.exit(1)
                json_profile_file = inner_files[0]
                with (
                    zip_ref2.open(json_profile_file) as source,
                    open("./data/" + name + ".json", "wb") as target,
                ):
                    target.write(source.read())
                if verbose:
                    print(
                        f"Extracted profile file: {profile_file.filename} into ./data/{name}.json"
                    )
            # Delete temporary zipped profile file
            Path("./data/" + profile_file.filename).unlink()
            # Find the files matching pipeline_config_json.gz
            config_files = [
                f for f in files if f.filename.endswith("pipeline_config.json.gz")
            ]
            # Choose the last one in sorted order
            if config_files:
                config_file = sorted(config_files, key=lambda f: f.filename)[-1]
                if verbose:
                    print(f"Found config file: {config_file}")
                # Extract the config file
                with (
                    zip_ref.open(config_file) as source,
                    open("./data/temp-config.json.gz", "wb") as target,
                ):
                    target.write(source.read())
                # Gunzip the extracted file
                with gzip.open("./data/temp-config.json.gz", "rb") as f_in:
                    with open("./data/temp-config.json", "wb") as f_out:
                        f_out.writelines(f_in)
                if verbose:
                    print(f"Extracted JSON into ./data/temp-config.json")
                # Delete the gz file
                Path("./data/temp-config.json.gz").unlink()
                # Extract the "program_code" field from the config json
                with open("./data/temp-config.json", "r") as f:
                    config_data = json.load(f)
                    program_code = config_data.get("program_code", "")
                    # Save the program code to a separate file
                    with open("./data/temp-program.sql", "w") as f_out:
                        f_out.write(program_code)
                if verbose:
                    print(f"Extracted program code into ./data/temp-program.sql")
                # Delete the original config json file
                Path("./data/temp-config.json").unlink()
                # Invoke the SQL compiler on the program code to generate the dataflow graph
                if verbose:
                    print("Invoking SQL to DBSP compiler to generate dataflow graph")
                subprocess.run(
                    [
                        "../sql-to-dbsp-compiler/SQL-compiler/sql-to-dbsp",
                        "./data/temp-program.sql",
                        "-i",
                        "--dataflow",
                        f"./data/dataflow-{name}.json",
                        "-q",
                        "--alltables",
                        "--noRust",
                        "--ignoreOrder",
                    ],
                    check=True,
                )
                if verbose:
                    print(f"Generated dataflow graph into ./data/dataflow-{name}.json")
                # Delete the temporary program file
                Path("./data/temp-program.sql").unlink()
                if verbose:
                    print("Deleted program file")
                # Modify the src/index.ts file to load the new files
                original_index_file = Path("src/index.ts")
                # Save the old copy of the index file
                index_file = original_index_file.rename(
                    original_index_file.with_suffix(".bak")
                )
                if verbose:
                    print(f"Backed up original index.ts file to: {index_file}")
                index_content = index_file.read_text()
                pattern = r'(.*loadFiles\("data", )"(.*)"\);'
                #           ^^^^^^^^^^^^^^^^^^^^^^  ^^^^
                #           Group 1                 Group 2
                # Replace the second group with {name}
                index_content = re.sub(
                    pattern, lambda m: f'{m.group(1)}"{name}");', index_content
                )
                original_index_file.write_text(index_content)
                if verbose:
                    print(f"Updated index file: {index_file}")
                    print("Installing npm packages")
                # Change directory to src and run npm install and tsc
                subprocess.run(["npm", "install"], cwd="src", check=True)
                if verbose:
                    print("Compiling typescript into javascript")
                subprocess.run(["npx", "tsc", "--build"], check=True)
                if verbose:
                    print("Starting web server")
                print("Point a web browser to http://localhost:5173")
                # Start a dev web server
                try:
                    subprocess.run(["npm", "run", "dev"])
                except KeyboardInterrupt as e:
                    # This is normal: the user stops it with Ctrl-C
                    pass
                # Restore the original index file when the user stops the session
                index_file.rename(original_index_file)
                if verbose:
                    print("Restored original index.ts file")
        except Exception as e:
            # Restore the original index file in case of error
            if index_file is not None:
                index_file.rename(original_index_file)
            print("Error extracting profile file: ", e)
            sys.exit(1)

print("Done")
