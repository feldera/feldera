import os
import json
import argparse

# File locations
SCRIPT_DIR = os.path.join(os.path.dirname(__file__))
PROGRAM_SQL = os.path.join(SCRIPT_DIR, "program.sql")
DEMO_TEMPLATE_JSON = os.path.join(SCRIPT_DIR, "demo.template.json")
DEMO_JSON = os.path.join(SCRIPT_DIR, "demo.json")


def main():
    # SQL
    print("Reading program SQL into JSON-serialized string...")
    with open(PROGRAM_SQL, "r") as f_in:
        program_sql_content = json.dumps(f_in.read())
    print("Read")

    # Replace within the template all placeholders with the actual values
    print("Generating demo.json using template...")
    with open(DEMO_TEMPLATE_JSON, "r") as f_in:
        with open(DEMO_JSON, "w+") as f_out:
            f_out.write(
                f_in.read()
                .replace("[REPLACE:PROGRAM-CODE]", program_sql_content[1:-1])
            )
    print("Finished")


# Main entry point
if __name__ == "__main__":
    main()
