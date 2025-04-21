import re
import sys

# Format the SQL must adhere to
SQL_FORMAT_REGEX_PATTERN = re.compile(
    "^-- (.+) \\(([a-zA-Z0-9_-]+)\\)[ \t]*\r?\n--[ \t]*\r?\n((-- .+\r?\n)+)--[ \t]*\r?\n"
)

# Format of each description line
SQL_FORMAT_DESCRIPTION_LINE = re.compile("-- .+\r?\n")


def main():
    args = sys.argv[1:]
    for filename in args:
        print(f"Validating: {filename}")
        sql = open(filename, "rt").read()
        result = re.match(SQL_FORMAT_REGEX_PATTERN, sql)
        if result is None:
            print("FAIL: preamble does not match regex pattern")
            exit(1)

        # Extract from the SQL preamble the metadata
        title = result.group(1).strip()
        name = result.group(2)
        description_lines = result.group(3)

        # Post-process description lines
        description_match = re.findall(SQL_FORMAT_DESCRIPTION_LINE, description_lines)
        if description_match is None:
            print(
                "FAIL: description line did not match pattern which should not happen"
            )
            exit(1)
        description = " ".join(
            list(map(lambda l: l.removeprefix("-- ").strip(), description_match))
        ).strip()

        # Character limits
        if len(title) == 0:
            print("FAIL: title is empty")
            exit(1)
        if len(title) > 100:
            print(f"FAIL: title '{title}' exceeds 100 characters")
            exit(1)
        # Name is already checked to be non-empty due to regex
        if len(name) > 100:
            print(f"FAIL: name '{name}' exceeds 100 characters")
            exit(1)
        if len(description) == 0:
            print("FAIL: description is empty")
            exit(1)
        if len(description) > 1000:
            print(f"FAIL: description '{description}' exceeds 1000 characters")
            exit(1)

        # Print
        print("Read preamble:")
        print(f"  > Title......... {title}")
        print(f"  > Name.......... {name}")
        print(f"  > Description... {description}")

        # Finish
        print("PASS: preamble is valid")


if __name__ == "__main__":
    main()
