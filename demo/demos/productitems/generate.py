import os
import json
import random
import datetime
from faker import Faker

# File locations
SCRIPT_DIR = os.path.join(os.path.dirname(__file__))
PROGRAM_SQL = os.path.join(SCRIPT_DIR, "program.sql")
DATA_PRODUCT_JSON = os.path.join(SCRIPT_DIR, "data-product.json")
DATA_ITEM_JSON = os.path.join(SCRIPT_DIR, "data-item.json")


def main():

    # Seed the fake data generation
    fake = Faker(locale='la')
    Faker.seed(1122334455)
    random.seed(123456789)

    # Sizes to generate
    num_product = 20
    minimum_items_available = 20
    maximum_items_available = 50
    num_item_entries = 200

    print("Generating data...")

    # Product
    with open(DATA_PRODUCT_JSON, "w+") as f_out:
        for i in range(1, num_product + 1):
            f_out.write(json.dumps({
                "insert": {
                    "id": i,
                    "name": f"Product no. {i}",
                    "description": f"Description of product no. {i}.",
                }
            }) + "\n")

    # Item
    with open(DATA_ITEM_JSON, "w+") as f_out:
        current_time = datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        available_items = []
        next_item_id = 1
        for _ in range(1, num_item_entries + 1):
            # Progress current time
            current_time += datetime.timedelta(seconds=random.randint(0, 180))

            # A certain minimum and maximum number of items must be available (= not sold)
            if len(available_items) <= minimum_items_available:
                insert = True
            elif len(available_items) >= maximum_items_available:
                insert = False
            else:
                # There is a 50% chance of it being either a new
                # item becoming available or an existing item
                # being sold (and thus becoming unavailable)
                insert = random.randint(0, 1) == 0

            if insert:
                # Manufacture date is chosen from a time span before start
                manufactured_at = fake.date_time_between(
                    datetime.datetime(2023, 1, 1, 0, 0, 0),
                    datetime.datetime(2023, 12, 31, 23, 59, 59),
                    tzinfo=datetime.timezone.utc
                ).strftime("%Y-%m-%d %H:%M:%S")
                row = {
                    "insert": {
                        "id": next_item_id,
                        "product_id": random.randint(1, num_product),
                        "manufactured_at": manufactured_at,
                        "bought_at": current_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "sold_at": None
                    }
                }
                available_items.append(next_item_id)
                next_item_id += 1
            else:
                chosen_sold_item_id = available_items[random.randint(0, len(available_items) - 1)]
                available_items.remove(chosen_sold_item_id)
                row = {
                    "update": {
                        "id": chosen_sold_item_id,
                        "sold_at": current_time.strftime("%Y-%m-%d %H:%M:%S")
                    }
                }
            f_out.write(json.dumps(row) + "\n")

    print("Finished generating data")

    # SQL
    print("Reading program SQL and outputting JSON-serialized string...")
    with open(PROGRAM_SQL, "r") as f_in:
        print(json.dumps(f_in.read()))

    print("Finished")


# Main entry point
if __name__ == "__main__":
    main()
