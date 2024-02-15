import json
import time
import random
import argparse
import datetime
from faker import Faker
from kafka import KafkaProducer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--num-product', required=False, default=100)
    parser.add_argument('--min-items-available', required=False, default=200)
    parser.add_argument('--max-items-available', required=False, default=500)
    parser.add_argument('--num-item-entries', required=False, default=1000000000000000000)
    parser.add_argument('--target-item-entry-rate-per-second', required=False, default=10)
    parser.add_argument('--bootstrap-servers', required=True)
    parser.add_argument('--security-protocol', required=True)
    parser.add_argument('--sasl-mechanism', required=True)
    parser.add_argument('--sasl-username', required=True)
    parser.add_argument('--sasl-password', required=True)
    args = parser.parse_args()

    # Seed the fake data generation
    fake = Faker(locale='la')
    Faker.seed(1122334455)
    random.seed(123456789)

    # Sizes to generate
    num_product = int(args.num_product)
    minimum_items_available = int(args.min_items_available)
    maximum_items_available = int(args.max_items_available)
    num_item_entries = int(args.num_item_entries)
    target_item_entry_rate_per_second = int(args.target_item_entry_rate_per_second)
    print("Data generation arguments:")
    print(f"  > Number of products: {num_product}")
    print(f"  > Minimum number of items available: {minimum_items_available}")
    print(f"  > Maximum number of items available: {maximum_items_available}")
    print(f"  > Number of item entries: {num_item_entries}")
    print(f"  > Target item entry rate: {target_item_entry_rate_per_second} per second")

    # Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        security_protocol=args.security_protocol,
        sasl_mechanism=args.sasl_mechanism,
        sasl_plain_username=args.sasl_username,
        sasl_plain_password=args.sasl_password,
    )

    # Product
    print("Producing product rows into topic demo-productitems-product...")
    for i in range(1, num_product + 1):
        producer.send("demo-productitems-product", value={
            "insert": {
                "id": i,
                "name": f"Product no. {i}",
                "description": f"Description of product no. {i}.",
            }
        })
    print("Finished product generation")

    # Item
    start_time_item_entry_generation = datetime.datetime.now().timestamp()
    if num_item_entries == -1:
        num_item_entries = 1000000000000000000
    current_time = datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    available_items = []
    next_item_id = 1
    for entry_no in range(1, num_item_entries + 1):
        # Rate limiting
        elapsed_s = datetime.datetime.now().timestamp() - start_time_item_entry_generation
        target_generated = target_item_entry_rate_per_second * elapsed_s
        if float(entry_no - 1) > target_generated:
            delta_too_much = float(entry_no - 1) - target_generated
            sleep_time_s = delta_too_much / target_item_entry_rate_per_second
            print(f"Total elapsed time: {elapsed_s}")
            print(f"Generated so far: {entry_no - 1}")
            time.sleep(sleep_time_s)

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
            product_id = random.randint(1, num_product)
            row = {
                "insert": {
                    "id": next_item_id,
                    "product_id": product_id,
                    "manufactured_at": manufactured_at,
                    "bought_at": current_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "sold_at": None
                }
            }
            available_items.append({
                "id": next_item_id,
                "product_id": product_id,
                "manufactured_at": manufactured_at,
                "bought_at": current_time.strftime("%Y-%m-%d %H:%M:%S")
            })
            next_item_id += 1
        else:
            chosen_sold_item = available_items[random.randint(0, len(available_items) - 1)]
            available_items.remove(chosen_sold_item)
            row = {
                "insert": {
                    "id": chosen_sold_item["id"],
                    "product_id": chosen_sold_item["product_id"],
                    "manufactured_at": chosen_sold_item["manufactured_at"],
                    "bought_at": chosen_sold_item["bought_at"],
                    "sold_at": current_time.strftime("%Y-%m-%d %H:%M:%S")
                }
            }
        producer.send("demo-productitems-item", value=row)
    print("Finished item generation")


# Main entry point
if __name__ == "__main__":
    main()
