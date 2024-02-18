import json
import time
import random
import argparse
import datetime
from kafka import KafkaProducer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--num-warehouse', required=False, default=10)
    parser.add_argument('--num-product', required=False, default=20)
    parser.add_argument('--num-storage-entries', required=False, default=1000000000000000000)
    parser.add_argument('--target-rate-per-second', required=False, default=200)
    parser.add_argument('--bootstrap-servers', required=True)
    parser.add_argument('--security-protocol', required=True)
    parser.add_argument('--sasl-mechanism', required=True)
    parser.add_argument('--sasl-username', required=True)
    parser.add_argument('--sasl-password', required=True)
    args = parser.parse_args()

    # Seed the fake data generation
    random.seed(123456789)

    # Sizes to generate
    num_warehouse = int(args.num_warehouse)
    num_product = int(args.num_product)
    num_storage_entries = int(args.num_storage_entries)
    target_rate_per_second = int(args.target_rate_per_second)
    print("Data generation arguments:")
    print(f"  > Number of warehouses: {num_warehouse}")
    print(f"  > Number of products: {num_product}")
    print(f"  > Number of storage entries: {num_storage_entries}")
    print(f"  > Target rate: {target_rate_per_second} per second")
    print("")

    # Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        security_protocol=args.security_protocol,
        sasl_mechanism=args.sasl_mechanism,
        sasl_plain_username=args.sasl_username,
        sasl_plain_password=args.sasl_password,
    )

    # Warehouse
    print("Producing warehouse rows into topic demo-product-availability-warehouse...")
    for i in range(1, num_warehouse + 1):
        producer.send("demo-product-availability-warehouse", value={
            "insert": {
                "id": i,
                "name": f"Warehouse no. {i}",
                "address": f"Address of warehouse no. {i}"
            }
        })
    print("Finished warehouse generation")

    # Product
    print("Producing product rows into topic demo-product-availability-product...")
    for i in range(1, num_product + 1):
        mass = random.uniform(0.001, 1000.0)
        volume = random.uniform(0.001, 1000.0)
        producer.send("demo-product-availability-product", value={
            "insert": {
                "id": i,
                "name": f"Product no. {i}",
                "mass": mass,
                "volume": volume
            }
        })
    print("Finished product generation")

    # Initial storage
    storage = {}
    id_pairs = []
    for warehouse_id in range(1, num_warehouse + 1):
        for product_id in range(1, num_product + 1):
            storage[(warehouse_id, product_id)] = random.randint(0, 200)
            id_pairs.append((warehouse_id, product_id))

    # Simulation time
    current_time = datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)

    # Generate the storage entries
    print("Producing storage rows into topic demo-product-availability-storage...")
    start_time_entry_generation_s = datetime.datetime.now().timestamp()
    last_ongoing_rate_update_s = start_time_entry_generation_s
    rate_update_interval_s = 1.0
    for entry_no in range(0, num_storage_entries):
        # Rate limiting
        now_s = datetime.datetime.now().timestamp()
        elapsed_s = now_s - start_time_entry_generation_s
        target_generated = target_rate_per_second * elapsed_s
        if float(entry_no) > target_generated:
            delta_too_much = float(entry_no - target_generated)
            sleep_time_s = delta_too_much / target_rate_per_second
            time.sleep(sleep_time_s)

        # Print regularly the data generation status
        if now_s - last_ongoing_rate_update_s >= rate_update_interval_s:
            rate_update_interval_s = 5.0
            print(f"[T={elapsed_s:.1f}] Overall rate achieved: {entry_no / elapsed_s:.0f} per second"
                  f" (generated in total so far: {entry_no})")
            last_ongoing_rate_update_s = now_s

        # Round is complete
        if entry_no % (num_warehouse * num_product) == 0:
            current_time += datetime.timedelta(seconds=1)  # Progress time
            random.shuffle(id_pairs)  # Shuffle update order

        # Warehouse and product number being updated
        id_pair = id_pairs[(entry_no % (num_warehouse * num_product))]
        (warehouse_id, product_id) = id_pair

        # Update availability of product at warehouse
        if storage[id_pair] < 20:
            probability_increase = 0.75
        elif 20 <= storage[id_pair] <= 100:
            probability_increase = 0.5
        else:
            probability_increase = 0.25
        if random.random() <= probability_increase:
            storage[id_pair] += random.randint(0, 5)
        else:
            storage[id_pair] -= random.randint(0, 5)

        # Must be in [0, 200]
        if storage[id_pair] < 0:
            storage[id_pair] = 0
        elif storage[id_pair] > 200:
            storage[id_pair] = 200

        # Upsert the availability in storage
        row = {
            "insert": {
                "warehouse_id": warehouse_id,
                "product_id": product_id,
                "updated_at": current_time.strftime("%Y-%m-%d %H:%M:%S"),
                "num_available": storage[id_pair]
            }
        }
        producer.send("demo-product-availability-storage", value=row)
    print("Finished storage entry generation")

    print("Data generation finished")


# Main entry point
if __name__ == "__main__":
    main()
