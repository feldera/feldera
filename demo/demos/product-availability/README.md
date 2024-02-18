# Demo: Product Availability

The use case is of a data model that captures product
availability changing across warehouses as they are bought and sold.

## Data model

There are three tables: `warehouse` and `product` have a many-to-many
relationship through `storage`, which stores the availability of a
product in a warehouse.

## Data generation

Data generation is as follows:

- A fixed number of warehouses is generated
- A fixed number of products is generated
- A target number of storage entries (upserts) is set
- The storage availability is randomly initialized in [0, 200]
- Each round all storage entries (warehouse-product combinations)
  are updated in random order
- Each storage availability is updated:
  - Minimum of 0
  - If less than 20, 75% probability to increase
  - If between 20 and 100, 50% probability to increase
  - If more than 100, 25% probability to increase
  - Maximum of 200
- The amount to increase or decrease is randomly chosen from [0, 5]
  (minimum of 0 and maximum of 200 are maintained)

## Usage

1. **Kafka:** setup of a Kafka server with the following setup:
   - Three topics:
     - `demo-product-availability-product` with unlimited retention
     - `demo-product-availability-warehouse` with unlimited retention
     - `demo-product-availability-storage` with time- or space-limited retention
   - Authorization setup with authentication:
     - A consumer user with read permission on the topics
       (`<consume-username>`, `<consume-password>`)
     - A producer user with write permission on the topics
       (`<produce-username>`, `<produce-password>`)

2. **Generate demo.json:** using its template, generate the final demo.json:
   ```
   python3 generate_demo_json.py
   ```
   ... which will generate `demo.json`. This will need to be hosted somewhere
   accessible over the network (see step 4).

3. **Setup secrets:** set up several Feldera connector secrets
   by following the [instructions](https://www.feldera.com/docs/cloud/secret-management):
   - product-availability-bootstrap-servers
   - product-availability-consume-sasl-username
   - product-availability-consume-sasl-password

4. **Start Feldera:** start the Feldera instance providing as argument
   how to reach the generated *demo.json*:
   ```
   ... --demos https://raw.githubusercontent.com/feldera/feldera/main/demo/demos/product-availability/demo.json
   ```

5. **Data generation**
   1. Start a container and enter its shell
   2. Install `python3` and the required modules using `pip3`
   3. Copy over `generate_continuous_data.py`
   4. Start the data generation script, for example:
      ```
      python3 generate_continuous_data.py \
          --num-warehouse=10 \
          --num-product=20 \
          --num-storage-entries=1000000000000000000 \
          --target-rate-per-second=200 \
          --bootstrap-servers [bootstrap-servers] \
          --security-protocol [security-protocol] \
          --sasl-mechanism [sasl-mechanism] \
          --sasl-username [produce-sasl-username] \
          --sasl-password [produce-sasl-password]
      ```
