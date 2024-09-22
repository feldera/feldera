# Feldera <-> Hopsworks Integration

## Instructions

- Run Feldera locally or in sandbox at: <https://try.feldera.com/>
- If you are running Feldera in sandbox mode, in notebook 1 and 2, update the
  client initialization to:
  `FelderaClient("https://try.feldera.com", FELDERA_API_KEY)`
- Create a Hopsworks account by signing up at: <https://c.app.hopsworks.ai/>
- Generate a Hopsworks API key at: <https://c.app.hopsworks.ai/account/api>
- Copy and paste this API key into `KEY` variables in all the notebooks.
- Run the notebooks.
  - Notebook 0 generates simulated data in Hopsworks Kafka and Feature Groups.
  - Notebook 1 creates a streaming feature pipeline from
    Hopsworks Kafka -> Feldera -> Hopsworks Kafka
  - Notebook 2 trains and tests a model
