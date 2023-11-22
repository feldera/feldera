# Build dbsp python library
cd ./crates/pipeline_manager && cargo make openapi_python
# Install Playwright system dependencies
cd ../../web-console && yarn playwright install-deps