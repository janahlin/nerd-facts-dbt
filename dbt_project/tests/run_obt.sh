#!/bin/bash

# Script to run Operational Batch Tests (OBT) for dbt
# This demonstrates how to run different types of tests as a batch

echo "===== Running Operational Batch Tests (OBT) ====="

# Run all tests in the operational directory
echo "\n--- Running operational tests ---"
dbt test --select test_type:singular --models tag:operational

# Run all generic tests applied to models
echo "\n--- Running generic tests ---"
dbt test --select test_type:generic

# Run specific tests for Star Wars models
echo "\n--- Running Star Wars data quality tests ---"
dbt test --select test_type:singular --models path:tests/operational/test_sw_character_quality.sql

# Run freshness tests
echo "\n--- Running data freshness tests ---"
dbt test --select test_type:singular --models path:tests/operational/test_data_freshness.sql

# Run all tests and output results to a JSON file for monitoring
echo "\n--- Running all tests with JSON output ---"
dbt test --output json --output-path=target/obt_results.json

echo "\n===== OBT Testing Complete ====="
echo "Results available in target/obt_results.json" 