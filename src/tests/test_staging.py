# test_staging.py
import sys
import os
import pytest
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.staging import HandlerBranchStaging
from src.spark_session import spark

def test_clean_data():
    df = spark.createDataFrame([
        ("12345", "This is a description", "Apartment", 4.5, "[\"Wifi\", \"Kitchen\"]"),
        ("67890.0", "Another description", "House", 5.0, "[\"Gym\"]")
    ], ["zipcode", "description", "property_type", "log_price", "amenities"])

    cleaned_df = HandlerBranchStaging.clean_data(df)

    assert "zipcode" in cleaned_df.columns
    assert "description" not in cleaned_df.columns
    assert "log_price" not in cleaned_df.columns
    assert "amenities" in cleaned_df.columns

    assert cleaned_df.filter((cleaned_df['zipcode'] == '12345') | (cleaned_df['zipcode'] == '67890')).count() == 2

    assert cleaned_df.select("amenities").where("amenities LIKE '%Wifi%' AND amenities LIKE '%Kitchen%'").count() == 1
    assert cleaned_df.select("amenities").where("amenities LIKE '%Gym%'").count() == 1

if __name__ == "__main__":
    pytest.main()
