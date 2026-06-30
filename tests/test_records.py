from retail_kafka_pipeline.records import normalize_record


def test_normalize_record_with_full_payload() -> None:
    row = {
        "Invoice": "489434",
        "StockCode": "85048",
        "Description": "  LIGHTS  ",
        "Quantity": "12",
        "InvoiceDate": "2009-12-01 07:45:00",
        "Price": "6.95",
        "CustomerID": "13085.0",
        "Country": "United Kingdom",
    }

    out = normalize_record(row)

    assert out["Invoice"] == "489434"
    assert out["StockCode"] == "85048"
    assert out["Description"] == "LIGHTS"
    assert out["Quantity"] == 12
    assert out["Price"] == 6.95
    assert out["CustomerID"] == 13085


def test_normalize_record_handles_missing_values() -> None:
    row = {
        "Invoice": "",
        "StockCode": "",
        "Description": " ",
        "Quantity": "",
        "InvoiceDate": "",
        "Price": "",
        "CustomerID": "",
        "Country": "",
    }

    out = normalize_record(row)

    assert out["Description"] is None
    assert out["Quantity"] == 0
    assert out["Price"] == 0.0
    assert out["CustomerID"] == 0
