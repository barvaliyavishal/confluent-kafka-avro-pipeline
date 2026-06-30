from typing import Dict


def _int_from_any(value: object, default: int = 0) -> int:
    if value is None:
        return default
    text = str(value).strip()
    if text == "":
        return default
    return int(float(text))


def _float_from_any(value: object, default: float = 0.0) -> float:
    if value is None:
        return default
    text = str(value).strip()
    if text == "":
        return default
    return float(text)


def normalize_record(row: Dict[str, object]) -> Dict[str, object]:
    description = str(row.get("Description", "")).strip()
    normalized = {
        "Invoice": str(row.get("Invoice", "")).strip(),
        "StockCode": str(row.get("StockCode", "")).strip(),
        "Description": description if description else None,
        "Quantity": _int_from_any(row.get("Quantity"), default=0),
        "InvoiceDate": str(row.get("InvoiceDate", "")).strip(),
        "Price": _float_from_any(row.get("Price"), default=0.0),
        "CustomerID": _int_from_any(row.get("CustomerID"), default=0),
        "Country": str(row.get("Country", "")).strip(),
    }
    return normalized
