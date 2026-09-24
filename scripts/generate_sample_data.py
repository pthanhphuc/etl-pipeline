"""Generate deterministic raw CSV fixtures for the four ETL datasets.

The fixtures deliberately contain both trusted examples and representative
defects.  They are written below the configured ``raw_data`` layout and do not
require PostgreSQL or any later pipeline/orchestration code.
"""

from __future__ import annotations

import csv
import re
import random
import shutil
from datetime import date
from pathlib import Path
from string import ascii_uppercase
from typing import Iterable


SEED = 20260915
DAY_1 = date(2026, 9, 15)
DAY_2 = date(2026, 9, 16)
ROOT = Path(__file__).resolve().parents[1]


DATASET_COLUMNS = {
    "car_brand": ["brand_code", "brand_name", "country_origin", "founded_year", "is_premium"],
    "customer": [
        "customer_id", "full_name", "email", "phone", "birth_date", "address_line",
        "city", "country", "loyalty_tier", "credit_score", "registered_at", "is_active",
    ],
    "car": [
        "car_id", "brand_code", "model", "plate_number", "manufacture_year", "color",
        "seats", "fuel_type", "daily_rate", "status", "last_service_at",
    ],
    "order": [
        "order_id", "customer_id", "car_id", "order_ts", "pickup_date", "return_date",
        "rental_days", "total_amount", "currency", "payment_method", "order_status",
    ],
}


def _configured_path(dataset: str, source_date: date) -> Path:
    """Render the configured source prefix relative to the repository root."""
    settings_path = ROOT / "config" / "settings.yaml"
    settings_text = settings_path.read_text(encoding="utf-8")
    raw_root = re.search(r"^\s+raw_root:\s*([^\s#]+)", settings_text, re.MULTILINE).group(1)
    template = re.search(r'^\s+source_prefix:\s*"([^"]+)"', settings_text, re.MULTILINE).group(1)
    rendered = template.format(
        raw_root=raw_root,
        dataset=dataset,
        yyyy=f"{source_date.year:04d}",
        mm=f"{source_date.month:02d}",
        dd=f"{source_date.day:02d}",
    )
    path = Path(rendered)
    return path if path.is_absolute() else ROOT / path


def _write_csv(dataset: str, source_date: date, rows: Iterable[dict[str, object]]) -> int:
    directory = _configured_path(dataset, source_date)
    directory.mkdir(parents=True, exist_ok=True)
    output = directory / f"{dataset}_{source_date:%Y%m%d}.csv"
    columns = DATASET_COLUMNS[dataset]
    materialized = [{column: row.get(column, "") for column in columns} for row in rows]
    with output.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        writer.writerows(materialized)
    return len(materialized)


def _clear_generated_data() -> None:
    """Remove only the generator's dataset directories before regeneration."""
    settings_text = (ROOT / "config" / "settings.yaml").read_text(encoding="utf-8")
    raw_root = Path(re.search(r"^\s+raw_root:\s*([^\s#]+)", settings_text, re.MULTILINE).group(1))
    raw_root = raw_root if raw_root.is_absolute() else ROOT / raw_root
    for dataset in DATASET_COLUMNS:
        directory = raw_root / dataset
        if directory.exists():
            shutil.rmtree(directory)


def _brand_rows() -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    brands = [
        ("TOYOTA", "Toyota", "JP", 1937, "N"),
        ("HONDA", "Honda", "JP", 1948, "N"),
        ("VINFAST", "VinFast", "VN", 2017, "Y"),
        ("FORD", "Ford", "US", 1903, "N"),
        ("BMW", "BMW", "DE", 1916, "Y"),
        ("MERCEDES", "Mercedes-Benz", "DE", 1926, "Y"),
        ("AUDI", "Audi", "DE", 1909, "Y"),
        ("KIA", "Kia", "KR", 1944, "N"),
        ("MAZDA", "Mazda", "JP", 1920, "N"),
        ("TESLA", "Tesla", "US", 2003, "Y"),
        ("LEXUS", "Lexus", "JP", 1989, "Y"),
        ("SUBARU", "Subaru", "JP", 1953, "N"),
    ]
    valid = [dict(zip(DATASET_COLUMNS["car_brand"], row)) for row in brands]
    day_1 = valid + [{"brand_code": "", "brand_name": "Missing Code", "country_origin": "US", "founded_year": "2000", "is_premium": "N"}]
    day_2 = [dict(row) for row in valid if row["brand_code"] != "LEXUS"]
    day_2[0]["brand_name"] = "Toyota Mobility"
    return day_1, day_2


def _customer_row(customer_id: int, *, name: str, email: str, phone: str,
                  birth_date: str, address: str, city: str, country: str,
                  tier: str, score: object, registered_at: str, active: str = "Y") -> dict[str, object]:
    return {
        "customer_id": str(customer_id), "full_name": name, "email": email, "phone": phone,
        "birth_date": birth_date, "address_line": address, "city": city, "country": country,
        "loyalty_tier": tier, "credit_score": str(score), "registered_at": registered_at,
        "is_active": active,
    }


def _customers() -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    phones = [
        "0120 123 4567", "+84 912 345 678", "0912.345.679", "84912345680",
        "(0912) 345 681", "120 123 4567", "0901-234-567", "0918 234 568",
    ]
    names = [
        "  nguyen   van an ", "Tran Thi Binh", "le van cuong", "Pham Van D",
        "Hoang Van E", "Vo Thi F", "Dr. maria nguyen", "nguyen van g",
        "de la cruz", "  single  ", "McDONALD van k", "mrs. ngo thi l",
        "Van Der Meer", "da silva", "Tran Thi O",
    ]
    valid: list[dict[str, object]] = []
    for offset, customer_id in enumerate(range(1001, 1016)):
        valid.append(_customer_row(
            customer_id,
            name=names[offset],
            email=f"User+{customer_id}@{'Gmail.COM' if offset % 3 == 0 else 'company.vn'}",
            phone=phones[offset % len(phones)],
            birth_date=["15/03/1987", "1995-07-22", "01/01/1990"][offset % 3],
            address=f"{12 + offset} le loi st, q {offset % 5 + 1}",
            city=["ho chi minh city", "ha noi", "da nang"][offset % 3],
            country=["vietnam", "VN", "viet nam"][offset % 3],
            tier=["gold", "SILVER", "bronze", "PLATINUM"][offset % 4],
            score=650 + offset * 7,
            registered_at=["15/03/2024 09:30", "2024-06-01 08:00:00", "06/02/2024 10:15"][offset % 3],
            active="Y" if offset % 4 else "true",
        ))

    # Additional trusted rows make the defective-row ratio close to one in six.
    for customer_id in range(1016, 1025):
        valid.append(_customer_row(
            customer_id, name=f"Customer {customer_id}", email=f"customer{customer_id}@example.com",
            phone=f"0912345{customer_id % 1000:03d}", birth_date="1988-05-05",
            address=f"{customer_id - 1000} nguyen hue rd", city="ha noi", country="VN",
            tier="SILVER", score=700, registered_at="2024-06-03 08:00:00",
        ))

    defects = [
        _customer_row(1101, name="Hoang Van Type", email="type@example.com", phone="0912345681",
                      birth_date="1991-02-02", address="3 ly thai to", city="ha noi", country="VN",
                      tier="BRONZE", score=650, registered_at="2024-06-04 08:00:00"),
        _customer_row(1102, name="", email="empty@example.com", phone="0912345682",
                      birth_date="1993-08-08", address="11 tran phu", city="ha noi", country="VN",
                      tier="GOLD", score=690, registered_at=""),
        _customer_row(1103, name="X" * 140, email="long@example.com", phone="0912345683",
                      birth_date="1993-08-08", address="11 tran phu", city="ha noi", country="VN",
                      tier="GOLD", score=690, registered_at="2024-06-05 08:00:00"),
        _customer_row(1104, name="Pham Van Bad", email="bad@example.com", phone="0912345684",
                      birth_date="1988-05-05", address="9 le duan", city="ha noi", country="VN",
                      tier="DIAMOND", score=9999, registered_at="2024-06-03 08:00:00"),
        _customer_row(1105, name="Future Person", email="abc@f.c", phone="0912345685",
                      birth_date="2099-01-01", address="1 hai ba trung", city="ha noi", country="VN",
                      tier="GOLD", score=690, registered_at="2024-06-05 08:00:00"),
        _customer_row(1106, name="Phone Address Bad", email="phone@example.com", phone="0000000000",
                      birth_date="1993-08-08", address="1 " + ", ".join(["st"] * 40), city="ha noi",
                      country="VN", tier="GOLD", score=690, registered_at="2024-06-05 08:00:00"),
    ]
    day_1 = valid + defects

    day_2: list[dict[str, object]] = []
    for offset, original in enumerate(valid[:15]):
        changed = dict(original)
        changed["loyalty_tier"] = ["SILVER", "GOLD", "PLATINUM"][offset % 3]
        changed["credit_score"] = str(720 + offset)
        changed["address_line"] = f"{50 + offset} new market rd"
        day_2.append(changed)
    for customer_id in range(2001, 2011):
        day_2.append(_customer_row(
            customer_id, name=f"New Customer {customer_id}", email=f"new{customer_id}@example.net",
            phone=f"091234{customer_id % 10000:04d}", birth_date="1992-02-02",
            address="20 le loi st", city="da nang", country="VN", tier="BRONZE", score=680,
            registered_at="2024-07-01 09:00:00",
        ))
    return day_1, day_2


def _car_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    fuels = ["PETROL", "DIESEL", "HYBRID", "ELECTRIC", "LPG"]
    for number in range(1, 16):
        rows.append({
            "car_id": f"CAR-{number:05d}", "brand_code": ["TOYOTA", "HONDA", "FORD"][number % 3],
            "model": ["Corolla", "Civic", "Ranger"][number % 3], "plate_number": f"51A-{number:05d}",
            "manufacture_year": str(2015 + number % 10), "color": ["white", "black", "blue"][number % 3],
            "seats": str(4 + number % 3), "fuel_type": fuels[number % len(fuels)],
            "daily_rate": f"{450000 + number * 12500:.2f}", "status": "AVAILABLE",
            "last_service_at": f"2026-08-{number:02d} 09:00:00",
        })
    rows.extend([
        {**rows[0], "car_id": "CAR-00016", "plate_number": "51A-1234"},  # pattern defect
        {**rows[1], "car_id": "CAR-00017", "fuel_type": "STEAM", "daily_rate": "-10.00"},  # allowed/range
        {**rows[2], "car_id": "CAR-00018", "manufacture_year": "1970"},  # range defect
        {**rows[3], "car_id": "CAR-00019", "model": "M" * 81},  # max length defect
        {**rows[4], "car_id": "CAR-00020", "seats": "many"},  # type defect
    ])
    return rows


def _order_row(order_number: int, *, rng: random.Random) -> dict[str, object]:
    pickup = date(2026, 9, 17 + order_number % 5)
    rental_days = order_number % 5 + 1
    return {
        "order_id": f"ORD-2026-{order_number:06d}", "customer_id": str(1001 + order_number % 15),
        "car_id": f"CAR-{order_number % 15 + 1:05d}",
        "order_ts": f"2026-09-{15 + order_number % 2:02d} {8 + order_number % 10:02d}:30:00",
        "pickup_date": pickup.isoformat(), "return_date": (pickup.fromordinal(pickup.toordinal() + rental_days)).isoformat(),
        "rental_days": str(rental_days), "total_amount": f"{rng.randint(25, 250) * 10000:.2f}",
        "currency": ["VND", "USD", "SGD", "EUR"][order_number % 4],
        "payment_method": ["CASH", "CARD", "TRANSFER", "EWALLET"][order_number % 4],
        "order_status": ["NEW", "CONFIRMED", "ONGOING", "COMPLETED"][order_number % 4],
    }


def _orders() -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    rng = random.Random(SEED)
    valid = [_order_row(number, rng=rng) for number in range(1, 31)]
    defects = [
        {**valid[0], "order_id": "ORD-2026-ABC123"},
        {**valid[1], "customer_id": "oops"},
        {**valid[2], "order_ts": "not-a-timestamp"},
        {**valid[3], "currency": "BTC"},
        {**valid[4], "payment_method": "CHEQUE"},
        {**valid[5], "order_status": "UNKNOWN"},
    ]
    day_1 = valid + defects
    replayed = [dict(row) for row in valid[:10]]
    new_rows = [_order_row(number, rng=rng) for number in range(31, 51)]
    return day_1, replayed + new_rows


def main() -> None:
    # Keep the seed explicit even though most fixtures are hand-authored; it
    # controls generated order amounts and documents reproducibility.
    random.seed(SEED)
    _clear_generated_data()

    brand_day_1, brand_day_2 = _brand_rows()
    customer_day_1, customer_day_2 = _customers()
    order_day_1, order_day_2 = _orders()

    counts = {
        ("car_brand", DAY_1): _write_csv("car_brand", DAY_1, brand_day_1),
        ("car_brand", DAY_2): _write_csv("car_brand", DAY_2, brand_day_2),
        ("customer", DAY_1): _write_csv("customer", DAY_1, customer_day_1),
        ("customer", DAY_2): _write_csv("customer", DAY_2, customer_day_2),
        ("car", DAY_1): _write_csv("car", DAY_1, _car_rows()),
        ("order", DAY_1): _write_csv("order", DAY_1, order_day_1),
        ("order", DAY_2): _write_csv("order", DAY_2, order_day_2),
    }

    print(f"Generated deterministic fixtures with seed {SEED}.")
    for (dataset, source_date), count in counts.items():
        print(f"{dataset:10} {source_date}: {count:2d} rows")
    print("car        2026-09-16: no file (unchanged dataset)")


if __name__ == "__main__":
    main()
