from pipeline import parse_numbers
import pytest


@pytest.mark.parametrize(
    "s, expected",
    [
        ("2024", [2024]),
        ("2019,2021,2023", [2019, 2021, 2023]),
        ("2019-2021", [2019, 2020, 2021]),  # range expands inclusive
    ],
)
def test_parse_numbers_valid(s, expected):
    assert parse_numbers(s) == expected


@pytest.mark.parametrize(
    "s",
    [
        "",
        " ",
        "2024,",
        ",2024",
        "20a4",
        "2024-",
        "-2024",
        "2024--2025",
        "2024,2025-2026",
        "2024 - 2025",
        "2025-2024",
    ],
)
def test_parse_numbers_invalid_raises(s):
    with pytest.raises(ValueError):
        parse_numbers(s)
