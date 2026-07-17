from datetime import date

import pytest

from atlaspump.downloader import build_archive_url


def test_build_archive_url() -> None:
    assert (
        build_archive_url("https://x/{year}/{month}/{day}/{hour}.zst", date(2026, 7, 1), 2)
        == "https://x/2026/07/01/02.zst"
    )


def test_rejects_invalid_hour() -> None:
    with pytest.raises(ValueError):
        build_archive_url("x/{hour}", date.today(), 24)
