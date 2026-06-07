from decimal import Decimal

import pytest

from app.services.currency import (
    CurrencyConverter,
    FixedRateProvider,
    RateProviderUnavailableError,
    UnsupportedCurrencyError,
)


class UnavailableRateProvider:
    async def get_usd_rate(self, currency: str) -> Decimal:
        raise RateProviderUnavailableError("Rate provider unavailable")


@pytest.fixture
def converter() -> CurrencyConverter:
    return CurrencyConverter(FixedRateProvider())


async def test_converts_usd_to_usd(converter: CurrencyConverter) -> None:
    result = await converter.convert_to_usd(Decimal("100"), "USD")

    assert result.amount_usd == Decimal("100.00")
    assert result.usd_rate == Decimal("1.00")


async def test_converts_eur_to_usd(converter: CurrencyConverter) -> None:
    result = await converter.convert_to_usd(Decimal("100"), "EUR")

    assert result.amount_usd == Decimal("108.00")
    assert result.usd_rate == Decimal("1.08")


async def test_unknown_currency_raises_domain_error(
    converter: CurrencyConverter,
) -> None:
    with pytest.raises(UnsupportedCurrencyError, match="Unsupported currency: JPY"):
        await converter.convert_to_usd(Decimal("100"), "JPY")


async def test_rate_provider_failure_stays_retryable() -> None:
    converter = CurrencyConverter(UnavailableRateProvider())

    with pytest.raises(
        RateProviderUnavailableError,
        match="Rate provider unavailable",
    ):
        await converter.convert_to_usd(Decimal("100"), "EUR")
