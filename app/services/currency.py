from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from typing import Protocol

USD_QUANTUM = Decimal("0.01")


class CurrencyConversionError(Exception):
    """Base error for currency conversion failures."""


class UnsupportedCurrencyError(CurrencyConversionError):
    def __init__(self, currency: str) -> None:
        super().__init__(f"Unsupported currency: {currency}")
        self.currency = currency


class RateProviderUnavailableError(CurrencyConversionError):
    """Raised when a rate provider is temporarily unavailable."""


class RateProvider(Protocol):
    async def get_usd_rate(self, currency: str) -> Decimal:
        """Return the multiplier from the source currency to USD."""


class FixedRateProvider:
    _rates = {
        "USD": Decimal("1.00"),
        "EUR": Decimal("1.08"),
        "GBP": Decimal("1.27"),
    }

    async def get_usd_rate(self, currency: str) -> Decimal:
        normalized_currency = currency.upper()
        try:
            return self._rates[normalized_currency]
        except KeyError as error:
            raise UnsupportedCurrencyError(normalized_currency) from error


@dataclass(frozen=True, slots=True)
class ConversionResult:
    amount_usd: Decimal
    usd_rate: Decimal


class CurrencyConverter:
    def __init__(self, rate_provider: RateProvider) -> None:
        self._rate_provider = rate_provider

    async def convert_to_usd(
        self,
        amount: Decimal,
        currency: str,
    ) -> ConversionResult:
        rate = await self._rate_provider.get_usd_rate(currency)
        amount_usd = (amount * rate).quantize(USD_QUANTUM, rounding=ROUND_HALF_UP)
        return ConversionResult(amount_usd=amount_usd, usd_rate=rate)
