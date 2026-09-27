// Package money implements the Money value object used by every financial
// computation in the service.
//
// # Representation
//
// A Money value is an immutable pair (units, currency) where units is a signed
// int64 counting the currency's minor unit (cents for BRL). The service works
// with a fixed scale of two decimal places, so only ISO 4217 currencies whose
// minor unit exponent is 2 are accepted (see supportedCurrencies).
//
// Limits: the representable range is [-92233720368547758.08, 92233720368547758.07].
// Every operation that could leave that range (parsing, Add, Sub, Neg) returns
// ErrOverflow instead of wrapping around.
//
// Floating point types are never used: parsing is done digit by digit on the
// decimal string and serialisation formats the integer directly.
package money

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Scale is the fixed number of decimal places carried by every Money value.
const Scale = 2

// unitsPerMajor is 10^Scale, the number of minor units in one major unit.
const unitsPerMajor = 100

// Sentinel errors. Callers classify failures with errors.Is.
var (
	// ErrInvalidAmount reports a malformed decimal string (empty, NaN,
	// Infinity, scientific notation, leading '+', stray characters...).
	ErrInvalidAmount = errors.New("money: invalid amount")
	// ErrScaleExceeded reports more fractional digits than Scale.
	ErrScaleExceeded = errors.New("money: amount exceeds the supported scale of 2 decimal places")
	// ErrNegativeAmount reports a negative value where only non-negative
	// amounts are allowed (external financial inputs).
	ErrNegativeAmount = errors.New("money: negative amount not allowed")
	// ErrInvalidCurrency reports an unknown or malformed ISO 4217 code.
	ErrInvalidCurrency = errors.New("money: invalid or unsupported currency")
	// ErrCurrencyMismatch reports arithmetic/comparison across currencies.
	ErrCurrencyMismatch = errors.New("money: currency mismatch")
	// ErrOverflow reports a result outside the int64 minor-unit range.
	ErrOverflow = errors.New("money: amount overflows the supported range")
	// ErrUninitialized reports the use of a zero-value Money or Currency.
	ErrUninitialized = errors.New("money: uninitialized value")
)

// supportedCurrencies lists the ISO 4217 codes accepted by the service. All of
// them use two decimal places, matching the fixed Scale.
var supportedCurrencies = map[string]struct{}{
	"BRL": {}, "USD": {}, "EUR": {}, "GBP": {}, "ARS": {}, "MXN": {},
	"CAD": {}, "AUD": {}, "CHF": {}, "CNY": {}, "COP": {}, "PEN": {},
}

// Currency is a validated ISO 4217 currency code. The zero value is invalid,
// which lets every consumer detect uninitialised currencies.
type Currency struct {
	code string
}

// ParseCurrency validates an ISO 4217 code. Codes must be exactly three
// upper-case letters and belong to supportedCurrencies; lower-case input is
// rejected instead of being normalised so that the idempotency hash sees a
// single canonical spelling.
func ParseCurrency(code string) (Currency, error) {
	if len(code) != 3 {
		return Currency{}, fmt.Errorf("%w: %q", ErrInvalidCurrency, code)
	}
	for i := 0; i < 3; i++ {
		if code[i] < 'A' || code[i] > 'Z' {
			return Currency{}, fmt.Errorf("%w: %q", ErrInvalidCurrency, code)
		}
	}
	if _, ok := supportedCurrencies[code]; !ok {
		return Currency{}, fmt.Errorf("%w: %q", ErrInvalidCurrency, code)
	}
	return Currency{code: code}, nil
}

// MustCurrency is a helper for tests and constants; it panics on invalid
// input and must never be used with untrusted data.
func MustCurrency(code string) Currency {
	c, err := ParseCurrency(code)
	if err != nil {
		panic(err)
	}
	return c
}

// Code returns the ISO 4217 code ("" for the zero value).
func (c Currency) Code() string { return c.code }

// IsValid reports whether the currency was built by ParseCurrency.
func (c Currency) IsValid() bool { return c.code != "" }

// String implements fmt.Stringer.
func (c Currency) String() string { return c.code }

// Money is an immutable monetary amount. All methods return new values; none
// of them mutate the receiver. The zero value is invalid (no currency) and is
// rejected by every operation with ErrUninitialized.
type Money struct {
	units    int64
	currency Currency
}

// FromUnits builds Money from an amount already expressed in minor units, as
// stored in the database (BIGINT). Negative values are accepted because
// differences and internal computations may be negative.
func FromUnits(units int64, currency Currency) (Money, error) {
	if !currency.IsValid() {
		return Money{}, ErrUninitialized
	}
	return Money{units: units, currency: currency}, nil
}

// Zero returns the zero amount for a currency.
func Zero(currency Currency) (Money, error) {
	return FromUnits(0, currency)
}

// Parse builds Money from a decimal string such as "25.00" or "-3.5". It is
// meant for internal/trusted representations; external financial inputs must
// go through ParseNonNegative.
//
// Accepted grammar: -?(0|[1-9][0-9]*)(\.[0-9]{1,2})?
// Rejected: "", "NaN", "Infinity", "1e3", "+1", "01", "1.", ".5", "1.234".
// Shorter fractions are normalised ("25" and "25.5" become "25.00" and
// "25.50"); longer fractions are rejected, never rounded.
func Parse(amount string, currencyCode string) (Money, error) {
	currency, err := ParseCurrency(currencyCode)
	if err != nil {
		return Money{}, err
	}
	units, err := parseUnits(amount)
	if err != nil {
		return Money{}, err
	}
	return Money{units: units, currency: currency}, nil
}

// ParseNonNegative is Parse restricted to amounts >= 0. It is the entry point
// for every externally supplied financial amount (HTTP and SQS).
func ParseNonNegative(amount string, currencyCode string) (Money, error) {
	if strings.HasPrefix(amount, "-") {
		return Money{}, fmt.Errorf("%w: %q", ErrNegativeAmount, amount)
	}
	return Parse(amount, currencyCode)
}

// parseUnits converts the decimal string to minor units with explicit
// overflow checks. It never goes through float parsing.
func parseUnits(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("%w: empty", ErrInvalidAmount)
	}
	negative := false
	if s[0] == '-' {
		negative = true
		s = s[1:]
	}
	intPart, fracPart, hasDot := strings.Cut(s, ".")
	if intPart == "" || (hasDot && fracPart == "") {
		return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, s)
	}
	if !allDigits(intPart) || !allDigits(fracPart) {
		return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, s)
	}
	if len(intPart) > 1 && intPart[0] == '0' {
		return 0, fmt.Errorf("%w: leading zeros in %q", ErrInvalidAmount, s)
	}
	if len(fracPart) > Scale {
		return 0, fmt.Errorf("%w: %q", ErrScaleExceeded, s)
	}
	for len(fracPart) < Scale {
		fracPart += "0"
	}

	// Accumulate as a negative number so that math.MinInt64 is reachable;
	// the positive range is one unit smaller and checked at the end.
	var acc int64
	for _, r := range intPart + fracPart {
		d := int64(r - '0')
		if acc < (math.MinInt64+d)/10 {
			return 0, fmt.Errorf("%w: %q", ErrOverflow, s)
		}
		acc = acc*10 - d
	}
	if negative {
		return acc, nil
	}
	if acc == math.MinInt64 {
		return 0, fmt.Errorf("%w: %q", ErrOverflow, s)
	}
	return -acc, nil
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// Units returns the amount in minor units.
func (m Money) Units() int64 { return m.units }

// Currency returns the currency of the amount.
func (m Money) Currency() Currency { return m.currency }

// IsValid reports whether m was built through a constructor.
func (m Money) IsValid() bool { return m.currency.IsValid() }

// IsZero reports whether the amount is exactly zero.
func (m Money) IsZero() bool { return m.units == 0 }

// IsPositive reports whether the amount is strictly greater than zero.
func (m Money) IsPositive() bool { return m.units > 0 }

// IsNegative reports whether the amount is strictly lower than zero.
func (m Money) IsNegative() bool { return m.units < 0 }

func (m Money) compatible(o Money) error {
	if !m.IsValid() || !o.IsValid() {
		return ErrUninitialized
	}
	if m.currency != o.currency {
		return fmt.Errorf("%w: %s vs %s", ErrCurrencyMismatch, m.currency, o.currency)
	}
	return nil
}

// Add returns m + o. Currencies must match; overflow is reported.
func (m Money) Add(o Money) (Money, error) {
	if err := m.compatible(o); err != nil {
		return Money{}, err
	}
	if (o.units > 0 && m.units > math.MaxInt64-o.units) ||
		(o.units < 0 && m.units < math.MinInt64-o.units) {
		return Money{}, ErrOverflow
	}
	return Money{units: m.units + o.units, currency: m.currency}, nil
}

// Sub returns m - o. Currencies must match; overflow is reported.
func (m Money) Sub(o Money) (Money, error) {
	if err := m.compatible(o); err != nil {
		return Money{}, err
	}
	if (o.units < 0 && m.units > math.MaxInt64+o.units) ||
		(o.units > 0 && m.units < math.MinInt64+o.units) {
		return Money{}, ErrOverflow
	}
	return Money{units: m.units - o.units, currency: m.currency}, nil
}

// Neg returns -m. Negating the minimum int64 overflows and is reported.
func (m Money) Neg() (Money, error) {
	if !m.IsValid() {
		return Money{}, ErrUninitialized
	}
	if m.units == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return Money{units: -m.units, currency: m.currency}, nil
}

// Cmp compares two amounts of the same currency and returns -1, 0 or +1.
func (m Money) Cmp(o Money) (int, error) {
	if err := m.compatible(o); err != nil {
		return 0, err
	}
	switch {
	case m.units < o.units:
		return -1, nil
	case m.units > o.units:
		return 1, nil
	default:
		return 0, nil
	}
}

// Equal reports whether both values have the same currency and amount.
func (m Money) Equal(o Money) bool {
	return m.currency == o.currency && m.units == o.units
}

// Amount formats the amount with exactly two decimals, e.g. "25.00", "-0.05".
func (m Money) Amount() string {
	u := m.units
	sign := ""
	// Work with uint64 so that math.MinInt64 formats correctly.
	var abs uint64
	if u < 0 {
		sign = "-"
		abs = uint64(-(u + 1)) + 1
	} else {
		abs = uint64(u)
	}
	whole := abs / unitsPerMajor
	frac := abs % unitsPerMajor
	return sign + strconv.FormatUint(whole, 10) + "." + fmt.Sprintf("%02d", frac)
}

// String implements fmt.Stringer ("25.00 BRL").
func (m Money) String() string {
	if !m.IsValid() {
		return "<invalid money>"
	}
	return m.Amount() + " " + m.currency.code
}

// DTO is the wire representation used by the external contract:
// {"amount":"25.00","currency":"BRL"}.
type DTO struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// ToDTO converts Money to its wire representation.
func (m Money) ToDTO() DTO {
	return DTO{Amount: m.Amount(), Currency: m.currency.code}
}

// MarshalJSON serialises Money as {"amount":"25.00","currency":"BRL"}; the
// amount is always a string so no JSON number (and no float) is involved.
func (m Money) MarshalJSON() ([]byte, error) {
	if !m.IsValid() {
		return nil, ErrUninitialized
	}
	return json.Marshal(m.ToDTO())
}

// UnmarshalJSON parses the wire representation. Negative amounts are
// accepted here because the type is also used for internal differences;
// external inputs are validated with ParseNonNegative by the adapters.
func (m *Money) UnmarshalJSON(data []byte) error {
	var dto DTO
	if err := json.Unmarshal(data, &dto); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidAmount, err)
	}
	parsed, err := Parse(dto.Amount, dto.Currency)
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}
