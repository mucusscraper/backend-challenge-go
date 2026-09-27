// Package money implementa o value object Money usado em todos os cálculos
// financeiros do serviço.
//
// # Representação
//
// Um valor Money é um par imutável (units, currency), onde units é um int64
// com sinal contando a unidade menor da moeda (centavos para BRL). O serviço
// opera com escala fixa de duas casas decimais, portanto só são aceitas moedas
// ISO 4217 cujo expoente da unidade menor seja 2 (ver supportedCurrencies).
//
// Limites: o intervalo representável é [-92233720368547758.08, 92233720368547758.07].
// Toda operação que poderia sair desse intervalo (parsing, Add, Sub, Neg)
// retorna ErrOverflow em vez de causar overflow silencioso.
//
// Tipos de ponto flutuante nunca são usados: o parsing é feito dígito a dígito
// na string decimal e a serialização formata o inteiro diretamente.
package money

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Scale é o número fixo de casas decimais em todo valor Money.
const Scale = 2

// unitsPerMajor é 10^Scale, o número de unidades menores em uma unidade maior.
const unitsPerMajor = 100

// Erros sentinela. Chamadores classificam falhas com errors.Is.
var (
	// ErrInvalidAmount indica uma string decimal malformada (vazia, NaN,
	// Infinity, notação científica, '+' inicial, caracteres estranhos...).
	ErrInvalidAmount = errors.New("money: invalid amount")
	// ErrScaleExceeded indica mais dígitos fracionários do que Scale.
	ErrScaleExceeded = errors.New("money: amount exceeds the supported scale of 2 decimal places")
	// ErrNegativeAmount indica um valor negativo onde só são permitidos
	// valores não-negativos (entradas financeiras externas).
	ErrNegativeAmount = errors.New("money: negative amount not allowed")
	// ErrInvalidCurrency indica um código ISO 4217 desconhecido ou malformado.
	ErrInvalidCurrency = errors.New("money: invalid or unsupported currency")
	// ErrCurrencyMismatch indica aritmética/comparação entre moedas distintas.
	ErrCurrencyMismatch = errors.New("money: currency mismatch")
	// ErrOverflow indica um resultado fora do intervalo de unidades menores int64.
	ErrOverflow = errors.New("money: amount overflows the supported range")
	// ErrUninitialized indica o uso de um Money ou Currency com valor zero.
	ErrUninitialized = errors.New("money: uninitialized value")
)

// supportedCurrencies lista os códigos ISO 4217 aceitos pelo serviço. Todos
// usam duas casas decimais, correspondendo ao Scale fixo.
var supportedCurrencies = map[string]struct{}{
	"BRL": {}, "USD": {}, "EUR": {}, "GBP": {}, "ARS": {}, "MXN": {},
	"CAD": {}, "AUD": {}, "CHF": {}, "CNY": {}, "COP": {}, "PEN": {},
}

// Currency é um código ISO 4217 validado. O valor zero é inválido,
// permitindo que todo consumidor detecte moedas não inicializadas.
type Currency struct {
	code string
}

// ParseCurrency valida um código ISO 4217. Os códigos devem ter exatamente três
// letras maiúsculas e pertencer a supportedCurrencies; entradas em minúsculas
// são rejeitadas em vez de normalizadas para que o hash de idempotência veja
// uma única forma canônica.
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

// MustCurrency é um helper para testes e constantes; entra em panic com
// entrada inválida e nunca deve ser usado com dados não confiáveis.
func MustCurrency(code string) Currency {
	c, err := ParseCurrency(code)
	if err != nil {
		panic(err)
	}
	return c
}

// Code retorna o código ISO 4217 ("" para o valor zero).
func (c Currency) Code() string { return c.code }

// IsValid informa se a moeda foi construída por ParseCurrency.
func (c Currency) IsValid() bool { return c.code != "" }

// String implementa fmt.Stringer.
func (c Currency) String() string { return c.code }

// Money é um valor monetário imutável. Todos os métodos retornam novos valores;
// nenhum deles muta o receptor. O valor zero é inválido (sem moeda) e é
// rejeitado por toda operação com ErrUninitialized.
type Money struct {
	units    int64
	currency Currency
}

// FromUnits constrói Money a partir de um valor já expresso em unidades menores,
// como armazenado no banco de dados (BIGINT). Valores negativos são aceitos
// porque diferenças e cálculos internos podem ser negativos.
func FromUnits(units int64, currency Currency) (Money, error) {
	if !currency.IsValid() {
		return Money{}, ErrUninitialized
	}
	return Money{units: units, currency: currency}, nil
}

// Zero retorna o valor zero para uma moeda.
func Zero(currency Currency) (Money, error) {
	return FromUnits(0, currency)
}

// Parse constrói Money a partir de uma string decimal como "25.00" ou "-3.5".
// Destina-se a representações internas/confiáveis; entradas financeiras externas
// devem passar por ParseNonNegative.
//
// Gramática aceita: -?(0|[1-9][0-9]*)(\.[0-9]{1,2})?
// Rejeitados: "", "NaN", "Infinity", "1e3", "+1", "01", "1.", ".5", "1.234".
// Frações mais curtas são normalizadas ("25" e "25.5" tornam-se "25.00" e
// "25.50"); frações mais longas são rejeitadas, nunca arredondadas.
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

// ParseNonNegative é Parse restrito a valores >= 0. É o ponto de entrada
// para todo valor financeiro fornecido externamente (HTTP e SQS).
func ParseNonNegative(amount string, currencyCode string) (Money, error) {
	if strings.HasPrefix(amount, "-") {
		return Money{}, fmt.Errorf("%w: %q", ErrNegativeAmount, amount)
	}
	return Parse(amount, currencyCode)
}

// parseUnits converte a string decimal para unidades menores com verificações
// explícitas de overflow. Nunca passa por parsing de ponto flutuante.
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

	// Acumula como número negativo para que math.MinInt64 seja alcançável;
	// o intervalo positivo é uma unidade menor e verificado ao final.
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

// Units retorna o valor em unidades menores.
func (m Money) Units() int64 { return m.units }

// Currency retorna a moeda do valor.
func (m Money) Currency() Currency { return m.currency }

// IsValid informa se m foi construído por um construtor.
func (m Money) IsValid() bool { return m.currency.IsValid() }

// IsZero informa se o valor é exatamente zero.
func (m Money) IsZero() bool { return m.units == 0 }

// IsPositive informa se o valor é estritamente maior que zero.
func (m Money) IsPositive() bool { return m.units > 0 }

// IsNegative informa se o valor é estritamente menor que zero.
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

// Add retorna m + o. As moedas devem ser iguais; overflow é reportado.
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

// Sub retorna m - o. As moedas devem ser iguais; overflow é reportado.
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

// Neg retorna -m. Negar o int64 mínimo causa overflow e é reportado.
func (m Money) Neg() (Money, error) {
	if !m.IsValid() {
		return Money{}, ErrUninitialized
	}
	if m.units == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return Money{units: -m.units, currency: m.currency}, nil
}

// Cmp compara dois valores da mesma moeda e retorna -1, 0 ou +1.
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

// Equal informa se ambos os valores têm a mesma moeda e valor.
func (m Money) Equal(o Money) bool {
	return m.currency == o.currency && m.units == o.units
}

// Amount formata o valor com exatamente duas casas decimais, ex.: "25.00", "-0.05".
func (m Money) Amount() string {
	u := m.units
	sign := ""
	// Opera com uint64 para que math.MinInt64 seja formatado corretamente.
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

// String implementa fmt.Stringer ("25.00 BRL").
func (m Money) String() string {
	if !m.IsValid() {
		return "<invalid money>"
	}
	return m.Amount() + " " + m.currency.code
}

// DTO é a representação wire usada pelo contrato externo:
// {"amount":"25.00","currency":"BRL"}.
type DTO struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// ToDTO converte Money para sua representação wire.
func (m Money) ToDTO() DTO {
	return DTO{Amount: m.Amount(), Currency: m.currency.code}
}

// MarshalJSON serializa Money como {"amount":"25.00","currency":"BRL"}; o
// valor é sempre uma string, portanto nenhum número JSON (nem float) é usado.
func (m Money) MarshalJSON() ([]byte, error) {
	if !m.IsValid() {
		return nil, ErrUninitialized
	}
	return json.Marshal(m.ToDTO())
}

// UnmarshalJSON analisa a representação wire. Valores negativos são aceitos
// aqui porque o tipo também é usado para diferenças internas; entradas externas
// são validadas com ParseNonNegative pelos adaptadores.
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
