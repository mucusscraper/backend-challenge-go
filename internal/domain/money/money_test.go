package money

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
)

func TestParseValid(t *testing.T) {
	cases := []struct {
		in    string
		units int64
		out   string
	}{
		{"0", 0, "0.00"},
		{"0.00", 0, "0.00"},
		{"25", 2500, "25.00"},
		{"25.5", 2550, "25.50"},
		{"25.50", 2550, "25.50"},
		{"0.01", 1, "0.01"},
		{"-0.05", -5, "-0.05"},
		{"1000.00", 100000, "1000.00"},
		{"92233720368547758.07", math.MaxInt64, "92233720368547758.07"},
		{"-92233720368547758.08", math.MinInt64, "-92233720368547758.08"},
	}
	for _, c := range cases {
		m, err := Parse(c.in, "BRL")
		if err != nil {
			t.Fatalf("Parse(%q): %v", c.in, err)
		}
		if m.Units() != c.units {
			t.Errorf("Parse(%q) units = %d, want %d", c.in, m.Units(), c.units)
		}
		if m.Amount() != c.out {
			t.Errorf("Parse(%q).Amount() = %q, want %q", c.in, m.Amount(), c.out)
		}
	}
}

func TestParseInvalid(t *testing.T) {
	cases := map[string]error{
		"":                      ErrInvalidAmount,
		"NaN":                   ErrInvalidAmount,
		"Infinity":              ErrInvalidAmount,
		"-Infinity":             ErrInvalidAmount,
		"1e3":                   ErrInvalidAmount,
		"1E3":                   ErrInvalidAmount,
		"+1.00":                 ErrInvalidAmount,
		" 1.00":                 ErrInvalidAmount,
		"1.00 ":                 ErrInvalidAmount,
		"1,00":                  ErrInvalidAmount,
		"01.00":                 ErrInvalidAmount,
		"1.":                    ErrInvalidAmount,
		".5":                    ErrInvalidAmount,
		"-":                     ErrInvalidAmount,
		"0x10":                  ErrInvalidAmount,
		"1.001":                 ErrScaleExceeded,
		"1.000":                 ErrScaleExceeded,
		"92233720368547758.08":  ErrOverflow,
		"-92233720368547758.09": ErrOverflow,
		"999999999999999999999": ErrOverflow,
	}
	for in, want := range cases {
		if _, err := Parse(in, "BRL"); !errors.Is(err, want) {
			t.Errorf("Parse(%q) error = %v, want %v", in, err, want)
		}
	}
}

func TestParseNonNegativeRejectsNegative(t *testing.T) {
	if _, err := ParseNonNegative("-1.00", "BRL"); !errors.Is(err, ErrNegativeAmount) {
		t.Fatalf("expected ErrNegativeAmount, got %v", err)
	}
	if _, err := ParseNonNegative("0.00", "BRL"); err != nil {
		t.Fatalf("zero must be accepted: %v", err)
	}
}

func TestCurrencyValidation(t *testing.T) {
	for _, in := range []string{"", "brl", "BR", "BRLL", "XXX", "JPY", "B1L"} {
		if _, err := ParseCurrency(in); !errors.Is(err, ErrInvalidCurrency) {
			t.Errorf("ParseCurrency(%q) = %v, want ErrInvalidCurrency", in, err)
		}
	}
	if _, err := ParseCurrency("BRL"); err != nil {
		t.Fatal(err)
	}
}

func mustParse(t *testing.T, amount, cur string) Money {
	t.Helper()
	m, err := Parse(amount, cur)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestArithmetic(t *testing.T) {
	a := mustParse(t, "100.00", "BRL")
	b := mustParse(t, "80.00", "BRL")
	sum, err := a.Add(b)
	if err != nil || sum.Amount() != "180.00" {
		t.Fatalf("Add = %v %v", sum, err)
	}
	diff, err := b.Sub(a)
	if err != nil || diff.Amount() != "-20.00" {
		t.Fatalf("Sub = %v %v", diff, err)
	}
	neg, err := a.Neg()
	if err != nil || neg.Amount() != "-100.00" {
		t.Fatalf("Neg = %v %v", neg, err)
	}
	if c, _ := a.Cmp(b); c != 1 {
		t.Fatalf("Cmp = %d", c)
	}
	if c, _ := b.Cmp(a); c != -1 {
		t.Fatalf("Cmp = %d", c)
	}
	if c, _ := a.Cmp(a); c != 0 {
		t.Fatalf("Cmp = %d", c)
	}
	// imutabilidade: os operandos não são alterados
	if a.Amount() != "100.00" || b.Amount() != "80.00" {
		t.Fatal("operands were mutated")
	}
}

func TestCurrencyMismatch(t *testing.T) {
	brl := mustParse(t, "1.00", "BRL")
	usd := mustParse(t, "1.00", "USD")
	if _, err := brl.Add(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Add: %v", err)
	}
	if _, err := brl.Sub(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Sub: %v", err)
	}
	if _, err := brl.Cmp(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Cmp: %v", err)
	}
	if brl.Equal(usd) {
		t.Error("different currencies must not be equal")
	}
}

func TestOverflow(t *testing.T) {
	brl := MustCurrency("BRL")
	maxM, _ := FromUnits(math.MaxInt64, brl)
	minM, _ := FromUnits(math.MinInt64, brl)
	one, _ := FromUnits(1, brl)
	if _, err := maxM.Add(one); !errors.Is(err, ErrOverflow) {
		t.Errorf("max+1: %v", err)
	}
	if _, err := minM.Sub(one); !errors.Is(err, ErrOverflow) {
		t.Errorf("min-1: %v", err)
	}
	if _, err := minM.Neg(); !errors.Is(err, ErrOverflow) {
		t.Errorf("-min: %v", err)
	}
	negOne, _ := one.Neg()
	if _, err := maxM.Sub(negOne); !errors.Is(err, ErrOverflow) {
		t.Errorf("max-(-1): %v", err)
	}
	if _, err := minM.Add(negOne); !errors.Is(err, ErrOverflow) {
		t.Errorf("min+(-1): %v", err)
	}
	if got := minM.Amount(); got != "-92233720368547758.08" {
		t.Errorf("min amount = %s", got)
	}
}

func TestUninitialized(t *testing.T) {
	var zero Money
	valid := mustParse(t, "1.00", "BRL")
	if _, err := zero.Add(valid); !errors.Is(err, ErrUninitialized) {
		t.Errorf("Add: %v", err)
	}
	if _, err := valid.Sub(zero); !errors.Is(err, ErrUninitialized) {
		t.Errorf("Sub: %v", err)
	}
	if _, err := zero.Neg(); !errors.Is(err, ErrUninitialized) {
		t.Errorf("Neg: %v", err)
	}
	if _, err := zero.Cmp(valid); !errors.Is(err, ErrUninitialized) {
		t.Errorf("Cmp: %v", err)
	}
	if _, err := json.Marshal(zero); err == nil {
		t.Error("marshal of zero value must fail")
	}
	if _, err := Zero(Currency{}); !errors.Is(err, ErrUninitialized) {
		t.Errorf("Zero: %v", err)
	}
}

func TestJSONRoundTrip(t *testing.T) {
	m := mustParse(t, "25", "BRL")
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"amount":"25.00","currency":"BRL"}` {
		t.Fatalf("json = %s", b)
	}
	var back Money
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if !back.Equal(m) {
		t.Fatalf("round trip %v != %v", back, m)
	}
	// JSON numbers are refused: amount must be a string.
	if err := json.Unmarshal([]byte(`{"amount":25.00,"currency":"BRL"}`), &back); err == nil {
		t.Fatal("numeric amount must be rejected")
	}
}

func FuzzParse(f *testing.F) {
	for _, s := range []string{"0", "1.5", "-2.25", "1e5", "NaN", "92233720368547758.07"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		m, err := Parse(s, "BRL")
		if err != nil {
			return
		}
		// Every accepted value round-trips through its canonical string.
		back, err := Parse(m.Amount(), "BRL")
		if err != nil || !back.Equal(m) {
			t.Fatalf("round trip failed for %q: %v", s, err)
		}
	})
}
