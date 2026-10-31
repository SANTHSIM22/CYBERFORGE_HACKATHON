// Package gen generates deterministic, skewed commerce data and bulk-loads it
// with binary COPY.
package gen

import "fmt"

// Profile is the deterministic identity of the i-th generated tenant.
type Profile struct {
	Slug        string
	Name        string
	Tier        string
	Timezone    string
	Currency    string
	AltCurrency string // "" = single-currency tenant
	Weight      int
}

var zones = []struct{ tz, cur string }{
	{"Asia/Kolkata", "INR"}, {"America/New_York", "USD"}, {"Europe/London", "GBP"}, {"Australia/Sydney", "AUD"},
	{"America/Los_Angeles", "USD"}, {"Europe/Berlin", "EUR"}, {"Asia/Kathmandu", "NPR"}, {"America/Sao_Paulo", "BRL"},
	{"Asia/Singapore", "SGD"}, {"UTC", "USD"},
}

var adjectives = []string{"Amber", "Bold", "Cedar", "Dapper", "Ember", "Fable", "Golden", "Harbor", "Indigo", "Juniper",
	"Kindred", "Lumen", "Maple", "Nimbus", "Orchid", "Pioneer", "Quartz", "Rustic", "Saffron", "Tidal"}
var nouns = []string{"Outfitters", "Goods", "Bazaar", "Supply", "Studio", "Market", "Collective", "Emporium", "Works", "Trading Co."}

var tierWeight = map[string]int{"S": 1, "M": 4, "L": 15, "XL": 100}

// TierFor skews tenants like real platforms: for n >= 10 roughly 1 XL, 9% L,
// 20% M and the rest S (70/20/9/1 at n = 100).
func TierFor(i, n int) string {
	if n < 10 {
		switch i {
		case 0:
			return "L"
		case 1:
			return "M"
		}
		return "S"
	}
	switch {
	case i == 0:
		return "XL"
	case i < 1+n*9/100:
		return "L"
	case i < 1+n*29/100:
		return "M"
	}
	return "S"
}

// ProfileFor returns the i-th tenant profile (0-based) for a fleet of n.
func ProfileFor(i, n int) Profile {
	z := zones[i%len(zones)]
	tier := TierFor(i, n)
	p := Profile{
		Slug:     fmt.Sprintf("tenant-%03d", i+1),
		Name:     fmt.Sprintf("%s %s", adjectives[(i*7)%len(adjectives)], nouns[(i*3)%len(nouns)]),
		Tier:     tier,
		Timezone: z.tz,
		Currency: z.cur,
		Weight:   tierWeight[tier],
	}
	if i%3 == 0 { // a third of tenants sell in two currencies
		p.AltCurrency = "USD"
		if p.Currency == "USD" {
			p.AltCurrency = "EUR"
		}
	}
	return p
}

// WeightFor returns the row-allocation weight for a tier.
func WeightFor(tier string) int {
	if w, ok := tierWeight[tier]; ok {
		return w
	}
	return 1
}
