package auth

import (
	"encoding/base64"
	"strings"
	"testing"
)

// Each role reads exactly what it should, and a token cannot be forged.
func TestRoles(t *testing.T) {
	s, err := NewSigner("0123456789abcdef-test")
	if err != nil {
		t.Fatal(err)
	}
	id := func(sub string) Identity {
		t.Helper()
		got, err := s.Verify("Bearer " + s.Issue(sub))
		if err != nil {
			t.Fatalf("%s: %v", sub, err)
		}
		return got
	}
	owner, admin, super := id("tenant:tenant-007"), id("org:aurora"), id("operator")
	cases := []struct {
		who        Identity
		store, org string // a store and the org it belongs to
		want       bool
	}{
		{owner, "tenant-007", "aurora", true},
		{owner, "tenant-008", "aurora", false}, // same org, other store
		{admin, "tenant-008", "aurora", true},  // any store in the org
		{admin, "tenant-011", "blue-harbor", false},
		{admin, "tenant-099", "", false}, // a store in no org
		{super, "tenant-099", "", true},
	}
	for _, c := range cases {
		if got := c.who.CanRead(c.store, c.org); got != c.want {
			t.Errorf("%s reading %s (org %q): got %v, want %v", c.who.Subject, c.store, c.org, got, c.want)
		}
	}
	if owner.CanReadOrg("aurora") || !admin.CanReadOrg("aurora") || admin.CanReadOrg("blue-harbor") || !super.CanReadOrg("blue-harbor") {
		t.Error("org dashboards: only that org's admins and superusers")
	}
	if owner.Role() != "store_owner" || admin.Role() != "org_admin" || super.Role() != "superuser" {
		t.Errorf("roles: %s %s %s", owner.Role(), admin.Role(), super.Role())
	}
	// A store owner rewrites their token's subject to an org: the signature no longer matches.
	sig := strings.SplitN(strings.TrimPrefix(s.Issue("tenant:tenant-007"), prefix), ".", 2)[1]
	forged := prefix + base64.RawURLEncoding.EncodeToString([]byte("org:aurora")) + "." + sig
	if _, err := s.Verify(forged); err == nil {
		t.Error("a token whose subject was swapped must be rejected")
	}
}
