// Package auth is the development identity adapter: stateless HMAC-signed
// bearer tokens naming a subject. A real OIDC/JWT adapter replaces Verify
// without touching handlers, which only see an Identity.
//
// Subjects, one per role:
//
//	tenant:<slug>  store owner: reads exactly one store
//	org:<slug>     org admin: reads the org's consolidated dashboard and every store in the org
//	operator       superuser: reads every org and store, and the /monitor stats
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strings"
)

type Identity struct {
	Subject    string
	TenantSlug string // store owner
	OrgSlug    string // org admin
	Operator   bool   // superuser
}

// Role names the identity's access level.
func (id Identity) Role() string {
	switch {
	case id.Operator:
		return "superuser"
	case id.OrgSlug != "":
		return "org_admin"
	}
	return "store_owner"
}

// CanRead reports whether the identity may read a store; orgSlug is the org
// the store belongs to ("" for none).
func (id Identity) CanRead(slug, orgSlug string) bool {
	return id.Operator || (id.TenantSlug != "" && id.TenantSlug == slug) || (id.OrgSlug != "" && id.OrgSlug == orgSlug)
}

// CanReadOrg reports whether the identity may read an org's consolidated data.
func (id Identity) CanReadOrg(orgSlug string) bool {
	return id.Operator || (id.OrgSlug != "" && id.OrgSlug == orgSlug)
}

type Signer struct{ key []byte }

func NewSigner(secret string) (*Signer, error) {
	if len(secret) < 16 {
		return nil, errors.New("AUTH_SECRET must be at least 16 characters")
	}
	return &Signer{key: []byte(secret)}, nil
}

const prefix = "tpdev."

func (s *Signer) Issue(subject string) string {
	p := base64.RawURLEncoding.EncodeToString([]byte(subject))
	return prefix + p + "." + s.sign(p)
}

func (s *Signer) sign(payload string) string {
	m := hmac.New(sha256.New, s.key)
	m.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

var ErrInvalidToken = errors.New("invalid token")

func (s *Signer) Verify(token string) (Identity, error) {
	token = strings.TrimSpace(strings.TrimPrefix(token, "Bearer "))
	if !strings.HasPrefix(token, prefix) {
		return Identity{}, ErrInvalidToken
	}
	parts := strings.Split(token[len(prefix):], ".")
	if len(parts) != 2 || !hmac.Equal([]byte(s.sign(parts[0])), []byte(parts[1])) {
		return Identity{}, ErrInvalidToken
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return Identity{}, ErrInvalidToken
	}
	return ParseSubject(string(raw))
}

func ParseSubject(sub string) (Identity, error) {
	switch {
	case sub == "operator":
		return Identity{Subject: sub, Operator: true}, nil
	case strings.HasPrefix(sub, "tenant:") && len(sub) > len("tenant:"):
		return Identity{Subject: sub, TenantSlug: sub[len("tenant:"):]}, nil
	case strings.HasPrefix(sub, "org:") && len(sub) > len("org:"):
		return Identity{Subject: sub, OrgSlug: sub[len("org:"):]}, nil
	}
	return Identity{}, ErrInvalidToken
}
