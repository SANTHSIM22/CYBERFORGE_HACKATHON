package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/shopspring/decimal"
)

// Orgs group stores under one owner; their admins see a consolidated
// dashboard in the org's reporting currency, converted at the fixed rates
// kept by `fx`.

func orgCmd(ctx context.Context, e *env, sub string, args []string) error {
	switch sub {
	case "list":
		return listOrgs(ctx, e)
	case "add":
		var slug, name, cur string
		if _, err := flags("org add", args, func(fs *flag.FlagSet) {
			fs.StringVar(&slug, "slug", "", "org slug, e.g. aurora")
			fs.StringVar(&name, "name", "", "display name")
			fs.StringVar(&cur, "currency", "USD", "reporting currency (ISO code)")
		}); err != nil {
			return err
		}
		if slug == "" {
			return fmt.Errorf("org add: --slug is required")
		}
		return addOrg(ctx, e, slug, or(name, slug), strings.ToUpper(cur))
	case "assign":
		var org, stores string
		if _, err := flags("org assign", args, func(fs *flag.FlagSet) {
			fs.StringVar(&org, "org", "", "org slug, or \"none\" to remove the stores from their org")
			fs.StringVar(&stores, "stores", "", "comma-separated store slugs")
		}); err != nil {
			return err
		}
		if org == "" || stores == "" {
			return fmt.Errorf("org assign: --org and --stores are required")
		}
		return assignStores(ctx, e, org, strings.Split(stores, ","))
	case "demo":
		var size int
		if _, err := flags("org demo", args, func(fs *flag.FlagSet) {
			fs.IntVar(&size, "size", 10, "stores per demo org")
		}); err != nil {
			return err
		}
		return demoOrgs(ctx, e, size, true)
	}
	return fmt.Errorf("org: unknown subcommand %q (list, add, assign, demo)", sub)
}

func listOrgs(ctx context.Context, e *env) error {
	rows, err := e.control.Query(ctx, `
		SELECT o.org_slug, o.display_name, o.reporting_currency, count(t.tenant_id),
		       coalesce(string_agg(t.tenant_slug, ',' ORDER BY t.tenant_slug), '')
		FROM orgs o LEFT JOIN tenants t USING (org_id)
		GROUP BY 1, 2, 3 ORDER BY 1`)
	if err != nil {
		return err
	}
	defer rows.Close()
	w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "ORG\tNAME\tCURRENCY\tSTORES\tSTORE SLUGS")
	n := 0
	for rows.Next() {
		var slug, name, cur, list string
		var k int
		if err := rows.Scan(&slug, &name, &cur, &k, &list); err != nil {
			return err
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\n", slug, name, strings.TrimSpace(cur), k, list)
		n++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_ = w.Flush()
	fmt.Printf("%d orgs\n", n)
	return nil
}

func addOrg(ctx context.Context, e *env, slug, name, cur string) error {
	tag, err := e.control.Exec(ctx, `INSERT INTO orgs (org_slug, display_name, reporting_currency) VALUES ($1, $2, $3)
		ON CONFLICT (org_slug) DO NOTHING`, slug, name, cur)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		fmt.Printf("org %s already exists\n", slug)
		return nil
	}
	audit(ctx, e, "", "org.add", fmt.Sprintf(`{"org":%q,"currency":%q}`, slug, cur))
	fmt.Printf("org %s created (%s, reporting in %s)\n", slug, name, cur)
	return nil
}

func assignStores(ctx context.Context, e *env, org string, stores []string) error {
	for i := range stores {
		stores[i] = strings.TrimSpace(stores[i])
	}
	var tag interface{ RowsAffected() int64 }
	var err error
	if org == "none" {
		tag, err = e.control.Exec(ctx, `UPDATE tenants SET org_id = NULL, updated_at = now() WHERE tenant_slug = ANY($1)`, stores)
	} else {
		tag, err = e.control.Exec(ctx, `UPDATE tenants SET org_id = (SELECT org_id FROM orgs WHERE org_slug = $1), updated_at = now()
			WHERE tenant_slug = ANY($2) AND EXISTS (SELECT 1 FROM orgs WHERE org_slug = $1)`, org, stores)
	}
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("no store changed: check the org slug and store slugs")
	}
	audit(ctx, e, "", "org.assign", fmt.Sprintf(`{"org":%q,"stores":%d}`, org, tag.RowsAffected()))
	fmt.Printf("%d stores now in %s\n", tag.RowsAffected(), org)
	return nil
}

var demoOrgNames = []struct{ slug, name, cur string }{
	{"aurora", "Aurora Retail Group", "INR"},
	{"blue-harbor", "Blue Harbor Holdings", "EUR"},
	{"cascade", "Cascade Commerce", "USD"},
	{"driftwood", "Driftwood Brands", "USD"},
	{"evergreen", "Evergreen Collective", "USD"},
	{"foxglove", "Foxglove Trading", "GBP"},
	{"granite-peak", "Granite Peak Retail", "USD"},
	{"horizon", "Horizon Goods Co.", "USD"},
	{"ironwood", "Ironwood Merchants", "AUD"},
	{"juniper-lane", "Juniper Lane Group", "USD"},
}

// demoOrgs groups the stores that have no org into demo orgs of `size`
// stores, in slug order. Unless forced, it does nothing once any org exists,
// so a re-run of seed never regroups stores someone assigned by hand.
func demoOrgs(ctx context.Context, e *env, size int, force bool) error {
	if size < 1 {
		return fmt.Errorf("org demo: --size must be at least 1")
	}
	var orgs int
	if err := e.control.QueryRow(ctx, `SELECT count(*) FROM orgs`).Scan(&orgs); err != nil {
		return err
	}
	if orgs > 0 && !force {
		return nil
	}
	rows, err := e.control.Query(ctx, `SELECT tenant_slug FROM tenants WHERE org_id IS NULL ORDER BY tenant_slug`)
	if err != nil {
		return err
	}
	var free []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			rows.Close()
			return err
		}
		free = append(free, s)
	}
	rows.Close()
	for i := 0; len(free) > 0; i++ {
		o := struct{ slug, name, cur string }{fmt.Sprintf("org-%02d", i+1), fmt.Sprintf("Demo Org %d", i+1), "USD"}
		if i < len(demoOrgNames) {
			o = demoOrgNames[i]
		}
		if err := addOrg(ctx, e, o.slug, o.name, o.cur); err != nil {
			return err
		}
		n := min(size, len(free))
		if err := assignStores(ctx, e, o.slug, free[:n]); err != nil {
			return err
		}
		free = free[n:]
	}
	return nil
}

func fxCmd(ctx context.Context, e *env, sub string, args []string) error {
	switch sub {
	case "list":
		rows, err := e.control.Query(ctx, `SELECT currency, units_per_usd::text, as_of::text FROM fx_rates ORDER BY currency`)
		if err != nil {
			return err
		}
		defer rows.Close()
		w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
		fmt.Fprintln(w, "CURRENCY\tUNITS PER USD\tAS OF")
		for rows.Next() {
			var c, r, d string
			if err := rows.Scan(&c, &r, &d); err != nil {
				return err
			}
			fmt.Fprintf(w, "%s\t%s\t%s\n", c, r, d)
		}
		_ = w.Flush()
		return rows.Err()
	case "set":
		var cur, rate, asOf string
		if _, err := flags("fx set", args, func(fs *flag.FlagSet) {
			fs.StringVar(&cur, "currency", "", "ISO code, e.g. INR")
			fs.StringVar(&rate, "per-usd", "", "units of the currency per 1 USD, e.g. 88.25")
			fs.StringVar(&asOf, "as-of", "", "date the rate is from (YYYY-MM-DD, default today)")
		}); err != nil {
			return err
		}
		r, err := decimal.NewFromString(rate)
		if cur == "" || err != nil || !r.IsPositive() {
			return fmt.Errorf("fx set: --currency and a positive --per-usd are required")
		}
		_, err = e.control.Exec(ctx, `INSERT INTO fx_rates (currency, units_per_usd, as_of) VALUES ($1, $2, coalesce(nullif($3, '')::date, current_date))
			ON CONFLICT (currency) DO UPDATE SET units_per_usd = EXCLUDED.units_per_usd, as_of = EXCLUDED.as_of, updated_at = now()`,
			strings.ToUpper(cur), r.String(), asOf)
		if err != nil {
			return err
		}
		audit(ctx, e, "", "fx.set", fmt.Sprintf(`{"currency":%q,"units_per_usd":%q}`, strings.ToUpper(cur), r.String()))
		fmt.Printf("%s = %s per USD (dashboards pick it up within 5 s)\n", strings.ToUpper(cur), r.String())
		return nil
	}
	return fmt.Errorf("fx: unknown subcommand %q (list, set)", sub)
}

// audit records an operator action in the control plane's audit log.
func audit(ctx context.Context, e *env, tenantID, action, details string) {
	var tid any
	if tenantID != "" {
		tid = tenantID
	}
	_, _ = e.control.Exec(ctx, `INSERT INTO tenant_admin_audit (tenant_id, action, actor, details) VALUES ($1, $2, $3, $4::jsonb)`,
		tid, action, e.prov.Actor, details)
}
