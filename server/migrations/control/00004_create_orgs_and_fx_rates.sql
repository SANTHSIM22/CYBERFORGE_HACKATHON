-- +goose Up
-- An organization owns several stores. Its admins see one consolidated
-- dashboard across them, with money converted into its reporting currency.
CREATE TABLE orgs (
    org_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_slug           TEXT NOT NULL UNIQUE CHECK (org_slug ~ '^[a-z0-9][a-z0-9-]{1,40}$'),
    display_name       TEXT NOT NULL,
    reporting_currency CHAR(3) NOT NULL DEFAULT 'USD' CHECK (reporting_currency ~ '^[A-Z]{3}$'),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A store belongs to at most one org; stores without one have no org dashboard.
ALTER TABLE tenants ADD COLUMN org_id UUID REFERENCES orgs (org_id);
CREATE INDEX idx_tenants_org ON tenants (org_id) WHERE org_id IS NOT NULL;

-- Fixed conversion rates for org dashboards: units of the currency per 1 USD.
-- Converted totals are approximate by design; exact per-currency totals are
-- always shown next to them. Change with `tpctl fx set --currency X --per-usd N`.
CREATE TABLE fx_rates (
    currency      CHAR(3) PRIMARY KEY CHECK (currency ~ '^[A-Z]{3}$'),
    units_per_usd NUMERIC(20, 8) NOT NULL CHECK (units_per_usd > 0),
    as_of         DATE NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Demo rates for the currencies the generator uses (approximate, not live).
INSERT INTO fx_rates (currency, units_per_usd, as_of) VALUES
    ('USD', 1, '2026-11-01'), ('EUR', 0.86, '2026-11-01'), ('GBP', 0.76, '2026-11-01'),
    ('INR', 88.00, '2026-11-01'), ('NPR', 141.00, '2026-11-01'), ('BRL', 5.40, '2026-11-01'),
    ('SGD', 1.30, '2026-11-01'), ('AUD', 1.53, '2026-11-01');

GRANT SELECT ON orgs, fx_rates TO tp_api, tp_ingest;

-- +goose Down
DROP TABLE fx_rates;
DROP INDEX idx_tenants_org;
ALTER TABLE tenants DROP COLUMN org_id;
DROP TABLE orgs;
