-- +goose Up
CREATE TABLE products (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sku        TEXT,
    name       TEXT NOT NULL,
    is_active  BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    version    BIGINT NOT NULL DEFAULT 1 CHECK (version > 0)
);
CREATE UNIQUE INDEX uq_products_sku ON products (sku) WHERE sku IS NOT NULL;

-- +goose Down
DROP TABLE products;
