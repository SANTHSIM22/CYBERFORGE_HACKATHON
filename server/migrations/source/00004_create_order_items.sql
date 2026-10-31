-- +goose Up
CREATE TABLE order_items (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id    UUID NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    product_id  UUID NOT NULL REFERENCES products (id) ON DELETE RESTRICT,
    line_number INTEGER NOT NULL CHECK (line_number > 0),
    quantity    NUMERIC(20, 6) NOT NULL CHECK (quantity > 0),
    unit_price  NUMERIC(20, 6) NOT NULL CHECK (unit_price >= 0),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    version     BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    deleted_at  TIMESTAMPTZ,
    UNIQUE (order_id, line_number)
);
CREATE INDEX idx_order_items_product_id ON order_items (product_id);
-- (order_id, line_number) unique index already serves order_id lookups.

-- +goose Down
DROP TABLE order_items;
