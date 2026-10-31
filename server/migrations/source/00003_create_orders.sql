-- +goose Up
-- created_at is immutable after insert and defines the reporting bucket.
-- Every update increments version exactly once in the same transaction.
CREATE TABLE orders (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id  UUID REFERENCES customers (id) ON DELETE SET NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    status       TEXT NOT NULL CHECK (status IN (
        'pending', 'placed', 'paid', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded'
    )),
    currency     CHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    total_amount NUMERIC(20, 4) NOT NULL CHECK (total_amount >= 0),
    version      BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    deleted_at   TIMESTAMPTZ
);
CREATE INDEX idx_orders_created_at ON orders (created_at);
CREATE INDEX idx_orders_status_created_at ON orders (status, created_at);
CREATE INDEX idx_orders_customer_id ON orders (customer_id) WHERE customer_id IS NOT NULL;

-- +goose Down
DROP TABLE orders;
