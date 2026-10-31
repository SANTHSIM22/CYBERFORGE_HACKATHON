-- Store (OLTP) workload for pgbench: run with capture paused and running to
-- measure what change capture costs the main database.
--   docker compose --profile tools run --rm pgbench -d tenant_001 -n -f /bench/oltp.sql -c 8 -j 4 -T 60 -P 10 -r
\set amount random(500, 50000)
BEGIN;
-- recent-orders list (typical admin page)
SELECT id, status, total_amount FROM orders WHERE created_at >= now() - interval '1 day' ORDER BY created_at DESC LIMIT 20;
-- checkout: order + one line
WITH o AS (
  INSERT INTO orders (status, currency, total_amount) VALUES ('placed', 'INR', :amount / 100.0) RETURNING id
)
INSERT INTO order_items (order_id, product_id, line_number, quantity, unit_price)
SELECT o.id, (SELECT id FROM products ORDER BY id LIMIT 1), 1, 1, :amount / 100.0 FROM o;
-- fulfilment: advance the newest placed order
UPDATE orders SET status = 'paid', version = version + 1, updated_at = now()
WHERE id = (SELECT id FROM orders WHERE status = 'placed' ORDER BY created_at DESC LIMIT 1 FOR UPDATE SKIP LOCKED);
END;
