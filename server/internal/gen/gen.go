package gen

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"math/big"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/shopspring/decimal"

	"tenantpulse/internal/domain"
)

// Plan describes what to load into one tenant database.
type Plan struct {
	Slug        string
	Rows        int64 // target total rows across customers+products+orders+order_items
	Currency    string
	AltCurrency string
	Loc         *time.Location
	Days        int
	Seed        uint64
	Now         time.Time
}

// Counts is reported per tenant so "10M records" is never ambiguous.
type Counts struct {
	Customers  int64 `json:"customers"`
	Products   int64 `json:"products"`
	Orders     int64 `json:"orders"`
	OrderItems int64 `json:"order_items"`
}

func (c Counts) Total() int64 { return c.Customers + c.Products + c.Orders + c.OrderItems }

const (
	itemsPerOrder    = 2.05 // mean of the line-count distribution below
	customersPerOrd  = 0.12
	productsPerOrder = 0.01
	chunkOrders      = 20000
)

type gen struct {
	r     *rand.Rand
	plan  Plan
	tax   decimal.Decimal
	zipf  *rand.Zipf
	prods [][16]byte
	custs [][16]byte
}

func (g *gen) uuid() [16]byte {
	var u [16]byte
	a, b := g.r.Uint64(), g.r.Uint64()
	for i := 0; i < 8; i++ {
		u[i], u[8+i] = byte(a>>(8*i)), byte(b>>(8*i))
	}
	u[6] = (u[6] & 0x0f) | 0x40
	u[8] = (u[8] & 0x3f) | 0x80
	return u
}

func uuidString(u [16]byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
}

func numeric(d decimal.Decimal) pgtype.Numeric {
	return pgtype.Numeric{Int: new(big.Int).Set(d.Coefficient()), Exp: d.Exponent(), Valid: true}
}

// Load seeds one tenant database. If the tenant already has products it runs
// additively: existing products/customers are reused and only new orders are
// generated (with a different seed), so data is never reset.
func Load(ctx context.Context, conn *pgx.Conn, plan Plan, progress func(Counts)) (Counts, error) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(plan.Slug))
	g := &gen{r: rand.New(rand.NewPCG(plan.Seed, h.Sum64())), plan: plan}
	g.tax = decimal.New(int64(g.r.IntN(19)), -2) // 0-18 % tenant tax rate
	var c Counts

	var existing int64
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM products`).Scan(&existing); err != nil {
		return c, err
	}
	var orders int64
	if existing == 0 {
		orders = int64(float64(plan.Rows) / (1 + itemsPerOrder + customersPerOrd + productsPerOrder))
		nProd := min(max(int64(float64(orders)*productsPerOrder), 30), 5000)
		nCust := max(int64(float64(orders)*customersPerOrd), 10)
		if err := g.loadProducts(ctx, conn, nProd); err != nil {
			return c, fmt.Errorf("products: %w", err)
		}
		if err := g.loadCustomers(ctx, conn, nCust); err != nil {
			return c, fmt.Errorf("customers: %w", err)
		}
		c.Products, c.Customers = nProd, nCust
	} else {
		orders = int64(float64(plan.Rows) / (1 + itemsPerOrder))
		if err := g.reuse(ctx, conn); err != nil {
			return c, err
		}
	}
	if len(g.prods) == 0 {
		return c, fmt.Errorf("tenant has no products")
	}
	g.zipf = rand.NewZipf(g.r, 1.15, 4, uint64(len(g.prods)-1))

	for done := int64(0); done < orders; {
		n := min(int64(chunkOrders), orders-done)
		o, it, err := g.loadOrderChunk(ctx, conn, n)
		if err != nil {
			return c, fmt.Errorf("orders: %w", err)
		}
		c.Orders += o
		c.OrderItems += it
		done += n
		if progress != nil {
			progress(c)
		}
	}
	for _, t := range []string{"products", "customers", "orders", "order_items"} {
		if _, err := conn.Exec(ctx, "ANALYZE "+t); err != nil {
			return c, err
		}
	}
	return c, nil
}

func (g *gen) reuse(ctx context.Context, conn *pgx.Conn) error {
	g.r = rand.New(rand.NewPCG(g.plan.Seed^uint64(time.Now().UnixNano()), g.r.Uint64()))
	rows, err := conn.Query(ctx, `SELECT id FROM products WHERE is_active ORDER BY id`)
	if err != nil {
		return err
	}
	g.prods, err = pgx.CollectRows(rows, pgx.RowTo[[16]byte])
	if err != nil {
		return err
	}
	rows, err = conn.Query(ctx, `SELECT id FROM customers TABLESAMPLE SYSTEM (10) LIMIT 20000`)
	if err != nil {
		return err
	}
	g.custs, err = pgx.CollectRows(rows, pgx.RowTo[[16]byte])
	return err
}

var productWords = []string{"Linen", "Wool", "Canvas", "Leather", "Cotton", "Bamboo", "Ceramic", "Copper", "Oak", "Glass",
	"Silk", "Denim", "Marble", "Steel", "Velvet", "Cork"}
var productKinds = []string{"Tote", "Mug", "Jacket", "Lamp", "Notebook", "Sneaker", "Scarf", "Backpack", "Candle", "Bowl",
	"Watch", "Cap", "Planter", "Wallet", "Blanket", "Bottle"}

func (g *gen) loadProducts(ctx context.Context, conn *pgx.Conn, n int64) error {
	created := g.plan.Now.AddDate(0, 0, -g.plan.Days-30)
	i := int64(0)
	_, err := conn.CopyFrom(ctx, pgx.Identifier{"products"},
		[]string{"id", "sku", "name", "is_active", "created_at", "updated_at", "version"},
		pgx.CopyFromFunc(func() ([]any, error) {
			if i >= n {
				return nil, nil
			}
			i++
			id := g.uuid()
			g.prods = append(g.prods, id)
			name := fmt.Sprintf("%s %s %d", productWords[g.r.IntN(len(productWords))], productKinds[g.r.IntN(len(productKinds))], 100+g.r.IntN(900))
			return []any{id, fmt.Sprintf("SKU-%06d", i), name, g.r.IntN(50) != 0, created, created, int64(1)}, nil
		}))
	return err
}

func (g *gen) loadCustomers(ctx context.Context, conn *pgx.Conn, n int64) error {
	created := g.plan.Now.AddDate(0, 0, -g.plan.Days-30)
	i := int64(0)
	_, err := conn.CopyFrom(ctx, pgx.Identifier{"customers"},
		[]string{"id", "email", "display_name", "created_at", "updated_at"},
		pgx.CopyFromFunc(func() ([]any, error) {
			if i >= n {
				return nil, nil
			}
			i++
			id := g.uuid()
			if len(g.custs) < 20000 {
				g.custs = append(g.custs, id)
			}
			return []any{id, fmt.Sprintf("customer%d@%s.example.test", i, g.plan.Slug), fmt.Sprintf("Customer %d", i), created, created}, nil
		}))
	return err
}

// createdAt samples order times with ~3x growth over the window, a diurnal
// curve in the tenant's local time and a weekend lift.
func (g *gen) createdAt() time.Time {
	span := float64(g.plan.Days) * 24 * float64(time.Hour)
	for {
		x := g.r.Float64() // 0 = oldest, 1 = now
		t := g.plan.Now.Add(-time.Duration((1 - x) * span))
		lt := t.In(g.plan.Loc)
		diurnal := 0.35 + 0.65*math.Pow(math.Sin(math.Pi*float64(lt.Hour()*60+lt.Minute())/1440), 2)
		weekend := 0.85
		if wd := lt.Weekday(); wd == time.Saturday || wd == time.Sunday {
			weekend = 1.0
		}
		if g.r.Float64() < (0.33+0.67*x)*diurnal*weekend {
			return t.Truncate(time.Microsecond)
		}
	}
}

func (g *gen) status(age time.Duration) (string, int64) {
	p := g.r.IntN(100)
	switch {
	case age < 24*time.Hour:
		switch {
		case p < 5:
			return "pending", 1
		case p < 45:
			return "placed", 1
		case p < 75:
			return "paid", 2
		case p < 95:
			return "processing", 3
		}
		return "cancelled", 2
	case age < 7*24*time.Hour:
		switch {
		case p < 5:
			return "paid", 2
		case p < 20:
			return "processing", 3
		case p < 65:
			return "shipped", 4
		case p < 90:
			return "delivered", 5
		case p < 98:
			return "cancelled", 2
		}
		return "refunded", 6
	}
	switch {
	case p < 1:
		return "pending", 1
	case p < 88:
		return "delivered", 5
	case p < 95:
		return "cancelled", 2
	}
	return "refunded", 6
}

var lineCounts = []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 4, 4, 5} // mean 2.05

func (g *gen) loadOrderChunk(ctx context.Context, conn *pgx.Conn, n int64) (int64, int64, error) {
	type item struct {
		id, order, product [16]byte
		line               int32
		qty, price         decimal.Decimal
		at                 time.Time
		deleted            bool
	}
	orderRows := make([][]any, 0, n)
	items := make([]item, 0, int(float64(n)*itemsPerOrder)+16)
	hundred := decimal.NewFromInt(100)
	shipping := decimal.RequireFromString("4.99")
	for i := int64(0); i < n; i++ {
		id := g.uuid()
		at := g.createdAt()
		st, ver := g.status(g.plan.Now.Sub(at))
		cur := g.plan.Currency
		if g.plan.AltCurrency != "" && g.r.IntN(100) < 15 {
			cur = g.plan.AltCurrency
		}
		lines := lineCounts[g.r.IntN(len(lineCounts))]
		sum := decimal.Zero
		for l := 1; l <= lines; l++ {
			pid := g.prods[g.zipf.Uint64()]
			qty := decimal.NewFromInt(int64([]int{1, 1, 1, 1, 1, 1, 1, 2, 2, 3}[g.r.IntN(10)]))
			price := domain.UnitPrice(uuidString(pid))
			sum = sum.Add(qty.Mul(price))
			items = append(items, item{g.uuid(), id, pid, int32(l), qty, price, at, g.r.IntN(250) == 0})
		}
		// Order total = lines + tax + shipping: intentionally differs from line revenue.
		total := sum.Add(sum.Mul(g.tax)).Round(2)
		if sum.LessThan(hundred) {
			total = total.Add(shipping)
		}
		upd := at.Add(time.Duration(g.r.IntN(72*60)) * time.Minute)
		if upd.After(g.plan.Now) {
			upd = g.plan.Now
		}
		var deleted any
		if g.r.IntN(300) == 0 {
			deleted = upd
		}
		var cust any
		if len(g.custs) > 0 && g.r.IntN(5) != 0 {
			cust = g.custs[g.r.IntN(len(g.custs))]
		}
		orderRows = append(orderRows, []any{id, cust, at, upd, st, cur, numeric(total), ver, deleted})
	}
	if _, err := conn.CopyFrom(ctx, pgx.Identifier{"orders"},
		[]string{"id", "customer_id", "created_at", "updated_at", "status", "currency", "total_amount", "version", "deleted_at"},
		pgx.CopyFromRows(orderRows)); err != nil {
		return 0, 0, err
	}
	k := 0
	if _, err := conn.CopyFrom(ctx, pgx.Identifier{"order_items"},
		[]string{"id", "order_id", "product_id", "line_number", "quantity", "unit_price", "created_at", "updated_at", "version", "deleted_at"},
		pgx.CopyFromFunc(func() ([]any, error) {
			if k >= len(items) {
				return nil, nil
			}
			it := items[k]
			k++
			var del any
			if it.deleted {
				del = it.at
			}
			return []any{it.id, it.order, it.product, it.line, numeric(it.qty), numeric(it.price), it.at, it.at, int64(1), del}, nil
		})); err != nil {
		return 0, 0, err
	}
	return n, int64(len(items)), nil
}
