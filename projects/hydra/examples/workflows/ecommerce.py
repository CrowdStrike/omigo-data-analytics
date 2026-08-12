from omigo_hydra import cluster_services_v2, cluster_common_v2
from omigo_hydra import cluster_protocol_v2
from omigo_hydra.cluster_services_v2 import SWFBuilder
from omigo_core import utils
import sys, os, argparse

"""Example: E-commerce pipeline SWF with 5 WFs — diamond pattern, multiple roots.
Demonstrates:
  1. Multiple root WFs (orders and products — two independent data sources)
  2. Fan-out (products feeds both order_enrichment and product_stats)
  3. Fan-in / diamond pattern (final_report reads from order_enrichment AND product_stats)
  4. ctx.from_maps() for root WFs with inline synthetic data
  5. ctx.read_df("upstream_name") for downstream WFs (proxy DataFrame, auto-resolved)
  6. inner_map_join with proxy DataFrame as join operand
  7. Checkpoint (intermediate output_id)
  8. SWFBuilder(ctx, params = {...}) with flat str->str params map
  9. read_df("upstream_id", params = ["key1", "key2"], prefix = "ctx") for param enrichment
  10. Variable bucket sizes (roots 60s, downstream 120s)
  11. Edge inference — no manual input_ids/output_ids anywhere

DAG (inferred automatically from ctx.read_df and join operand calls):
  orders (root, 60s)        products (root, 60s)
     |                          |
     +-> order_enrichment ------+  (join orders + products on product_id, 120s)
     |                          |
     |   product_stats ---------+  (aggregate products by category, 120s)
     |       |
     +-> final_report ----------+  (join order_enrichment + product_stats on category, 120s)

  Phase 0: orders, products              [2 independent roots]
  Phase 1: order_enrichment, product_stats [fan-in join, fan-out aggregation]
  Phase 2: final_report                   [diamond closure: reads both phase-1 outputs]

Run:
  python ecommerce.py --mode local
  python ecommerce.py --mode cluster
"""

# Command Line
parser = argparse.ArgumentParser(description = "SWF example: e-commerce pipeline (5 WFs, diamond pattern)")
parser.add_argument("--mode", choices = ["local", "cluster"], default = "local", help = "Execution mode: local (inmemory) or cluster")
args = parser.parse_args()

# point at temp dir for local, real cluster for cluster mode
if (args.mode == "local"):
    os.environ.setdefault("HYDRA_PATH", "/tmp/hydra-v2-example")

# ============================================================
# 1. Initialize
# ============================================================
utils.info("=" * 60)
utils.info("1. Initializing (mode={})".format(args.mode))
utils.info("=" * 60)

if (args.mode == "cluster"):
    cluster_services_v2.init("ecommerce")
utils.info("  HYDRA_PATH: {}".format(cluster_common_v2.HYDRA_PATH))

# ============================================================
# 2. Create ExecutorContext
# ============================================================
utils.info("")
utils.info("=" * 60)
utils.info("2. Creating ExecutorContext")
utils.info("=" * 60)

if (args.mode == "local"):
    ctx = cluster_protocol_v2.ClusterExecutorContext(
        namespace = "platform",
        session_protocol = None,
    )
else:
    ctx = cluster_protocol_v2.ClusterExecutorContext(
        namespace = "platform",
        session_protocol = cluster_services_v2.SESSION_PROTOCOL,
    )

utils.info("  namespace: {}".format(ctx.namespace))
utils.info("  session_protocol: {}".format("None (local)" if (ctx.session_protocol is None) else "set"))

# ============================================================
# 3. Build broadcast variables
# ============================================================
utils.info("")
utils.info("=" * 60)
utils.info("3. Building broadcast variables")
utils.info("=" * 60)

bctx = ctx.get_broadcast_context()
bctx.min_order_total = bctx.asFloat(25.0)
bctx.min_price = bctx.asFloat(10.0)

utils.info("  min_order_total = {}".format(bctx.min_order_total.value))
utils.info("  min_price       = {}".format(bctx.min_price.value))

# ============================================================
# 4. Define SWF params
# ============================================================
utils.info("")
utils.info("=" * 60)
utils.info("4. Defining SWF params")
utils.info("=" * 60)

swf_params = {
    "region": "us-west",
    "currency": "USD",
    "report_date": "2026-05-03",
}
utils.info("  params: {}".format(swf_params))

# ============================================================
# 5. Define WF blueprints
# ============================================================
utils.info("")
utils.info("=" * 60)
utils.info("5. Defining WF blueprints")
utils.info("=" * 60)

# --- WF: orders (root) ---
# Inline 8-row order dataset. Filters orders with total >= min_order_total (BVar).
# Adds checkpoint after filter.
orders_blueprint = ctx.from_maps([
    {"order_id": "ORD-001", "product_id": "P01", "customer": "Alice",   "quantity": "2", "order_total": "59.98", "region": "us-west"},
    {"order_id": "ORD-002", "product_id": "P02", "customer": "Bob",     "quantity": "1", "order_total": "24.99", "region": "us-east"},
    {"order_id": "ORD-003", "product_id": "P03", "customer": "Charlie", "quantity": "3", "order_total": "89.97", "region": "us-west"},
    {"order_id": "ORD-004", "product_id": "P04", "customer": "Diana",   "quantity": "1", "order_total": "12.50", "region": "eu-west"},
    {"order_id": "ORD-005", "product_id": "P01", "customer": "Eve",     "quantity": "1", "order_total": "29.99", "region": "us-west"},
    {"order_id": "ORD-006", "product_id": "P05", "customer": "Frank",   "quantity": "2", "order_total": "39.98", "region": "us-east"},
    {"order_id": "ORD-007", "product_id": "P03", "customer": "Grace",   "quantity": "1", "order_total": "29.99", "region": "us-west"},
    {"order_id": "ORD-008", "product_id": "P06", "customer": "Hank",    "quantity": "4", "order_total": "119.96", "region": "eu-west"},
]) \
    .ge_float("order_total", bctx.min_order_total.value) \
    .checkpoint("orders_filtered") \
    .sort("order_total") \
    .to_wf_spec()

utils.info("  orders:            {} job op(s)  [root, filter order_total >= {}, checkpoint, sort]".format(
    len(orders_blueprint.jobs_operations), bctx.min_order_total.value))

# --- WF: products (root) ---
# Inline 6-row product catalog. Filters products with price >= min_price (BVar).
# Adds a price_band transform column.
products_blueprint = ctx.from_maps([
    {"product_id": "P01", "product_name": "Wireless Mouse",    "price": "29.99", "category": "Electronics", "in_stock": "true"},
    {"product_id": "P02", "product_name": "Python Handbook",   "price": "24.99", "category": "Books",       "in_stock": "true"},
    {"product_id": "P03", "product_name": "USB-C Hub",         "price": "29.99", "category": "Electronics", "in_stock": "true"},
    {"product_id": "P04", "product_name": "Sci-Fi Novel",      "price": "12.50", "category": "Books",       "in_stock": "false"},
    {"product_id": "P05", "product_name": "Running Shoes",     "price": "19.99", "category": "Clothing",    "in_stock": "true"},
    {"product_id": "P06", "product_name": "Mechanical Keyboard", "price": "29.99", "category": "Electronics", "in_stock": "true"},
]) \
    .eq_str("in_stock", "true") \
    .ge_float("price", bctx.min_price.value) \
    .transform("price", lambda p: "premium" if (float(p) >= 25.0) else "budget", "price_band") \
    .to_wf_spec()

utils.info("  products:          {} job op(s)  [root, filter in_stock + price >= {}, transform price_band]".format(
    len(products_blueprint.jobs_operations), bctx.min_price.value))

# --- WF: order_enrichment (downstream of orders + products) ---
# Reads upstream "orders" via ctx.read_df proxy. Joins with upstream "products" via
# ctx.read_df proxy as join operand. Demonstrates fan-in (two upstream sources).
# Also demonstrates param enrichment: region + currency from seed_input params.
order_enrichment_blueprint = ctx.read_df("orders", params = ["region", "currency"], prefix = "ctx") \
    .inner_map_join(ctx.read_df("products"), ["product_id"]) \
    .to_wf_spec()

utils.info("  order_enrichment:  {} job op(s)  [reads orders + products, join on product_id, param enrichment]".format(
    len(order_enrichment_blueprint.jobs_operations)))

# --- WF: product_stats (downstream of products) ---
# Reads upstream "products", aggregates by category using group_count.
# Demonstrates fan-out from products root (products feeds both order_enrichment and product_stats).
product_stats_blueprint = ctx.read_df("products") \
    .select(["category", "product_name", "price"]) \
    .group_count(["category"]) \
    .sort("category") \
    .to_wf_spec()

utils.info("  product_stats:     {} job op(s)  [reads products, group_count by category]".format(
    len(product_stats_blueprint.jobs_operations)))

# --- WF: final_report (downstream of order_enrichment + product_stats) ---
# Diamond closure: reads order_enrichment (primary input) and joins with product_stats
# on category. Adds sequence number. Demonstrates diamond pattern resolution.
final_report_blueprint = ctx.read_df("order_enrichment") \
    .inner_map_join(ctx.read_df("product_stats"), ["category"]) \
    .add_seq_num("row_num") \
    .to_wf_spec()

utils.info("  final_report:      {} job op(s)  [reads order_enrichment + product_stats, diamond join, seq_num]".format(
    len(final_report_blueprint.jobs_operations)))

# ============================================================
# 6. Materialize SWF
# ============================================================
utils.info("")
utils.info("=" * 60)
utils.info("6. Materializing SWF (mode={})".format(args.mode))
utils.info("=" * 60)

swf_id = (
    SWFBuilder(ctx, params = swf_params)
        .add_wf("orders", orders_blueprint,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("products", products_blueprint,
                 is_live = True, bucket_interval = 5, duration = 30)
        .add_wf("order_enrichment", order_enrichment_blueprint,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("product_stats", product_stats_blueprint,
                 is_live = True, bucket_interval = 10, duration = 30)
        .add_wf("final_report", final_report_blueprint,
                 is_live = True, bucket_interval = 10, duration = 30)
        .materialize()
)

utils.info("  SWF result: {}".format(swf_id))

# ============================================================
# 7. Read and display results
# ============================================================
utils.info("")
utils.info("=" * 60)
utils.info("7. Results")
utils.info("=" * 60)

if (args.mode == "local"):
    orders_out = ctx.read_output("orders")
    orders_filtered_out = ctx.read_output("orders_filtered")
    products_out = ctx.read_output("products")
    order_enrichment_out = ctx.read_output("order_enrichment")
    product_stats_out = ctx.read_output("product_stats")
    final_report_out = ctx.read_output("final_report")
else:
    # wait for SWF completion
    final_state = cluster_services_v2.wait_for_swf_completion("platform", swf_id)
    status = cluster_services_v2.get_swf_status("platform", swf_id)
    utils.info("  SWF final state: {}".format(status["swf_state"]))
    for wf_info in status["wfs"]:
        label = wf_info["name"] if (wf_info["name"] != "") else wf_info["wf_id"]
        utils.info("    WF {} ({}): {}".format(label, wf_info["wf_id"], wf_info["state"]))

    if (not status["is_successful"]):
        utils.info("  ERROR: SWF failed")
        sys.exit(1)

    orders_out = cluster_services_v2.read_swf_live_output("platform", swf_id, "orders")
    orders_filtered_out = cluster_services_v2.read_swf_live_output("platform", swf_id, "orders_filtered")
    products_out = cluster_services_v2.read_swf_live_output("platform", swf_id, "products")
    order_enrichment_out = cluster_services_v2.read_swf_live_output("platform", swf_id, "order_enrichment")
    product_stats_out = cluster_services_v2.read_swf_live_output("platform", swf_id, "product_stats")
    final_report_out = cluster_services_v2.read_swf_live_output("platform", swf_id, "final_report")

utils.info("")
utils.info("  orders (sorted):          {} rows, cols: {}".format(orders_out.num_rows(), orders_out.get_columns()))
utils.info("  orders_filtered (ckpt):   {} rows".format(orders_filtered_out.num_rows()))
utils.info("  products:                 {} rows, cols: {}".format(products_out.num_rows(), products_out.get_columns()))
utils.info("  order_enrichment (join):  {} rows, cols: {}".format(order_enrichment_out.num_rows(), order_enrichment_out.get_columns()))
utils.info("  product_stats (agg):      {} rows, cols: {}".format(product_stats_out.num_rows(), product_stats_out.get_columns()))
utils.info("  final_report (diamond):   {} rows, cols: {}".format(final_report_out.num_rows(), final_report_out.get_columns()))

# ============================================================
# Summary
# ============================================================
utils.info("")
utils.info("=" * 60)
utils.info("DONE - E-commerce pipeline SWF completed")
utils.info("=" * 60)
utils.info("")
utils.info("  DAG executed:")
utils.info("    orders (root)            -> {} rows  [filter total >= {}]".format(orders_out.num_rows(), bctx.min_order_total.value))
utils.info("    products (root)          -> {} rows  [filter in_stock, price >= {}]".format(products_out.num_rows(), bctx.min_price.value))
utils.info("    order_enrichment (join)  -> {} rows  [orders x products on product_id]".format(order_enrichment_out.num_rows()))
utils.info("    product_stats (agg)      -> {} rows  [group_count by category]".format(product_stats_out.num_rows()))
utils.info("    final_report (diamond)   -> {} rows  [order_enrichment x product_stats on category]".format(final_report_out.num_rows()))
