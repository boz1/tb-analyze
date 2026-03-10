"""
compute_pnl.py

Standalone script to compute mark-to-market PnL for all identified searcher
transactions across multiple horizons. Reads raw CSVs, enriches with auction
and price-feed data, then writes searcher_txs_with_pnl.csv.

Run from the repo root:
    python compute_pnl.py
"""

import pandas as pd
import glob

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

HORIZONS = [5]

STABLECOINS = {"USDC", "USD₮0"}

SEARCHERS = {
    "Wintermute": [
        "0xcb43d843f6cadf4f4844f3f57032468aadd9b95c",
        "0x27920e8039d2b6e93e36f5d5f53b998e2e631a70",
    ],
    "Selini": [
        "0xee2e7bbb67676292af2e31dffd1fea2276d6c7ba",
    ],
}
ALL_SEARCHER_ADDRS = [a for addrs in SEARCHERS.values() for a in addrs]

# ---------------------------------------------------------------------------
# Load raw data
# ---------------------------------------------------------------------------

print("Loading transactions...")
csv_files = glob.glob("data/case_study/timeboost_txs_parsed.csv")
txs = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)

# Drop any stale auction-derived columns from previous runs
auction_cols = [
    "merged", "auction_round", "winner_address", "winner_name",
    "top_bid_eth", "paid_bid_eth", "express_lane_controller_address",
    "round_start_time", "round_end_time", "top_bid_usd", "paid_bid_usd",
]
txs = txs.drop(columns=[c for c in auction_cols if c in txs.columns])
txs["timeboosted"] = txs["timeboosted"].fillna(False)
txs["block_time"] = pd.to_datetime(txs["block_time"], utc=True).dt.tz_convert(None)
txs = txs.sort_values("block_time")

print("Loading auctions...")
auctions = pd.read_csv("data/case_study/timeboost_auctions_casestudy.csv")
auctions["round_start_time"] = pd.to_datetime(auctions["round_start_time"], utc=True)
auctions["round_end_time"]   = pd.to_datetime(auctions["round_end_time"],   utc=True)
auctions = auctions.sort_values("round_start_time")

print("Loading price feeds...")
PRICE_COLS = [
    "open_time_us", "open", "high", "low", "close", "volume",
    "close_time_us", "quote_vol", "trades", "taker_base", "taker_quote", "ignore",
]

def load_pricefeed(path, token):
    df = pd.read_csv(path, header=None, names=PRICE_COLS)
    df["timestamp"] = pd.to_datetime(df["open_time_us"] / 1e6, unit="s", utc=True)
    df["midprice"]  = (df["high"] + df["low"]) / 2
    df = df[["timestamp", "midprice"]].rename(columns={"midprice": f"{token}_mid"})
    return df.sort_values("timestamp").reset_index(drop=True)

binance_ethusd = load_pricefeed("data/case_study/prices/ETHUSDT-1s-merged.csv", "ETH")
binance_btcusd = load_pricefeed("data/case_study/prices/BTCUSDT-1s-merged.csv", "BTC")
binance_arbusd = load_pricefeed("data/case_study/prices/ARBUSDT-1s-merged.csv", "ARB")

pricefeeds = {
    "ETH": binance_ethusd,
    "BTC": binance_btcusd,
    "ARB": binance_arbusd,
}

# ---------------------------------------------------------------------------
# Enrich txs with auction info
# ---------------------------------------------------------------------------

print("Enriching transactions with auction data...")
auctions_tz_naive = auctions.copy()
auctions_tz_naive["round_start_time"] = auctions_tz_naive["round_start_time"].dt.tz_convert(None)
auctions_tz_naive["round_end_time"]   = auctions_tz_naive["round_end_time"].dt.tz_convert(None)

# ETH price at auction start for bid USD conversion
eth_at_auction = binance_ethusd.copy()
eth_at_auction["timestamp"] = eth_at_auction["timestamp"].dt.tz_convert(None)
auctions_tz_naive = pd.merge_asof(
    auctions_tz_naive.sort_values("round_start_time"),
    eth_at_auction.rename(columns={"ETH_mid": "eth_price_at_auction_start"}).sort_values("timestamp"),
    left_on="round_start_time",
    right_on="timestamp",
    direction="backward",
)
auctions_tz_naive["top_bid_usd"]  = auctions_tz_naive["top_bid_eth"]  * auctions_tz_naive["eth_price_at_auction_start"]
auctions_tz_naive["paid_bid_usd"] = auctions_tz_naive["paid_bid_eth"] * auctions_tz_naive["eth_price_at_auction_start"]

txs_enriched = pd.merge_asof(
    txs.sort_values("block_time"),
    auctions_tz_naive[[
        "round_start_time", "round_end_time", "auction_round",
        "top_bid_usd", "paid_bid_usd", "winner_name",
    ]].sort_values("round_start_time"),
    left_on="block_time",
    right_on="round_start_time",
    direction="backward",
)
mask = (
    (txs_enriched["block_time"] >= txs_enriched["round_start_time"]) &
    (txs_enriched["block_time"] <= txs_enriched["round_end_time"])
)
txs_enriched.loc[~mask, [
    "auction_round", "top_bid_usd", "paid_bid_usd",
    "round_start_time", "round_end_time",
]] = None
txs = txs_enriched

# ETH price at block_time for fee conversion
eth_prices = binance_ethusd[["timestamp", "ETH_mid"]].copy()
eth_prices["timestamp"] = eth_prices["timestamp"].dt.tz_convert(None)
txs = pd.merge_asof(
    txs.sort_values("block_time"),
    eth_prices.rename(columns={"ETH_mid": "eth_price_at_tx"}).sort_values("timestamp"),
    left_on="block_time",
    right_on="timestamp",
    direction="backward",
)
txs["tx_fee_usd"] = txs["tx_fee_eth"] * txs["eth_price_at_tx"]

# ---------------------------------------------------------------------------
# PnL helpers
# ---------------------------------------------------------------------------

# Pre-index pricefeeds for fast asof lookup
for tok, pf in pricefeeds.items():
    pricefeeds[tok] = pf.set_index("timestamp").sort_index()


def get_mark_price(token_symbol, mark_time):
    tok = token_symbol.replace("W", "")  # WETH→ETH, WBTC→BTC
    if tok in STABLECOINS:
        return 1.0
    pf = pricefeeds.get(tok)
    if pf is None or pf.empty:
        return None
    if pf.index.tz is not None and mark_time.tzinfo is None:
        mark_time = mark_time.tz_localize(pf.index.tz)
    elif pf.index.tz is not None and mark_time.tzinfo is not None:
        mark_time = mark_time.tz_convert(pf.index.tz)
    idx = pf.index.get_indexer([mark_time], method="pad")[0]
    if idx == -1:
        return None
    return pf.iloc[idx][f"{tok}_mid"]


def compute_pnl_row(row, h):
    mark_time    = row["block_time"] + pd.to_timedelta(h, unit="s")
    buy_price    = get_mark_price(row["bought_token_symbol"], mark_time)
    sell_price   = get_mark_price(row["sold_token_symbol"],  mark_time)
    eth_price_t0 = get_mark_price("ETH", row["block_time"])
    if None in (buy_price, sell_price, eth_price_t0):
        return None
    return (
        row["bought_token_amount"] * buy_price
        - row["sold_token_amount"] * sell_price
        - row["tx_fee_eth"] * eth_price_t0
    )

# ---------------------------------------------------------------------------
# Filter to searchers and compute PnL
# ---------------------------------------------------------------------------

print(f"Filtering to {len(ALL_SEARCHER_ADDRS)} searcher addresses...")
searcher_txs = txs[txs["tx_to_address"].isin(ALL_SEARCHER_ADDRS)].copy()
searcher_txs["block_time"] = pd.to_datetime(searcher_txs["block_time"], utc=True)
searcher_txs = searcher_txs.sort_values("block_time").reset_index(drop=True)

print(f"Computing PnL for {len(searcher_txs):,} transactions across {len(HORIZONS)} horizons...")
for h in HORIZONS:
    print(f"  horizon t={h}s ...", end=" ", flush=True)
    searcher_txs[f"pnl_t{h}"] = searcher_txs.apply(
        lambda row, h=h: compute_pnl_row(row, h), axis=1
    )
    n_ok = searcher_txs[f"pnl_t{h}"].notna().sum()
    print(f"{n_ok:,} computed")

# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------

out_path = "searcher_txs_with_pnl_2.csv"
searcher_txs.to_csv(out_path, index=False)
print(f"\nSaved {len(searcher_txs):,} rows -> {out_path}")
