"""
╔══════════════════════════════════════════════════════════════════════╗
║           WAVE — Data Fetcher  (Daily / Weekly / Hist)              ║
╚══════════════════════════════════════════════════════════════════════╝

Downloads OHLC data for all WAVE stocks and writes JSON cache files
directly into your App_Wave/data/ folder — the exact format WAVE reads
at startup, so scans and backfill run instantly with zero network calls.

FOLDER STRUCTURE (output):
  App_Wave/
  └── data/
      ├── yf_cache/                       ← scan cache (fetchOHLCV layers)
      │   ├── HDFCBANK_NS_1y_1d.json          ← --tf 1d  primary
      │   ├── HDFCBANK_NS_5y_1wk.json         ← --tf 1d  HTF1  (also --tf 1wk primary)
      │   ├── HDFCBANK_NS_10y_1mo.json        ← --tf 1d  HTF2  ← weekly scan HTF
      │   └── ...  (symbol × timeframe × HTF level)
      └── wave_hist/                      ← backfill history (--hist)
          ├── HDFCBANK.json                   ← packed 2Y daily candles
          ├── SECTOR_BANK_1wk.json            ← sector HTF candles
          ├── SECTOR_BANK_1mo.json            ← monthly — weekly scan fallback
          └── ...

WHY --tf 1d WRITES TWO HTF LEVELS:
  WAVE's weekly scan uses HTF_MAP['1wk'] = {interval:'1mo', period:'10y'}.
  --tf 1d writes both 1wk and 1mo so running just "--tf 1d --merge" daily
  is sufficient for all scan timeframes (Daily + Weekly).

FILE FORMAT — yf_cache (matches WAVE's fetchOHLCV cache exactly):
  {
    "ts": 1711234567890,          ← epoch ms — WAVE uses this for 48h TTL
    "data": [
      {"date":"2024-01-02T09:15:00.000Z", "open":2450.1, "high":2465.5,
       "low":2430.0, "close":2458.3, "volume":5123456},
      ...
    ]
  }

FILE FORMAT — wave_hist (matches WAVE's packCandles/unpackCandles exactly):
  {
    "ts": 1711234567890,          ← epoch ms — WAVE uses this for 7-day TTL
    "c": [
      ["2024-01-02", 2450.10, 2465.50, 2430.00, 2458.30, 5123456],
      ...                         ← [date10, open, high, low, close, volume]
    ]
  }

SETUP:
  pip install yfinance pandas

USAGE:
  # ── Recommended daily cron (covers ALL scan timeframes) ─────────────
  python wave_fetch.py --tf 1d --merge   # writes 1d + 1wk + 1mo HTF files
  python wave_fetch.py --hist --merge    # refreshes backfill history

  # ── Scan cache (yf_cache/) ──────────────────────────────────────────
  python wave_fetch.py --tf 1d          # daily + weekly + monthly HTF
  python wave_fetch.py --tf 1wk         # weekly + monthly HTF
  python wave_fetch.py --tf all         # both timeframes

  # ── Backfill history (wave_hist/) ──────────────────────────────────
  python wave_fetch.py --hist           # 2Y daily for all F&O + indices
                                        # + sector HTF (1wk/1mo)
                                        # → Backfill Step 1 runs from disk,
                                        #   zero Yahoo network calls in browser

  # ── Full pre-load (run once before opening WAVE) ────────────────────
  python wave_fetch.py --tf 1d          # fills yf_cache/ for all scan TFs
  python wave_fetch.py --hist           # fills wave_hist/ for backfill

  # ── Incremental (skip files still fresh) ────────────────────────────
  python wave_fetch.py --tf 1d --merge
  python wave_fetch.py --hist --merge   # skips symbols fresh within 7 days

  # ── Target specific stocks ──────────────────────────────────────────
  python wave_fetch.py --tf 1d --symbols RELIANCE TCS HDFCBANK
  python wave_fetch.py --hist --symbols SBIN HDFCBANK ICICIBANK

  # ── Sector filter (scan cache only) ─────────────────────────────────
  python wave_fetch.py --tf 1d --sector BANK

  # ── Quick test with first 10 stocks ─────────────────────────────────
  python wave_fetch.py --tf 1d --top 10
  python wave_fetch.py --hist --top 10

  # ── Summary of what's cached ─────────────────────────────────────────
  python wave_fetch.py --summary        # yf_cache/ summary
  python wave_fetch.py --hist --summary # wave_hist/ summary

  # ── Custom App_Wave folder location ─────────────────────────────────
  python wave_fetch.py --tf 1d --outdir /path/to/App_Wave/data/yf_cache
  python wave_fetch.py --hist  --outdir /path/to/App_Wave/data

SCHEDULE (cron — daily after market close ~4 PM IST = 10:30 UTC):
  30 10 * * 1-5  cd /path/to/project && python wave_fetch.py --tf 1d --merge
  30 10 * * 1-5  cd /path/to/project && python wave_fetch.py --hist --merge

HOW TO CONNECT IN WAVE:
  1. Run this script to pre-populate yf_cache/ and wave_hist/
  2. Open WAVE.html in Chrome
  3. Click the 📁 folder icon (top-right) → select your App_Wave folder
  4. Run a scan  — data loads from yf_cache/, no proxies needed
  5. Run Backfill — Step 1 reads wave_hist/ from disk, no Yahoo calls
"""

from __future__ import annotations
import sys, time, json, argparse, re
from datetime import datetime, timezone
from pathlib import Path

try:
    import yfinance as yf
    import pandas as pd
except ImportError:
    print("Missing dependencies. Run:  pip install yfinance pandas")
    sys.exit(1)


# ═══════════════════════════════════════════════════════════════════════════════
# WAVE stock universe — mirrors ALL_STOCKS in WAVE.html exactly
# Each entry: (symbol, yahoo_ticker, sector, cap, fno)
# ═══════════════════════════════════════════════════════════════════════════════
ALL_STOCKS = [
    # ── Indices ─────────────────────────────────────────────
    ("NIFTY", "^NSEI", "INDEX", "index", False),
    ("BANKNIFTY", "^NSEBANK", "INDEX", "index", False),
    ("FINNIFTY", "NIFTY_FIN_SERVICE.NS", "INDEX", "index", False),
    ("MIDCPNIFTY", "^NSEMDCP50", "INDEX", "index", False),
    # ── BANK ────────────────────────────────────────────────

    # ── 3 ──────────────────────────────────────────────────────
    ("360ONE", "360ONE.NS", "FINANCE", "mid", True),
    # ── A ──────────────────────────────────────────────────────
    ("ABB", "ABB.NS", "INFRA", "large", True),
    ("ABCAPITAL", "ABCAPITAL.NS", "FINANCE", "mid", True),
    ("ADANIENSOL", "ADANIENSOL.NS", "ENERGY", "large", True),
    ("ADANIENT", "ADANIENT.NS", "ENERGY", "large", True),
    ("ADANIGREEN", "ADANIGREEN.NS", "ENERGY", "large", True),
    ("ADANIPORTS", "ADANIPORTS.NS", "ENERGY", "large", True),
    ("ALKEM", "ALKEM.NS", "PHARMA", "mid", True),
    ("AMBER", "AMBER.NS", "CONSUMER", "mid", True),
    ("AMBUJACEM", "AMBUJACEM.NS", "INFRA", "large", True),
    ("ANGELONE", "ANGELONE.NS", "FINANCE", "mid", True),
    ("APLAPOLLO", "APLAPOLLO.NS", "CONSUMER", "mid", True),
    ("APOLLOHOSP", "APOLLOHOSP.NS", "PHARMA", "large", True),
    ("ASHOKLEY", "ASHOKLEY.NS", "AUTO", "mid", True),
    ("ASIANPAINT", "ASIANPAINT.NS", "CONSUMER", "large", True),
    ("ASTRAL", "ASTRAL.NS", "CONSUMER", "mid", True),
    ("AUBANK", "AUBANK.NS", "BANK", "mid", True),
    ("AUROPHARMA", "AUROPHARMA.NS", "PHARMA", "mid", True),
    ("AXISBANK", "AXISBANK.NS", "BANK", "large", True),
    # ── B ──────────────────────────────────────────────────────
    ("BAJAJ-AUTO", "BAJAJ-AUTO.NS", "AUTO", "large", True),
    ("BAJAJFINSV", "BAJAJFINSV.NS", "FINANCE", "large", True),
    ("BAJAJHLDNG", "BAJAJHLDNG.NS", "CONSUMER", "large", True),
    ("BAJFINANCE", "BAJFINANCE.NS", "FINANCE", "large", True),
    ("BANDHANBNK", "BANDHANBNK.NS", "BANK", "mid", True),
    ("BANKBARODA", "BANKBARODA.NS", "BANK", "large", True),
    ("BANKINDIA", "BANKINDIA.NS", "BANK", "large", True),
    ("BDL", "BDL.NS", "INFRA", "mid", True),
    ("BEL", "BEL.NS", "INFRA", "large", True),
    ("BERGEPAINT", "BERGEPAINT.NS", "CONSUMER", "large", True),
    ("BHARATFORG", "BHARATFORG.NS", "AUTO", "large", True),
    ("BHARTIARTL", "BHARTIARTL.NS", "TELECOM", "large", True),
    ("BIOCON", "BIOCON.NS", "PHARMA", "mid", True),
    ("BLUESTARCO", "BLUESTARCO.NS", "CONSUMER", "mid", True),
    ("BOSCHLTD", "BOSCHLTD.NS", "AUTO", "large", True),
    ("BPCL", "BPCL.NS", "ENERGY", "large", True),
    ("BRITANNIA", "BRITANNIA.NS", "FMCG", "large", True),
    ("BSE", "BSE.NS", "FINANCE", "mid", True),
    # ── C ──────────────────────────────────────────────────────
    ("CAMS", "CAMS.NS", "FINANCE", "mid", True),
    ("CANBK", "CANBK.NS", "BANK", "large", True),
    ("CDSL", "CDSL.NS", "FINANCE", "mid", True),
    ("CGPOWER", "CGPOWER.NS", "INFRA", "mid", True),
    ("CHOLAFIN", "CHOLAFIN.NS", "FINANCE", "large", True),
    ("CIPLA", "CIPLA.NS", "PHARMA", "large", True),
    ("COFORGE", "COFORGE.NS", "IT", "mid", True),
    ("COLPAL", "COLPAL.NS", "FMCG", "large", True),
    ("CONCOR", "CONCOR.NS", "LOGISTICS", "large", True),
    ("COROMANDEL", "COROMANDEL.NS", "AGRI", "mid", True),
    # ── LOGISTICS ───────────────────────────────────────────
    ("CROMPTON", "CROMPTON.NS", "CONSUMER", "mid", True),
    # ── D ──────────────────────────────────────────────────────
    ("DABUR", "DABUR.NS", "FMCG", "large", True),
    ("DALBHARAT", "DALBHARAT.NS", "INFRA", "large", True),
    ("DELHIVERY", "DELHIVERY.NS", "LOGISTICS", "large", True),
    ("DIVISLAB", "DIVISLAB.NS", "PHARMA", "large", True),
    ("DIXON", "DIXON.NS", "CONSUMER", "mid", True),
    ("DLF", "DLF.NS", "INFRA", "large", True),
    ("DMART", "DMART.NS", "CONSUMER", "large", True),
    ("DRREDDY", "DRREDDY.NS", "PHARMA", "large", True),
    # ── E ──────────────────────────────────────────────────────
    ("EICHERMOT", "EICHERMOT.NS", "AUTO", "large", True),
    ("ETERNAL", "ETERNAL.NS", "FMCG", "large", True),
    ("EXIDEIND", "EXIDEIND.NS", "AUTO", "mid", True),
    # ── F ──────────────────────────────────────────────────────
    ("FEDERALBNK", "FEDERALBNK.NS", "BANK", "mid", True),
    ("FORTIS", "FORTIS.NS", "PHARMA", "mid", True),
    # ── G ──────────────────────────────────────────────────────
    ("GAIL", "GAIL.NS", "ENERGY", "large", True),
    ("GLENMARK", "GLENMARK.NS", "PHARMA", "mid", True),
    ("GODREJCP", "GODREJCP.NS", "FMCG", "large", True),
    ("GODREJPROP", "GODREJPROP.NS", "INFRA", "large", True),
    ("GRASIM", "GRASIM.NS", "INFRA", "large", True),
    # ── H ──────────────────────────────────────────────────────
    ("HAL", "HAL.NS", "INFRA", "large", True),
    ("HAVELLS", "HAVELLS.NS", "CONSUMER", "large", True),
    ("HCLTECH", "HCLTECH.NS", "IT", "large", True),
    ("HDFCAMC", "HDFCAMC.NS", "FINANCE", "large", True),
    ("HDFCBANK", "HDFCBANK.NS", "BANK", "large", True),
    ("HDFCLIFE", "HDFCLIFE.NS", "FINANCE", "large", True),
    ("HEROMOTOCO", "HEROMOTOCO.NS", "AUTO", "large", True),
    ("HINDALCO", "HINDALCO.NS", "METAL", "large", True),
    ("HINDPETRO", "HINDPETRO.NS", "ENERGY", "large", True),
    ("HINDUNILVR", "HINDUNILVR.NS", "FMCG", "large", True),
    ("HINDZINC", "HINDZINC.NS", "METAL", "large", True),
    ("HUDCO", "HUDCO.NS", "ENERGY", "mid", True),
    # ── I ──────────────────────────────────────────────────────
    ("ICICIBANK", "ICICIBANK.NS", "BANK", "large", True),
    ("ICICIGI", "ICICIGI.NS", "FINANCE", "large", True),
    ("ICICIPRULI", "ICICIPRULI.NS", "FINANCE", "large", True),
    ("IDEA", "IDEA.NS", "TELECOM", "mid", True),
    ("IDFCFIRSTB", "IDFCFIRSTB.NS", "BANK", "mid", True),
    ("INDHOTEL", "INDHOTEL.NS", "CONSUMER", "large", True),
    ("INDIANB", "INDIANB.NS", "BANK", "mid", True),
    ("INDIGO", "INDIGO.NS", "LOGISTICS", "large", True),
    ("INDUSINDBK", "INDUSINDBK.NS", "BANK", "large", True),
    ("INDUSTOWER", "INDUSTOWER.NS", "TELECOM", "large", True),
    ("INFY", "INFY.NS", "IT", "large", True),
    ("INOXWIND", "INOXWIND.NS", "ENERGY", "mid", True),
    ("IOC", "IOC.NS", "ENERGY", "large", True),
    ("IREDA", "IREDA.NS", "ENERGY", "mid", True),
    ("IRFC", "IRFC.NS", "FINANCE", "large", True),
    ("ITC", "ITC.NS", "FMCG", "large", True),
    # ── J ──────────────────────────────────────────────────────
    ("JINDALSTEL", "JINDALSTEL.NS", "METAL", "mid", True),
    ("JIOFIN", "JIOFIN.NS", "FINANCE", "large", True),
    ("JSWENERGY", "JSWENERGY.NS", "ENERGY", "mid", True),
    ("JSWSTEEL", "JSWSTEEL.NS", "METAL", "large", True),
    # ── K ──────────────────────────────────────────────────────
    ("KALYANKJIL", "KALYANKJIL.NS", "CONSUMER", "large", True),
    ("KAYNES", "KAYNES.NS", "IT", "mid", True),
    ("KFINTECH", "KFINTECH.NS", "IT", "mid", True),
    ("KOTAKBANK", "KOTAKBANK.NS", "BANK", "large", True),
    ("KPITTECH", "KPITTECH.NS", "IT", "mid", True),
    # ── L ──────────────────────────────────────────────────────
    ("LAURUSLABS", "LAURUSLABS.NS", "PHARMA", "mid", True),
    ("LICHSGFIN", "LICHSGFIN.NS", "FINANCE", "mid", True),
    ("LICI", "LICI.NS", "FINANCE", "large", True),
    ("LODHA", "LODHA.NS", "INFRA", "large", True),
    ("LT", "LT.NS", "INFRA", "large", True),
    ("LTF", "LTF.NS", "FINANCE", "mid", True),
    ("LTIM", "LTIM.NS", "IT", "large", True),
    ("LUPIN", "LUPIN.NS", "PHARMA", "large", True),
    # ── M ──────────────────────────────────────────────────────
    ("M&M", "M&M.NS", "AUTO", "large", True),
    ("MANAPPURAM", "MANAPPURAM.NS", "FINANCE", "mid", True),
    ("MANKIND", "MANKIND.NS", "PHARMA", "large", True),
    ("MARICO", "MARICO.NS", "FMCG", "large", True),
    ("MARUTI", "MARUTI.NS", "AUTO", "large", True),
    ("MAXHEALTH", "MAXHEALTH.NS", "PHARMA", "mid", True),
    ("MCX", "MCX.NS", "FINANCE", "mid", True),
    ("MFSL", "MFSL.NS", "FINANCE", "mid", True),
    ("MOTHERSON", "MOTHERSON.NS", "AUTO", "large", True),
    ("MPHASIS", "MPHASIS.NS", "IT", "mid", True),
    ("MUTHOOTFIN", "MUTHOOTFIN.NS", "FINANCE", "large", True),
    # ── N ──────────────────────────────────────────────────────
    ("NATIONALUM", "NATIONALUM.NS", "METAL", "large", True),
    ("NAUKRI", "NAUKRI.NS", "CONSUMER", "large", True),
    ("NBCC", "NBCC.NS", "INFRA", "mid", True),
    ("NESTLEIND", "NESTLEIND.NS", "FMCG", "large", True),
    ("NHPC", "NHPC.NS", "ENERGY", "mid", True),
    ("NMDC", "NMDC.NS", "METAL", "large", True),
    ("NTPC", "NTPC.NS", "ENERGY", "large", True),
    ("NUVAMA", "NUVAMA.NS", "FINANCE", "mid", True),
    ("NYKAA", "NYKAA.NS", "CONSUMER", "large", True),
    # ── O ──────────────────────────────────────────────────────
    ("OBEROIRLTY", "OBEROIRLTY.NS", "INFRA", "large", True),
    ("OFSS", "OFSS.NS", "IT", "mid", True),
    ("OIL", "OIL.NS", "ENERGY", "large", True),
    ("ONGC", "ONGC.NS", "ENERGY", "large", True),
    # ── P ──────────────────────────────────────────────────────
    ("PAGEIND", "PAGEIND.NS", "CONSUMER", "large", True),
    ("PATANJALI", "PATANJALI.NS", "FMCG", "mid", True),
    ("PAYTM", "PAYTM.NS", "FINANCE", "mid", True),
    ("PERSISTENT", "PERSISTENT.NS", "IT", "mid", True),
    ("PETRONET", "PETRONET.NS", "ENERGY", "large", True),
    ("PFC", "PFC.NS", "FINANCE", "large", True),
    ("PGEL", "PGEL.NS", "INFRA", "mid", True),
    ("PHOENIXLTD", "PHOENIXLTD.NS", "CONSUMER", "large", True),
    ("PIDILITIND", "PIDILITIND.NS", "CONSUMER", "large", True),
    ("PIIND", "PIIND.NS", "CHEM", "large", True),
    ("PNB", "PNB.NS", "BANK", "large", True),
    ("PNBHOUSING", "PNBHOUSING.NS", "FINANCE", "mid", True),
    ("POLICYBZR", "POLICYBZR.NS", "FINANCE", "mid", True),
    ("POLYCAB", "POLYCAB.NS", "CONSUMER", "large", True),
    ("POWERGRID", "POWERGRID.NS", "ENERGY", "large", True),
    ("PREMIERENE", "PREMIERENE.NS", "ENERGY", "mid", True),
    ("PRESTIGE", "PRESTIGE.NS", "INFRA", "large", True),
    # ── R ──────────────────────────────────────────────────────
    ("RBLBANK", "RBLBANK.NS", "BANK", "mid", True),
    ("RECLTD", "RECLTD.NS", "FINANCE", "large", True),
    ("RELIANCE", "RELIANCE.NS", "ENERGY", "large", True),
    ("RVNL", "RVNL.NS", "INFRA", "mid", True),
    # ── S ──────────────────────────────────────────────────────
    ("SAIL", "SAIL.NS", "METAL", "large", True),
    ("SAMMAANCAP", "SAMMAANCAP.NS", "CONSUMER", "mid", True),
    ("SBICARD", "SBICARD.NS", "FINANCE", "large", True),
    ("SBILIFE", "SBILIFE.NS", "FINANCE", "large", True),
    ("SBIN", "SBIN.NS", "BANK", "large", True),
    ("SHREECEM", "SHREECEM.NS", "INFRA", "large", True),
    ("SIEMENS", "SIEMENS.NS", "INFRA", "large", True),
    ("SOLARINDS", "SOLARINDS.NS", "CONSUMER", "mid", True),
    ("SONACOMS", "SONACOMS.NS", "AUTO", "mid", True),
    ("SRF", "SRF.NS", "CHEM", "large", True),
    ("SUNPHARMA", "SUNPHARMA.NS", "PHARMA", "large", True),
    ("SUPREMEIND", "SUPREMEIND.NS", "CONSUMER", "mid", True),
    ("SUZLON", "SUZLON.NS", "ENERGY", "mid", True),
    ("SWIGGY", "SWIGGY.NS", "FMCG", "large", True),
    # ── T ──────────────────────────────────────────────────────
    ("TATACHEM", "TATACHEM.NS", "CHEM", "mid", True),
    ("TATACONSUM", "TATACONSUM.NS", "FMCG", "large", True),
    ("TATAELXSI", "TATAELXSI.NS", "IT", "mid", True),
    ("TATAPOWER", "TATAPOWER.NS", "ENERGY", "mid", True),
    ("TATASTEEL", "TATASTEEL.NS", "METAL", "large", True),
    ("TATATECH", "TATATECH.NS", "IT", "mid", True),
    ("TCS", "TCS.NS", "IT", "large", True),
    ("TECHM", "TECHM.NS", "IT", "large", True),
    ("TIINDIA", "TIINDIA.NS", "AUTO", "mid", True),
    ("TITAN", "TITAN.NS", "CONSUMER", "large", True),
    ("TORNTPHARM", "TORNTPHARM.NS", "PHARMA", "mid", True),
    ("TORNTPOWER", "TORNTPOWER.NS", "ENERGY", "mid", True),
    ("TRENT", "TRENT.NS", "CONSUMER", "large", True),
    ("TVSMOTOR", "TVSMOTOR.NS", "AUTO", "large", True),
    # ── U ──────────────────────────────────────────────────────
    ("ULTRACEMCO", "ULTRACEMCO.NS", "INFRA", "large", True),
    ("UNIONBANK", "UNIONBANK.NS", "BANK", "mid", True),
    ("UNITDSPR", "UNITDSPR.NS", "FMCG", "mid", True),
    ("UNOMINDA", "UNOMINDA.NS", "AUTO", "mid", True),
    ("UPL", "UPL.NS", "AGRI", "large", True),
    # ── V ──────────────────────────────────────────────────────
    ("VBL", "VBL.NS", "FMCG", "large", True),
    ("VEDL", "VEDL.NS", "METAL", "large", True),
    ("VOLTAS", "VOLTAS.NS", "CONSUMER", "large", True),
    # ── W ──────────────────────────────────────────────────────
    ("WAAREEENER", "WAAREEENER.NS", "ENERGY", "mid", True),
    ("WIPRO", "WIPRO.NS", "IT", "large", True),
    # ── Y ──────────────────────────────────────────────────────
    ("YESBANK", "YESBANK.NS", "BANK", "mid", True),
    # ── Z ──────────────────────────────────────────────────────
    ("ZYDUSLIFE", "ZYDUSLIFE.NS", "PHARMA", "large", True),
]

# ── Timeframe config — must match WAVE's TIMEFRAMES + HTF_MAP ─────────────────
# WAVE uses: ticker|period|interval as the cache key → filename
#
# htf_chain: all HTF levels to write for this scan timeframe.
# --tf 1d writes 1wk (its own HTF) AND 1mo (HTF of the weekly scan) so that
# running just "--tf 1d --merge" is sufficient for both Daily AND Weekly scan cache.
TF_CONFIG = {
    "1d":  {"interval": "1d",  "period": "1y",  "max_age_h": 12,
            "htf_chain": [
                ("1wk", "5y"),   # 1D scan HTF  (HTF_MAP['1d']  = 1wk/5y)
                ("1mo", "10y"),  # weekly scan HTF (HTF_MAP['1wk'] = 1mo/10y) — KEY FIX
            ]},
    "1wk": {"interval": "1wk", "period": "5y",  "max_age_h": 48,
            "htf_chain": [
                ("1mo", "10y"),  # 1W scan HTF  (HTF_MAP['1wk'] = 1mo/10y)
            ]},
}
# Convenience aliases kept for sector-index section that still references them directly
def _primary_htf(tf_key):
    """Returns (interval, period) of the first HTF in the chain (for sector index fetch)."""
    return TF_CONFIG[tf_key]["htf_chain"][0]

# ── wave_hist config — mirrors WAVE's backfill constants exactly ───────────────
# WAVE constants:  tf = {period:'2y', interval:'1d'},  MAX_BARS = 375,  HIST_TTL = 7d
HIST_CONFIG = {
    "interval":  "1d",
    "period":    "2y",
    "max_bars":  375,        # WAVE keeps last 375 trading days (~18 months)
    "max_age_h": 168,        # 7 days — matches WAVE's HIST_TTL
}

# ── Sector HTF intervals — mirrors WAVE's Step 1b fetch exactly ───────────────
# WAVE fetches 2 intervals per sector for 1D/1W scan HTF support
HIST_HTF_INTERVALS = [
    {"interval": "1wk", "period": "5y",  "max_bars": 500},  # for 1D scan HTF
    {"interval": "1mo", "period": "10y", "max_bars": 500},  # for 1W scan HTF
]

# Sector indices — mirrors SECTOR_INDEX_MAP in WAVE.html
# Key format matches WAVE's sectorHistKey():  SECTOR_{SECTOR}_{interval}
SECTOR_INDEX_MAP = {
    "BANK":     "^NSEBANK",
    "IT":       "^CNXIT",
    "PHARMA":   "^CNXPHARMA",
    "ENERGY":   "^CNXENERGY",
    "AUTO":     "^CNXAUTO",
    "METAL":    "^CNXMETAL",
    "FMCG":     "^CNXFMCG",
    "FINANCE":  "NIFTY_FIN_SERVICE.NS",
    "INFRA":    "^CNXINFRA",
    "CONSUMER": "^CNXCONSUM",
    "TELECOM":  "BHARTIARTL.NS",   # ^CNXTELECOM delisted on Yahoo — Bharti Airtel proxy
    "CHEM":     "PIDILITIND.NS",   # ^CNXCHEMICAL unavailable — Pidilite proxy
    "AGRI":     "UPL.NS",          # ^CNXAGROINDSTRS unreliable — UPL proxy
    # LOGISTICS and INDEX both map to ^NSEI — fetched via NIFTY stock entry in Step 1a
}

# Sector indices fetched as HTF for regime detection — mirrors SECTOR_INDEX_MAP in WAVE
SECTOR_INDICES = [
    ("SECTOR_BANK",     "^NSEBANK"),
    ("SECTOR_IT",       "^CNXIT"),
    ("SECTOR_PHARMA",   "^CNXPHARMA"),
    ("SECTOR_ENERGY",   "^CNXENERGY"),
    ("SECTOR_AUTO",     "^CNXAUTO"),
    ("SECTOR_METAL",    "^CNXMETAL"),
    ("SECTOR_FMCG",     "^CNXFMCG"),
    ("SECTOR_FINANCE",  "NIFTY_FIN_SERVICE.NS"),
    ("SECTOR_INFRA",    "^CNXINFRA"),
    ("SECTOR_CONSUMER", "^CNXCONSUM"),
    ("SECTOR_TELECOM",  "BHARTIARTL.NS"),
    ("SECTOR_CHEM",     "PIDILITIND.NS"),
    ("SECTOR_AGRI",     "UPL.NS"),
    # SECTOR_LOGISTICS and SECTOR_INDEX excluded — both use ^NSEI (same as NIFTY)
]


# ═══════════════════════════════════════════════════════════════════════════════
# Cache key → filename  (matches WAVE's `key = ticker + '|' + period + '|' + interval`)
# The key contains special chars, WAVE sanitises as: key.replace(/[^a-zA-Z0-9_\-\.]/g,'_')
# ═══════════════════════════════════════════════════════════════════════════════
def cache_key(ticker: str, period: str, interval: str) -> str:
    """Returns the raw cache key exactly as WAVE builds it."""
    return f"{ticker}|{period}|{interval}"

def safe_filename(key: str) -> str:
    """Sanitise key to filename — mirrors WAVE's regex: /[^a-zA-Z0-9_\\-\\.]/g → '_'"""
    return re.sub(r'[^a-zA-Z0-9_\-\.]', '_', key) + '.json'


# ═══════════════════════════════════════════════════════════════════════════════
# Packed candle format — mirrors WAVE's packCandles / unpackCandles exactly
# WAVE: packCandles = cs => cs.map(c => [c.date.slice(0,10), +c.open.toFixed(2), ...])
# ═══════════════════════════════════════════════════════════════════════════════
def pack_candles(records: list) -> list:
    """Convert full OHLCV dicts to WAVE's compact array format."""
    return [
        [r["date"][:10], round(float(r["open"]), 2), round(float(r["high"]), 2),
         round(float(r["low"]), 2), round(float(r["close"]), 2), int(r["volume"])]
        for r in records
    ]


# ═══════════════════════════════════════════════════════════════════════════════
# Fetch helpers
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_ohlcv(ticker: str, period: str, interval: str, retries: int = 3):
    """
    Download OHLC data via yfinance.
    For .NS tickers that fail, automatically retries with .BO (BSE) suffix.
    Returns list of {date, open, high, low, close, volume} dicts (WAVE format)
    or None on failure.
    """
    candidates = [ticker]
    if ticker.endswith(".NS"):
        candidates.append(ticker.replace(".NS", ".BO"))

    for t in candidates:
        result = _try_fetch(t, period, interval, retries)
        if result:
            if t != ticker:
                print(f"(.BO fallback) ", end="", flush=True)
            return result
    return None


def _try_fetch(ticker: str, period: str, interval: str, retries: int = 3):
    for attempt in range(retries):
        try:
            df = yf.download(
                ticker,
                period=period,
                interval=interval,
                auto_adjust=True,
                progress=False,
                threads=False,
            )
            if df is None or df.empty:
                return None

            df = df.reset_index()
            # Flatten MultiIndex columns if present (yfinance >= 0.2.x)
            df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

            time_col = "Datetime" if "Datetime" in df.columns else "Date"
            records = []
            for _, row in df.iterrows():
                t = row[time_col]
                if hasattr(t, 'isoformat'):
                    if hasattr(t, 'tzinfo') and t.tzinfo is not None:
                        date_str = t.isoformat()
                    else:
                        date_str = t.isoformat() + 'Z'
                else:
                    date_str = str(t)[:19] + 'Z'

                o = float(row.get("Open", 0) or 0)
                h = float(row.get("High", 0) or 0)
                l = float(row.get("Low",  0) or 0)
                c = float(row.get("Close",0) or 0)
                v = int(row.get("Volume",0) or 0)
                if o and h and l and c:
                    records.append({"date": date_str, "open": o, "high": h,
                                    "low": l, "close": c, "volume": v})
            return records if records else None

        except Exception:
            if attempt < retries - 1:
                time.sleep(1.5 * (attempt + 1))
            else:
                return None
    return None


def load_cache(path: Path):
    """Load existing cache file. Returns dict or None."""
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def save_cache(path: Path, ticker: str, period: str, interval: str, data: list):
    """Write yf_cache file in WAVE's exact format: {ts, data}."""
    payload = {
        "ts":   int(datetime.now(timezone.utc).timestamp() * 1000),
        "data": data,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(payload, f, separators=(',', ':'))


def save_hist(path: Path, data: list, max_bars: int):
    """Write wave_hist file in WAVE's packed candle format: {ts, c:[...]}."""
    trimmed = data[-max_bars:]
    payload = {
        "ts": int(datetime.now(timezone.utc).timestamp() * 1000),
        "c":  pack_candles(trimmed),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(payload, f, separators=(',', ':'))


def cache_age_hours(path: Path) -> float:
    """Return age of cache file in hours. 9999 if not found."""
    if not path.exists():
        return 9999.0
    age_s = datetime.now(timezone.utc).timestamp() - path.stat().st_mtime
    return age_s / 3600


def is_stale(path: Path, max_age_h: float) -> bool:
    return cache_age_hours(path) > max_age_h


# ═══════════════════════════════════════════════════════════════════════════════
# --hist mode: write wave_hist/ for Backfill Step 1
# ═══════════════════════════════════════════════════════════════════════════════
def run_hist_mode(hist_dir: Path, stocks: list, args):
    """
    Fetch 2Y daily history for all F&O + index stocks and write
    App_Wave/data/wave_hist/SYMBOL.json in WAVE's packed candle format.

    Also fetches sector index HTF candles (1d/1wk/1mo) and writes
    SECTOR_{SECTOR}_{interval}.json — matching WAVE's Step 1b fetch.

    With --merge, skips any file still fresh within HIST_TTL (7 days).
    """
    hist_dir.mkdir(parents=True, exist_ok=True)
    cfg = HIST_CONFIG
    mode = "MERGE (skip fresh)" if args.merge else "FULL FETCH"

    # ── Backfill universe: F&O + indices (mirrors WAVE's fetchHistory filter) ──
    universe = [s for s in stocks if s[4] or s[3] == "index"]
    total = len(universe)

    print(f"\n{'═'*66}")
    print(f"  WAVE Hist Fetcher  (wave_hist/ — Backfill Step 1)")
    print(f"  Mode      : {mode}")
    print(f"  Period    : {cfg['period']}  interval: {cfg['interval']}")
    print(f"  Max bars  : {cfg['max_bars']}  TTL: {cfg['max_age_h']}h (7 days)")
    print(f"  Symbols   : {total}  (F&O + indices)")
    print(f"  Output    : {hist_dir.resolve()}")
    print(f"{'═'*66}\n")
    print(f"── Step 1a: Stock history ──\n")

    ok = skipped = failed = 0
    t0 = time.time()

    for i, (symbol, yahoo, sector, cap, fno) in enumerate(universe, 1):
        path = hist_dir / f"{symbol}.json"
        label = f"  [{i:>3}/{total}] {symbol:<14}"

        if args.merge and not is_stale(path, cfg["max_age_h"]):
            age = cache_age_hours(path)
            print(f"{label} ~ cached  ({age:.0f}h old)")
            skipped += 1
            continue

        print(f"{label}", end="  ", flush=True)
        data = fetch_ohlcv(yahoo, cfg["period"], cfg["interval"])
        if data and len(data) >= 60:
            save_hist(path, data, cfg["max_bars"])
            print(f"✓  {min(len(data), cfg['max_bars']):4d} bars")
            ok += 1
        else:
            print("✗  no data")
            failed += 1

        if args.delay > 0 and i < total:
            time.sleep(args.delay)

    elapsed = time.time() - t0
    print(f"\n  ✓ {ok} fetched  ~ {skipped} cached  ✗ {failed} failed  ({elapsed:.0f}s)\n")

    # ── Step 1b: Sector HTF candles ───────────────────────────────────────────
    # Mirrors WAVE's Step 1b: fetches 3 intervals per sector (1d/1wk/1mo)
    # File names match WAVE's sectorHistKey(): SECTOR_{SECTOR}_{interval}
    print(f"── Step 1b: Sector HTF candles  ({len(SECTOR_INDEX_MAP)} sectors × 2 intervals) ──\n")

    sec_ok = sec_skip = sec_fail = 0
    sec_total = len(SECTOR_INDEX_MAP) * len(HIST_HTF_INTERVALS)
    sec_i = 0
    t1 = time.time()

    for sector_name, yahoo_ticker in SECTOR_INDEX_MAP.items():
        for htf in HIST_HTF_INTERVALS:
            sec_i += 1
            # Key mirrors WAVE's sectorHistKey(sector, interval):
            # "SECTOR_BANK_1d", "SECTOR_IT_1wk", etc.
            hist_key = f"SECTOR_{sector_name}_{htf['interval']}"
            path = hist_dir / f"{hist_key}.json"
            label = f"  [{sec_i:>3}/{sec_total}] {hist_key:<28}"

            if args.merge and not is_stale(path, cfg["max_age_h"]):
                age = cache_age_hours(path)
                print(f"{label} ~ cached  ({age:.0f}h old)")
                sec_skip += 1
                continue

            print(f"{label}", end="  ", flush=True)
            data = fetch_ohlcv(yahoo_ticker, htf["period"], htf["interval"])
            if data and len(data) >= 12:
                save_hist(path, data, htf["max_bars"])
                print(f"✓  {min(len(data), htf['max_bars']):4d} bars")
                sec_ok += 1
            else:
                print("✗  no data")
                sec_fail += 1

            if args.delay > 0:
                time.sleep(args.delay)

    elapsed2 = time.time() - t1
    print(f"\n  ✓ {sec_ok} fetched  ~ {sec_skip} cached  ✗ {sec_fail} failed  ({elapsed2:.0f}s)\n")

    # ── Footer ────────────────────────────────────────────────────────────────
    print(f"{'═'*66}")
    print(f"  wave_hist/ written to:  {hist_dir.resolve()}")
    print()
    print(f"  CONNECT TO WAVE:")
    print(f"    1. Open WAVE.html in Chrome")
    print(f"    2. Click 📁 folder icon (top-right) → select App_Wave folder")
    print(f"    3. Click  Backfill  →  WAVE reads wave_hist/ from disk,")
    print(f"       Step 1 completes with ZERO Yahoo Finance network calls.")
    print()
    print(f"  INCREMENTAL UPDATE (run after market close):")
    print(f"    python wave_fetch.py --hist --merge")
    print(f"{'═'*66}\n")


# ═══════════════════════════════════════════════════════════════════════════════
# Summary
# ═══════════════════════════════════════════════════════════════════════════════
def print_summary(outdir: Path, packed: bool = False):
    files = sorted(outdir.glob("*.json"))
    if not files:
        print("  No cache files found.")
        return
    total_mb = sum(f.stat().st_size for f in files) / 1_048_576
    field = "BARS" if packed else "CANDLES"
    print(f"\n  {'FILE':<50} {field:>8}  {'AGE':>8}  {'SIZE':>7}")
    print(f"  {'─'*50} {'─'*8}  {'─'*8}  {'─'*7}")
    for f in sorted(files):
        try:
            d = json.loads(f.read_text())
            # wave_hist uses 'c', yf_cache uses 'data'
            nc = len(d.get("c", d.get("data", [])))
            age_h = cache_age_hours(f)
            age_s = f"{age_h:.1f}h" if age_h < 48 else f"{age_h/24:.1f}d"
            sz = f"{f.stat().st_size/1024:.0f}KB"
            print(f"  {f.name:<50} {nc:>8}  {age_s:>8}  {sz:>7}")
        except Exception:
            print(f"  {f.name:<50}  (unreadable)")
    print(f"\n  Total: {len(files)} files  |  {total_mb:.1f} MB")


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(
        description="WAVE Data Fetcher — pre-populates App_Wave/data/",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--tf", default="1d", choices=["1d", "1wk", "all"],
                        help="Timeframe to fetch for yf_cache/: 1d, 1wk, or all (default: 1d)")
    parser.add_argument("--hist", action="store_true",
                        help="Fetch 2Y daily history into wave_hist/ — feeds Backfill Step 1 "
                             "so the browser makes ZERO Yahoo Finance calls during backfill")
    parser.add_argument("--merge", action="store_true",
                        help="Skip files that are still fresh (within TTL)")
    parser.add_argument("--symbols", nargs="+", default=None,
                        help="Specific symbols only (e.g. RELIANCE TCS HDFCBANK)")
    parser.add_argument("--sector", default=None,
                        help="Fetch only one sector for yf_cache/ (e.g. BANK, IT, PHARMA)")
    parser.add_argument("--fno-only", action="store_true",
                        help="Fetch only F&O-eligible stocks + indices")
    parser.add_argument("--top", type=int, default=None,
                        help="Quick test: only first N symbols")
    parser.add_argument("--outdir", default=None,
                        help="Base output dir for --hist (default: ./App_Wave/data) "
                             "or yf_cache dir for --tf (default: ./App_Wave/data/yf_cache)")
    parser.add_argument("--delay", type=float, default=0.35,
                        help="Seconds between requests (default 0.35)")
    parser.add_argument("--no-htf", action="store_true",
                        help="Skip Higher TimeFrame fetch for yf_cache/ (saves ~50%% of requests)")
    parser.add_argument("--summary", action="store_true",
                        help="Show cache summary and exit")
    args = parser.parse_args()

    # ── Build stock list ──────────────────────────────────────────────────────
    stocks = ALL_STOCKS
    if args.symbols:
        sym_set = set(args.symbols)
        stocks = [s for s in ALL_STOCKS if s[0] in sym_set]
        if not stocks:
            print(f"  No matching symbols found for: {args.symbols}")
            sys.exit(1)
    elif args.sector and not args.hist:
        stocks = [s for s in ALL_STOCKS if s[2] == args.sector.upper()]
        if not stocks:
            print(f"  No stocks found for sector: {args.sector}")
            sys.exit(1)
    elif args.fno_only:
        stocks = [s for s in ALL_STOCKS if s[4] or s[3] == "index"]

    if args.top:
        stocks = stocks[:args.top]

    # ══════════════════════════════════════════════════════════════════════════
    # --hist mode: write wave_hist/ (Backfill Step 1 pre-load)
    # ══════════════════════════════════════════════════════════════════════════
    if args.hist:
        base_dir = Path(args.outdir) if args.outdir else Path("./App_Wave/data")
        hist_dir = base_dir / "wave_hist"

        if args.summary:
            print_summary(hist_dir, packed=True)
            return

        run_hist_mode(hist_dir, stocks, args)
        return

    # ══════════════════════════════════════════════════════════════════════════
    # --tf mode: write yf_cache/ (scan cache)
    # ══════════════════════════════════════════════════════════════════════════
    outdir = Path(args.outdir) if args.outdir else Path("./App_Wave/data/yf_cache")
    outdir.mkdir(parents=True, exist_ok=True)

    if args.summary:
        print_summary(outdir, packed=False)
        return

    tfs = ["1d", "1wk"] if args.tf == "all" else [args.tf]

    mode = "MERGE (skip fresh)" if args.merge else "FULL FETCH"
    print(f"\n{'═'*66}")
    print(f"  WAVE Data Fetcher  (yf_cache/)")
    print(f"  Mode      : {mode}")
    print(f"  Timeframes: {', '.join(tfs)}")
    print(f"  Symbols   : {len(stocks)}")
    print(f"  Output    : {outdir.resolve()}")
    print(f"{'═'*66}\n")

    for tf in tfs:
        cfg = TF_CONFIG[tf]
        interval  = cfg["interval"]
        period    = cfg["period"]
        htf_chain = cfg["htf_chain"]
        max_age_h = cfg["max_age_h"]

        htf_labels = " + ".join(f"{i}({p})" for i, p in htf_chain)
        print(f"── {tf.upper()}  ({period})  +HTF {htf_labels} ──\n")

        ok = skipped = failed = 0
        t0 = time.time()

        for i, (symbol, yahoo, sector, cap, fno) in enumerate(stocks, 1):
            label = f"  [{i:>3}/{len(stocks)}] {symbol:<14}"

            key  = cache_key(yahoo, period, interval)
            path = outdir / safe_filename(key)

            if args.merge and not is_stale(path, max_age_h):
                print(f"{label} ~ cached  ({cache_age_hours(path):.1f}h old)")
                skipped += 1
            else:
                print(f"{label}", end="  ", flush=True)
                data = fetch_ohlcv(yahoo, period, interval)
                if data:
                    save_cache(path, yahoo, period, interval, data)
                    htf_parts = []
                    if not args.no_htf:
                        for htf_interval, htf_period in htf_chain:
                            htf_key  = cache_key(yahoo, htf_period, htf_interval)
                            htf_path = outdir / safe_filename(htf_key)
                            if not (args.merge and not is_stale(htf_path, max_age_h * 2)):
                                htf_data = fetch_ohlcv(yahoo, htf_period, htf_interval)
                                if htf_data:
                                    save_cache(htf_path, yahoo, htf_period, htf_interval, htf_data)
                                    htf_parts.append(f"{htf_interval}:{len(htf_data)}")
                                time.sleep(args.delay)
                    htf_str = "  HTF " + " ".join(htf_parts) if htf_parts else ""
                    print(f"✓  {len(data):4d} candles{htf_str}")
                    ok += 1
                else:
                    print("✗  no data")
                    failed += 1

            if args.delay > 0 and i < len(stocks):
                time.sleep(args.delay)

        # ── Sector indices ────────────────────────────────────────────────────
        if not args.no_htf and not args.symbols:
            htf_label = " + ".join(i for i, _ in htf_chain)
            print(f"\n  Sector indices ({interval} scan + HTF {htf_label}):")
            for sec_name, sec_ticker in SECTOR_INDICES:
                scan_key  = cache_key(sec_ticker, period, interval)
                scan_path = outdir / safe_filename(scan_key)

                # Check if all HTF levels are already cached
                all_htf_cached = args.merge and all(
                    not is_stale(outdir / safe_filename(cache_key(sec_ticker, hp, hi)), max_age_h * 4)
                    for hi, hp in htf_chain
                )
                scan_cached = args.merge and not is_stale(scan_path, max_age_h * 4)

                if scan_cached and all_htf_cached:
                    print(f"    ~ {sec_name:<22} all cached")
                    continue

                print(f"    {sec_name:<22}", end="  ", flush=True)
                parts = []
                if not scan_cached:
                    data = fetch_ohlcv(sec_ticker, period, interval)
                    if data:
                        save_cache(scan_path, sec_ticker, period, interval, data)
                        parts.append(f"scan:{len(data)}")
                    else:
                        parts.append("scan:✗")
                    time.sleep(args.delay)

                for htf_interval, htf_period in htf_chain:
                    htf_key  = cache_key(sec_ticker, htf_period, htf_interval)
                    htf_path = outdir / safe_filename(htf_key)
                    htf_cached = args.merge and not is_stale(htf_path, max_age_h * 4)
                    if not htf_cached:
                        htf_data = fetch_ohlcv(sec_ticker, htf_period, htf_interval)
                        if htf_data:
                            save_cache(htf_path, sec_ticker, htf_period, htf_interval, htf_data)
                            parts.append(f"{htf_interval}:{len(htf_data)}")
                        else:
                            parts.append(f"{htf_interval}:✗")
                        time.sleep(args.delay)

                print("  ".join(parts))

        elapsed = time.time() - t0
        print(f"\n  ✓ {ok} fetched  ~ {skipped} cached  ✗ {failed} failed  ({elapsed:.0f}s)\n")

    # ── Footer ────────────────────────────────────────────────────────────────
    print(f"{'═'*66}")
    print(f"  Cache written to:  {outdir.resolve()}")
    print()
    print(f"  CONNECT TO WAVE:")
    print(f"    1. Open WAVE.html in Chrome")
    print(f"    2. Click the 📁 folder icon (top-right toolbar)")
    print(f"    3. Select your  App_Wave/  folder")
    print(f"    4. Run any scan (Daily / Weekly) — all load from disk")
    print()
    print(f"  NOTE: --tf 1d writes 2 levels: 1d + 1wk + 1mo HTF")
    print(f"  This means --tf 1d alone covers Daily AND Weekly scan cache.")
    print()
    print(f"  ALSO RUN --hist for zero-network Backfill:")
    print(f"    python wave_fetch.py --hist")
    print()
    print(f"  INCREMENTAL UPDATE (run after market close):")
    print(f"    python wave_fetch.py --tf 1d --merge   # covers daily + weekly")
    print(f"    python wave_fetch.py --hist --merge    # refreshes backfill")
    print(f"{'═'*66}\n")


if __name__ == "__main__":
    main()
