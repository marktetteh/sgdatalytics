#!/usr/bin/env python3
"""
SG Datalytics — Weekly Insight Generator
Scans all datasets, surfaces the most newsworthy pattern,
generates a branded 1080×1080 PNG flyer + LinkedIn caption.
"""

import os, sys, json, warnings, textwrap
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib import font_manager
warnings.filterwarnings('ignore')

# ── Paths ─────────────────────────────────────────────────────
BASE      = '/sessions/friendly-trusting-hopper/mnt/sgdatalytics'
DATASETS  = os.path.join(BASE, 'datasets/clean')
FLYERS    = os.path.join(BASE, 'flyers')
os.makedirs(FLYERS, exist_ok=True)

# ── Brand colours ─────────────────────────────────────────────
NAVY   = '#1a2535'
TEAL   = '#00957a'
AMBER  = '#e8960a'
WHITE  = '#ffffff'
GREY1  = '#c8d4e0'
GREY2  = '#7a8fa8'
PANEL  = '#243044'   # slightly lighter than navy for panels

# ── Font setup ───────────────────────────────────────────────
FONT_PATH = '/usr/share/fonts/truetype/lato'
for style, fname in [
    ('regular', 'Lato-Regular.ttf'),
    ('bold',    'Lato-Bold.ttf'),
    ('black',   'Lato-Black.ttf'),
    ('light',   'Lato-Light.ttf'),
]:
    fp = os.path.join(FONT_PATH, fname)
    if os.path.exists(fp):
        font_manager.fontManager.addfont(fp)

plt.rcParams['font.family'] = 'Lato'

# ══════════════════════════════════════════════════════════════
#  DATA LOADERS
# ══════════════════════════════════════════════════════════════

def load_bog_inflation():
    path = os.path.join(DATASETS, 'bog/cpi_inflation_2026-04.csv')
    df = pd.read_csv(path)
    df['headline_inflation'] = pd.to_numeric(df['headline_inflation'], errors='coerce')
    df['core_inflation_excl_energy'] = pd.to_numeric(df['core_inflation_excl_energy'], errors='coerce')
    df = df.dropna(subset=['headline_inflation']).reset_index(drop=True)
    return df

def load_bog_fx():
    path = os.path.join(DATASETS, 'bog/fx_rates_2026-04.csv')
    df = pd.read_csv(path)
    df['usd_ghs_period_average'] = pd.to_numeric(df['usd_ghs_period_average'], errors='coerce')
    df['usd_ghs_end_period'] = pd.to_numeric(df['usd_ghs_end_period'], errors='coerce')
    df = df.dropna(subset=['usd_ghs_period_average']).reset_index(drop=True)
    return df

def load_sgmpi(category_file):
    path = os.path.join(DATASETS, f'sgmpi/{category_file}')
    df = pd.read_csv(path)
    df['price_ghs'] = pd.to_numeric(df['price_ghs'], errors='coerce')
    df = df[df['price_outlier_flag'] != True]
    df = df.dropna(subset=['price_ghs'])
    return df

def load_bog_key():
    path = os.path.join(DATASETS, 'bog/key_indicators_2026-04.csv')
    df = pd.read_csv(path)
    for col in ['headline_inflation','gross_international_reserves_usd_m','private_sector_credit_growth']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


# ══════════════════════════════════════════════════════════════
#  INSIGHT CANDIDATES
# ══════════════════════════════════════════════════════════════

def insight_inflation():
    """Inflation trend — how fast is it falling?"""
    df = load_bog_inflation()
    recent = df.head(6)
    latest = recent.iloc[0]
    prev   = recent.iloc[1]
    change = latest['headline_inflation'] - prev['headline_inflation']

    # Score: bigger change = more newsworthy; falling inflation is extra interesting
    score = abs(change) * 2 + abs(latest['headline_inflation'] - recent.iloc[-1]['headline_inflation'])

    # Build label list for chart (reverse so oldest left)
    labels = recent['date'].tolist()[::-1]
    values = recent['headline_inflation'].tolist()[::-1]

    direction = "fell" if change < 0 else "rose"
    headline  = f"Ghana inflation {direction} to {latest['headline_inflation']:.1f}%"
    subline   = (f"Down {abs(change):.1f}pp from {prev['headline_inflation']:.1f}% last month · "
                 f"Core inflation at {latest['core_inflation_excl_energy']:.1f}%")
    kpi_val   = f"{latest['headline_inflation']:.1f}%"
    kpi_label = "Headline Inflation"
    period    = str(latest['date'])
    category  = "INFLATION WATCH"
    color     = TEAL if change < 0 else AMBER

    return dict(score=score, headline=headline, subline=subline,
                kpi_val=kpi_val, kpi_label=kpi_label, period=period,
                category=category, color=color,
                chart_type='line', chart_labels=labels,
                chart_values=values, chart_ylabel='Inflation (%)',
                caption_hook=f"🇬🇭 Ghana's headline inflation {direction} to {latest['headline_inflation']:.1f}% in {period}.")


def insight_fx():
    """Exchange rate movement."""
    df = load_bog_fx()
    recent = df.head(6)
    latest = recent.iloc[0]
    prev   = recent.iloc[1]
    rate_now  = latest['usd_ghs_period_average']
    rate_prev = prev['usd_ghs_period_average']
    pct_change = (rate_now - rate_prev) / rate_prev * 100

    score = abs(pct_change) * 3

    labels = recent['month'].tolist()[::-1]
    values = recent['usd_ghs_period_average'].tolist()[::-1]

    direction = "weakened" if pct_change > 0 else "strengthened"
    headline  = f"Cedi {direction} — 1 USD = GHS {rate_now:.2f}"
    subline   = (f"{abs(pct_change):.1f}% {'depreciation' if pct_change > 0 else 'appreciation'} "
                 f"vs. GHS {rate_prev:.2f} last month")
    kpi_val   = f"GHS {rate_now:.2f}"
    kpi_label = "1 USD ="
    period    = f"{latest['month']} {latest['year']}"
    category  = "CURRENCY WATCH"
    color     = AMBER if pct_change > 0 else TEAL

    return dict(score=score, headline=headline, subline=subline,
                kpi_val=kpi_val, kpi_label=kpi_label, period=period,
                category=category, color=color,
                chart_type='line', chart_labels=labels,
                chart_values=values, chart_ylabel='GHS per USD',
                caption_hook=f"💱 The Ghanaian cedi {direction} against the dollar: 1 USD = GHS {rate_now:.2f} in {period}.")


def insight_phones():
    """SGMPI mobile phones — median price + top listed brand."""
    df = load_sgmpi('sgmpi_mobile_phones_2026-04.csv')
    median_price = df['price_ghs'].median()
    mean_price   = df['price_ghs'].mean()
    n_listings   = len(df)

    # Price ranges
    bins   = [0, 500, 1000, 2000, 5000, 10000, 50000]
    labels = ['<500', '500–1k', '1k–2k', '2k–5k', '5k–10k', '10k+']
    df['band'] = pd.cut(df['price_ghs'], bins=bins, labels=labels)
    dist = df['band'].value_counts().reindex(labels, fill_value=0)

    # Most common region
    top_region = df['region'].value_counts().index[0] if 'region' in df.columns else 'Ghana'

    score = 60  # SGMPI is always newsworthy as it's our own data

    headline = f"Mobile phones: median asking price GHS {median_price:,.0f}"
    subline  = (f"{n_listings} listings analysed · Top region: {top_region} · "
                f"Avg GHS {mean_price:,.0f}")
    kpi_val   = f"GHS {median_price:,.0f}"
    kpi_label = "Median Phone Price"
    period    = "Apr 2026"
    category  = "SGMPI MARKET PRICE"
    color     = TEAL

    return dict(score=score, headline=headline, subline=subline,
                kpi_val=kpi_val, kpi_label=kpi_label, period=period,
                category=category, color=color,
                chart_type='bar', chart_labels=labels,
                chart_values=dist.tolist(), chart_ylabel='Listings',
                caption_hook=f"📱 Our SGMPI data shows the median mobile phone price in Ghana is GHS {median_price:,.0f} based on {n_listings} live listings.")


def insight_vehicles():
    """SGMPI vehicles — median price + condition breakdown."""
    df = load_sgmpi('sgmpi_vehicles_2026-04.csv')
    median_price = df['price_ghs'].median()
    n_listings   = len(df)

    # Condition split
    cond = df['condition'].value_counts().head(4)

    score = 55

    headline = f"Vehicles: median market price GHS {median_price:,.0f}"
    subline  = (f"{n_listings} vehicle listings across Ghana · "
                f"Most listed: {cond.index[0]} ({cond.iloc[0]} listings)")
    kpi_val   = f"GHS {median_price:,.0f}"
    kpi_label = "Median Vehicle Price"
    period    = "Apr 2026"
    category  = "SGMPI MARKET PRICE"
    color     = TEAL

    # Price distribution bands
    bins   = [0,10000,30000,60000,100000,200000,1000000]
    labels = ['<10k','10–30k','30–60k','60–100k','100–200k','200k+']
    df['band'] = pd.cut(df['price_ghs'], bins=bins, labels=labels)
    dist = df['band'].value_counts().reindex(labels, fill_value=0)

    return dict(score=score, headline=headline, subline=subline,
                kpi_val=kpi_val, kpi_label=kpi_label, period=period,
                category=category, color=color,
                chart_type='bar', chart_labels=labels,
                chart_values=dist.tolist(), chart_ylabel='Listings',
                caption_hook=f"🚗 Ghana vehicle market: median asking price is GHS {median_price:,.0f} across {n_listings} listings in our SGMPI database.")


def insight_reserves():
    """Ghana's international reserves."""
    df = load_bog_key()
    df = df.dropna(subset=['gross_international_reserves_usd_m']).head(6)
    latest = df.iloc[0]
    prev   = df.iloc[1]
    reserves_now  = latest['gross_international_reserves_usd_m']
    reserves_prev = prev['gross_international_reserves_usd_m']
    change_pct = (reserves_now - reserves_prev) / reserves_prev * 100

    score = abs(change_pct) * 2.5

    labels = df['date'].tolist()[::-1]
    values = df['gross_international_reserves_usd_m'].tolist()[::-1]

    direction = "rose" if change_pct > 0 else "fell"
    headline  = f"Ghana's reserves {direction} to USD {reserves_now/1000:.1f}B"
    subline   = (f"{abs(change_pct):.1f}% change vs last month · "
                 f"Up from USD {reserves_prev/1000:.1f}B")
    kpi_val   = f"${reserves_now/1000:.2f}B"
    kpi_label = "Int'l Reserves"
    period    = str(latest['date'])
    category  = "RESERVES & STABILITY"
    color     = TEAL if change_pct > 0 else AMBER

    return dict(score=score, headline=headline, subline=subline,
                kpi_val=kpi_val, kpi_label=kpi_label, period=period,
                category=category, color=color,
                chart_type='bar', chart_labels=labels,
                chart_values=[v/1000 for v in values], chart_ylabel='USD Billion',
                caption_hook=f"🏦 Ghana's gross international reserves {direction} to USD {reserves_now/1000:.1f}B in {period}.")


# ══════════════════════════════════════════════════════════════
#  INSIGHT SELECTOR — weekly rotation + score
# ══════════════════════════════════════════════════════════════

INSIGHT_POOL = [
    insight_inflation,
    insight_fx,
    insight_phones,
    insight_vehicles,
    insight_reserves,
]

def pick_insight():
    """Pick this week's insight: weekly rotation + boost for high-score candidates."""
    week_num   = datetime.now().isocalendar()[1]
    rotation   = week_num % len(INSIGHT_POOL)

    candidates = []
    for fn in INSIGHT_POOL:
        try:
            ins = fn()
            candidates.append(ins)
        except Exception as e:
            print(f"  Skipping {fn.__name__}: {e}")

    if not candidates:
        raise RuntimeError("No insights could be computed")

    # Primary: rotated pick; but if another candidate scores 2× higher, use that instead
    primary = candidates[rotation % len(candidates)]
    best    = max(candidates, key=lambda x: x['score'])

    if best['score'] > primary['score'] * 2:
        return best
    return primary


# ══════════════════════════════════════════════════════════════
#  FLYER GENERATOR
# ══════════════════════════════════════════════════════════════

def make_flyer(ins, out_path):
    """Render a 1080×1080 branded PNG flyer from an insight dict."""

    fig = plt.figure(figsize=(10.8, 10.8), dpi=100)
    fig.patch.set_facecolor(NAVY)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, 1080)
    ax.set_ylim(0, 1080)
    ax.axis('off')
    ax.set_facecolor(NAVY)

    accent = ins['color']

    # ── Top accent bar ─────────────────────────────────────────
    bar = patches.FancyBboxPatch((0, 1044), 1080, 36,
                                  boxstyle="square,pad=0",
                                  facecolor=accent, edgecolor='none')
    ax.add_patch(bar)

    # ── Logo + branding (top left) ────────────────────────────
    ax.text(40, 1020, "SG DATALYTICS",
            color=WHITE, fontsize=16, fontweight='bold',
            fontfamily='Lato', va='top', alpha=0.95)
    ax.text(40, 1002, "Ghana Economic Data",
            color=GREY2, fontsize=10, fontfamily='Lato', va='top')

    # ── "WEEKLY INSIGHT" pill (top right) ─────────────────────
    pill = patches.FancyBboxPatch((820, 1005), 220, 28,
                                   boxstyle="round,pad=4",
                                   facecolor=accent, edgecolor='none', alpha=0.18)
    ax.add_patch(pill)
    ax.text(930, 1019, ins['category'],
            color=accent, fontsize=9, fontweight='bold',
            fontfamily='Lato', ha='center', va='center', alpha=0.95)

    # ── Divider line ──────────────────────────────────────────
    ax.axhline(y=980, xmin=0.037, xmax=0.963,
               color=GREY2, linewidth=0.6, alpha=0.3)

    # ── KPI block ─────────────────────────────────────────────
    ax.text(40, 958, ins['kpi_label'].upper(),
            color=GREY2, fontsize=10, fontfamily='Lato', va='top')

    # Period badge (right side of KPI row)
    ax.text(1040, 958, ins['period'].upper(),
            color=accent, fontsize=10, fontweight='bold',
            fontfamily='Lato', va='top', ha='right', alpha=0.80)

    # Big KPI value
    ax.text(40, 930, ins['kpi_val'],
            color=accent, fontsize=68, fontweight='black',
            fontfamily='Lato', va='top')

    # ── Headline ──────────────────────────────────────────────
    wrapped = textwrap.fill(ins['headline'], width=46)
    lines   = wrapped.split('\n')
    y_head  = 830
    for line in lines:
        ax.text(40, y_head, line,
                color=WHITE, fontsize=26, fontweight='bold',
                fontfamily='Lato', va='top')
        y_head -= 34

    # ── Subline ───────────────────────────────────────────────
    # Replace commas in numbers to avoid bad word-wrap splits
    subline_clean = ins['subline']
    wrapped_sub = textwrap.fill(subline_clean, width=72, break_long_words=False)
    sub_lines   = wrapped_sub.split('\n')
    y_sub = y_head - 8
    for line in sub_lines:
        ax.text(40, y_sub, line,
                color=GREY1, fontsize=13.5, fontfamily='Lato', va='top', alpha=0.75)
        y_sub -= 20

    # ── Chart (embedded axes) ─────────────────────────────────
    chart_top    = 0.06   # fraction from bottom
    chart_height = 0.36
    chart_left   = 0.04
    chart_width  = 0.92

    chart_ax = fig.add_axes([chart_left, chart_top, chart_width, chart_height])
    chart_ax.set_facecolor(PANEL)
    chart_ax.tick_params(colors=GREY2, labelsize=9)
    for spine in chart_ax.spines.values():
        spine.set_edgecolor(GREY2)
        spine.set_alpha(0.2)
    chart_ax.spines['top'].set_visible(False)
    chart_ax.spines['right'].set_visible(False)

    labels = ins['chart_labels']
    values = ins['chart_values']
    x      = range(len(labels))

    if ins['chart_type'] == 'line':
        chart_ax.plot(x, values, color=accent, linewidth=2.5,
                      marker='o', markersize=6, markerfacecolor=accent,
                      markeredgecolor=NAVY, markeredgewidth=1.5)
        chart_ax.fill_between(x, values, alpha=0.12, color=accent)
        # Annotate last point
        chart_ax.annotate(f"{values[-1]:.1f}",
                          xy=(len(values)-1, values[-1]),
                          xytext=(6, 4), textcoords='offset points',
                          color=accent, fontsize=9, fontweight='bold')
    else:  # bar
        bars = chart_ax.bar(x, values, color=accent, alpha=0.80,
                            width=0.6, zorder=3)
        # Highlight tallest bar
        max_idx = values.index(max(values)) if max(values) > 0 else 0
        bars[max_idx].set_alpha(1.0)
        bars[max_idx].set_edgecolor(WHITE)
        bars[max_idx].set_linewidth(1.5)

    chart_ax.set_xticks(list(x))
    chart_ax.set_xticklabels(labels, fontfamily='Lato', color=GREY2, fontsize=9)
    chart_ax.set_ylabel(ins['chart_ylabel'], color=GREY2, fontsize=9,
                        fontfamily='Lato')
    chart_ax.yaxis.label.set_color(GREY2)
    chart_ax.tick_params(axis='y', colors=GREY2)
    chart_ax.tick_params(axis='x', colors=GREY2)
    chart_ax.grid(axis='y', color=GREY2, alpha=0.15, linewidth=0.8)
    chart_ax.set_facecolor(PANEL)

    # Chart title
    chart_ax.set_title(ins['chart_ylabel'] + " — SG Datalytics",
                       color=GREY2, fontsize=8, fontfamily='Lato',
                       loc='left', pad=6)

    # ── Bottom footer ─────────────────────────────────────────
    ax.text(40, 28, f"sgdatalytics.org  ·  Data updated {ins['period']}  ·  Sources: BoG, GSS, World Bank, SGMPI",
            color=GREY2, fontsize=9, fontfamily='Lato', va='bottom', alpha=0.6)

    ax.text(1040, 28, f"Week of {datetime.now().strftime('%d %b %Y')}",
            color=GREY2, fontsize=9, fontfamily='Lato',
            ha='right', va='bottom', alpha=0.6)

    plt.savefig(out_path, dpi=100, bbox_inches='tight',
                facecolor=NAVY, edgecolor='none')
    plt.close()
    print(f"  Flyer saved → {out_path}")


# ══════════════════════════════════════════════════════════════
#  LINKEDIN CAPTION
# ══════════════════════════════════════════════════════════════

def make_caption(ins):
    week_str = datetime.now().strftime('%d %b %Y')
    caption = f"""📊 SG Datalytics Weekly Insight — {week_str}

{ins['caption_hook']}

{ins['subline']}

This week's chart: {ins['chart_ylabel']} — {ins['period']}

🔍 Our datasets cover:
• Ghana CPI & Inflation (GSS + BoG)
• Exchange Rates (Bank of Ghana)
• GDP & Fiscal Data
• SG Market Price Index (SGMPI) — real market prices from online classifieds

📥 Access all 31 datasets at sgdatalytics.org/marketplace.html — free 10-row preview available without a subscription.

#Ghana #GhanaEconomy #DataAnalytics #SGDatalytics #SGMPI #GhanaInflation #GhanaData #EconomicInsight #AfricaData #OpenData"""
    return caption


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════

if __name__ == '__main__':
    print(f"\n{'='*55}")
    print(f"  SG Datalytics Weekly Insight — {datetime.now().strftime('%A %d %b %Y')}")
    print(f"{'='*55}\n")

    print("Scanning datasets...")
    ins = pick_insight()

    print(f"\n✓ Selected insight: [{ins['category']}]")
    print(f"  Headline : {ins['headline']}")
    print(f"  KPI      : {ins['kpi_val']} ({ins['kpi_label']})")
    print(f"  Score    : {ins['score']:.1f}")

    # Generate flyer
    week_tag  = datetime.now().strftime('%Y-W%W')
    flyer_path = os.path.join(FLYERS, f'sgdatalytics_insight_{week_tag}.png')
    print(f"\nGenerating flyer...")
    make_flyer(ins, flyer_path)

    # Save caption
    caption     = make_caption(ins)
    caption_path = os.path.join(FLYERS, f'sgdatalytics_caption_{week_tag}.txt')
    with open(caption_path, 'w') as f:
        f.write(caption)
    print(f"  Caption saved → {caption_path}")

    # Save metadata
    meta = {
        'week':      week_tag,
        'generated': datetime.now().isoformat(),
        'category':  ins['category'],
        'headline':  ins['headline'],
        'kpi_val':   ins['kpi_val'],
        'kpi_label': ins['kpi_label'],
        'flyer':     flyer_path,
        'caption':   caption_path,
    }
    meta_path = os.path.join(FLYERS, f'sgdatalytics_meta_{week_tag}.json')
    with open(meta_path, 'w') as f:
        json.dump(meta, f, indent=2)

    print(f"\n{'='*55}")
    print(f"  ✅ All done!")
    print(f"  Flyer  : {flyer_path}")
    print(f"  Caption: {caption_path}")
    print(f"{'='*55}\n")
    print("LinkedIn caption preview:\n")
    print(caption)
