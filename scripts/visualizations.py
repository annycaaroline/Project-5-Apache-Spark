#!/usr/bin/env python3
"""
Climate Anomaly Visualization Script figures from MongoDB results
"""

from pymongo import MongoClient
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

print("=" * 60)
print("CLIMATE ANOMALY VISUALIZATION")
print("=" * 60)

# Connect to MongoDB
print("\n1. Connecting to MongoDB...")
client = MongoClient('mongodb://localhost:"WRITE YOUR LOCAL HOST HERE"/')
db = client['climate_analysis']

# Set style figures
plt.style.use('seaborn-v0_8-darkgrid')
plt.rcParams['figure.dpi'] = 300
plt.rcParams['font.size'] = 10
plt.rcParams['axes.labelsize'] = 11
plt.rcParams['axes.titlesize'] = 12
plt.rcParams['legend.fontsize'] = 9

# ============================================================
# Temperature Anomalies by Decade
# ============================================================
print("\n2. Creating Figure 1: Anomalies by Decade...")

data = list(db.anomalies_by_decade.find())
df_anomalies = pd.DataFrame(data)

# Pivot data
pivot = df_anomalies.pivot(index='decade', columns='type', values='count')
pivot = pivot[['extreme_cold', 'cold', 'normal', 'heat', 'extreme_heat']]

# Create figure
fig, ax = plt.subplots(figsize=(12, 6))
pivot.plot(
    kind='bar',
    stacked=True,
    ax=ax,
    color=['#2166ac', '#67a9cf', '#d9d9d9', '#ef8a62', '#b2182b'],
    edgecolor='white',
    linewidth=0.5
)

ax.set_title(
    'Temperature Anomaly Distribution by Decade (1980-2020)',
    fontsize=14, fontweight='bold', pad=15
)
ax.set_xlabel('Decade', fontsize=12, fontweight='bold')
ax.set_ylabel('Frequency Count', fontsize=12, fontweight='bold')
ax.legend(
    title='Anomaly Type',
    bbox_to_anchor=(1.02, 1),
    loc='upper left',
    frameon=True,
    shadow=True
)
ax.grid(axis='y', alpha=0.3, linestyle='--')
plt.xticks(rotation=0)
plt.tight_layout()
plt.savefig('figures/fig1_anomalies_by_decade.png', dpi=300, bbox_inches='tight')
plt.close()
print("   Saved: figures/fig1_anomalies_by_decade.png")

# ============================================================
# Heat vs Cold Comparison
# ============================================================
print("\n3. Creating Figure 2: Heat vs Cold Comparison...")

data_hc = list(db.heat_cold_comparison.find())
df_hc = pd.DataFrame(data_hc)

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

# Left: Counts
ax1.plot(
    df_hc['decade'], df_hc['heat_count'], 'o-',
    linewidth=2.5, markersize=10,
    label='Heat Events', markeredgecolor='white', markeredgewidth=1
)
ax1.plot(
    df_hc['decade'], df_hc['cold_count'], 'o-',
    linewidth=2.5, markersize=10,
    label='Cold Events', markeredgecolor='white', markeredgewidth=1
)
ax1.set_title('Heat vs Cold Anomaly Counts', fontsize=12, fontweight='bold')
ax1.set_xlabel('Decade', fontsize=11, fontweight='bold')
ax1.set_ylabel('Event Count', fontsize=11, fontweight='bold')
ax1.legend(frameon=True, shadow=True)
ax1.grid(True, alpha=0.3, linestyle='--')
ax1.set_ylim(
    0,
    max(df_hc['heat_count'].max(), df_hc['cold_count'].max()) * 1.1
)

# Right: Ratio
ax2.plot(
    df_hc['decade'], df_hc['ratio'], 'o-',
    linewidth=2.5, markersize=10,
    markeredgecolor='white', markeredgewidth=1
)
ax2.axhline(
    y=1.0, linestyle='--',
    linewidth=2, alpha=0.7, label='Equal ratio (1.0)'
)
ax2.fill_between(
    df_hc['decade'], 1, df_hc['ratio'],
    where=(df_hc['ratio'] > 1),
    alpha=0.3, label='Heat dominant'
)
ax2.fill_between(
    df_hc['decade'], df_hc['ratio'], 1,
    where=(df_hc['ratio'] < 1),
    alpha=0.3, label='Cold dominant'
)
ax2.set_title(
    'Heat-to-Cold Anomaly Ratio Evolution',
    fontsize=12, fontweight='bold'
)
ax2.set_xlabel('Decade', fontsize=11, fontweight='bold')
ax2.set_ylabel('Ratio (Heat/Cold)', fontsize=11, fontweight='bold')
ax2.legend(frameon=True, shadow=True, loc='upper left')
ax2.grid(True, alpha=0.3, linestyle='--')
ax2.set_ylim(0, df_hc['ratio'].max() * 1.15)

plt.tight_layout()
plt.savefig('figures/fig2_heat_cold_comparison.png', dpi=300, bbox_inches='tight')
plt.close()
print("   Saved: figures/fig2_heat_cold_comparison.png")

# ============================================================
#  Trend Analysis 
# ============================================================
print("\n4. Creating Figure 3: Temporal Trends...")

fig, ax = plt.subplots(figsize=(10, 6))

# Calculate percentages
df_hc['heat_pct'] = (
    df_hc['heat_count'] /
    (df_hc['heat_count'] + df_hc['cold_count'])
) * 100
df_hc['cold_pct'] = (
    df_hc['cold_count'] /
    (df_hc['heat_count'] + df_hc['cold_count'])
) * 100

ax.plot(
    df_hc['decade'], df_hc['heat_pct'], 'o-',
    linewidth=3, markersize=12,
    label='Heat Anomalies (%)',
    markeredgecolor='white', markeredgewidth=1.5
)
ax.plot(
    df_hc['decade'], df_hc['cold_pct'], 'o-',
    linewidth=3, markersize=12,
    label='Cold Anomalies (%)',
    markeredgecolor='white', markeredgewidth=1.5
)
ax.axhline(y=50, linestyle='--', linewidth=2, alpha=0.5)

ax.set_title(
    'Temporal Evolution of Heat vs Cold Anomalies (1980-2020)',
    fontsize=13, fontweight='bold', pad=15
)
ax.set_xlabel('Decade', fontsize=12, fontweight='bold')
ax.set_ylabel(
    'Percentage of Total Anomalies (%)',
    fontsize=12, fontweight='bold'
)
ax.legend(frameon=True, shadow=True, loc='best', fontsize=11)
ax.grid(True, alpha=0.3, linestyle='--')
ax.set_ylim(30, 70)

plt.tight_layout()
plt.savefig('figures/fig3_temporal_trends.png', dpi=300, bbox_inches='tight')
plt.close()
print("   Saved: figures/fig3_temporal_trends.png")

# ============================================================
# Summary Statistics
# ============================================================
print("\n5. Generating Summary Statistics Table...")

print("\n" + "=" * 70)
print("HEAT VS COLD COMPARISON BY DECADE")
print("=" * 70)
summary = df_hc[['decade', 'heat_count', 'cold_count', 'ratio']].copy()
summary.columns = ['Decade', 'Heat Events', 'Cold Events', 'Heat/Cold Ratio']
summary['Change (%)'] = summary['Heat/Cold Ratio'].pct_change() * 100
print(summary.to_string(index=False))
print("=" * 70)

# Key findings
print("\n" + "=" * 70)
print("KEY FINDINGS")
print("=" * 70)
print(f"1. Initial ratio (1980): {df_hc.iloc[0]['ratio']:.2f}")
print(f"2. Final ratio (2020): {df_hc.iloc[-1]['ratio']:.2f}")
print(
    f"3. Total increase: "
    f"{((df_hc.iloc[-1]['ratio'] / df_hc.iloc[0]['ratio']) - 1) * 100:.1f}%"
)
print(f"4. Heat events in 2020: {df_hc.iloc[-1]['heat_count']:,}")
print(f"5. Cold events in 2020: {df_hc.iloc[-1]['cold_count']:,}")
print("=" * 70)

# Correlation
corr_data = list(db.temp_precip_correlation.find())
if corr_data:
    print("\nTEMPERATURE-PRECIPITATION CORRELATION")
    print("=" * 70)
    print(f"Correlation coefficient: {corr_data[0]['correlation']:.3f}")
    print(f"P-value: {corr_data[0]['p_value']:.4f}")
    print(f"Sample size: {corr_data[0]['n_samples']:,}")
    print(f"Interpretation: {corr_data[0]['interpretation']}")
    print("=" * 70)
