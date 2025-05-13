import argparse
import glob
import pandas as pd
import matplotlib.pyplot as plt

# 1) Parse CLI args
p = argparse.ArgumentParser()
p.add_argument('--x', choices=['threads','files'],   required=True)
p.add_argument('--y', choices=['ops_per_sec','elapsed_ms'], required=True)
p.add_argument('--pattern', type=str,
               default='results/results_*.csv',
               help='Glob pattern to match result CSV files')
p.add_argument('--output', type=str,
               default='plot.pdf',
               help='Filename (with .pdf) to save the figure')
args = p.parse_args()

# 2) Load all CSVs
files = glob.glob(args.pattern)
if not files:
    raise FileNotFoundError(f"No files match {args.pattern}")
df = pd.concat((pd.read_csv(f) for f in files), ignore_index=True)

# 3) Compute mean and percentiles
group = df.groupby(args.x)[args.y]
mean = group.mean()
q25  = group.quantile(0.25)
q75  = group.quantile(0.75)

# 4) Plot with “T” error bars
fig, ax = plt.subplots()
# yerr takes two arrays: lower-error, upper-error
lower_err = mean.values - q25.values
upper_err = q75.values - mean.values

ax.errorbar(
    mean.index, 
    mean.values, 
    yerr=[lower_err, upper_err],
    fmt='o-',         # circle markers with a line
    capsize=5,        # length of the little T caps
    markersize=6,
    linewidth=1
)
ax.set_xlabel(args.x)
ax.set_ylabel(args.y)
ax.set_title(f"{args.y} vs {args.x} (mean ± 25/75 pct)")
plt.tight_layout()

# 5) Save to PDF
plt.savefig(args.output)
print(f"Saved plot (with percentiles) to {args.output}")
