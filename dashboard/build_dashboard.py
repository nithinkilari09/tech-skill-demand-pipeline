"""Builds dashboard/dist/index.html: a static, self-contained Plotly page
rendered from the Gold-layer aggregates, published to GitHub Pages by
.github/workflows/dashboard.yml on a schedule. Output lives in dist/ (gitignored,
rebuilt every run) so the Pages artifact -- which uploads the whole directory
it's pointed at -- publishes only the generated site, not this script.
No live backend -- interactivity (hover, zoom, legend toggle, the domain/field
dropdowns) is Plotly.js running entirely client-side against data baked into
the HTML at build time.

Connection: env vars DATABRICKS_HOST / DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN
(set as GitHub Actions secrets in CI) fall back to ~/.databrickscfg's `host` +
the warehouse HTTP path + PAT for local runs, so this script behaves the same
in CI and on a dev machine.
"""

import configparser
import os
from datetime import datetime, timezone
from pathlib import Path

import plotly.graph_objects as go
from databricks import sql

CATALOG = "tech_skill_demand"
WAREHOUSE_HTTP_PATH_DEFAULT = "/sql/1.0/warehouses/e9e6035f9dba7d89"
OUTPUT_DIR = Path(__file__).parent / "dist"
OUTPUT_PATH = OUTPUT_DIR / "index.html"

# Fixed color per CS domain -- "color follows the entity, never its rank":
# each domain keeps this hue everywhere it appears (overview bars, dropdown
# detail chart), rather than colors being reassigned by whatever order a
# query happens to return. Drawn from the categorical palette's first 6
# slots, in the palette's own validated order.
DOMAIN_COLORS = {
    "data engineer": "#2a78d6",   # slot 1 blue
    "data analyst": "#eb6834",    # slot 2 orange
    "frontend": "#1baf7a",        # slot 3 aqua
    "backend": "#eda100",         # slot 4 yellow
    "full-stack": "#e87ba4",      # slot 5 magenta
    "mobile": "#008300",          # slot 6 green
}
DOMAIN_ORDER = list(DOMAIN_COLORS.keys())

# Secondary section gets one consistent hue (violet, slot 7) across every
# broad_field view -- 11 buckets is too many for distinct per-entity colors
# to stay CVD-safe, and since only one field's bars are ever on screen at a
# time (dropdown-driven, not a shared multi-series legend), a single fixed
# hue reads as "you're in the Beyond Tech section" without stretching the
# categorical palette past what it validates for.
BROAD_FIELD_COLOR = "#4a3aa7"  # slot 7 violet

CHART_SURFACE_LIGHT = "#fcfcfb"
INK_LIGHT = "#0b0b0b"
MUTED = "#898781"
GRID_LIGHT = "#e1e0d9"


def _connection_params():
    host = os.environ.get("DATABRICKS_HOST")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH")
    token = os.environ.get("DATABRICKS_TOKEN")
    if host and http_path and token:
        return host, http_path, token

    # Local fallback: ~/.databrickscfg has host + token; the warehouse HTTP
    # path isn't stored there, so it defaults to the one warehouse this
    # project uses (scripts/config.py has no warehouse entry since Gold
    # queries the warehouse directly, not via scripts/).
    cfg = configparser.ConfigParser()
    cfg.read(Path.home() / ".databrickscfg")
    section = cfg["DEFAULT"]
    return section["host"].replace("https://", ""), WAREHOUSE_HTTP_PATH_DEFAULT, section["token"]


def fetch_table(cursor, query):
    cursor.execute(query)
    columns = [c[0] for c in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def fetch_scalar(cursor, query):
    cursor.execute(query)
    return cursor.fetchone()[0]


def top_n_by_group(rows, group_key, value_key, n=10):
    grouped = {}
    for row in rows:
        grouped.setdefault(row[group_key], []).append(row)
    return {
        key: sorted(items, key=lambda r: r[value_key], reverse=True)[:n]
        for key, items in grouped.items()
    }


CHART_FONT = dict(color=INK_LIGHT, family="system-ui, -apple-system, Segoe UI, sans-serif", size=13)
HOVERLABEL = dict(bgcolor="#ffffff", bordercolor=GRID_LIGHT, font=dict(color=INK_LIGHT, size=13))


def overview_bar_chart(rows, key_col, order, colors, subtitle):
    ordered = sorted(rows, key=lambda r: order.index(r[key_col]) if r[key_col] in order else 999)
    x = [r[key_col] for r in ordered]
    y = [r["posting_count"] for r in ordered]
    bar_colors = colors if isinstance(colors, str) else [colors.get(k) for k in x]

    fig = go.Figure(
        go.Bar(
            x=x,
            y=y,
            marker=dict(color=bar_colors, cornerradius=6),
            hovertemplate="<b>%{x}</b><br>%{y} postings<extra></extra>",
            text=y,
            textposition="outside",
            textfont=dict(color=MUTED, size=12),
        )
    )
    fig.update_layout(
        title=dict(text=subtitle, font=dict(size=15, color=MUTED), x=0, xanchor="left"),
        paper_bgcolor=CHART_SURFACE_LIGHT,
        plot_bgcolor=CHART_SURFACE_LIGHT,
        font=CHART_FONT,
        hoverlabel=HOVERLABEL,
        bargap=0.35,
        yaxis=dict(gridcolor=GRID_LIGHT, title=None, zeroline=False, showticklabels=False),
        xaxis=dict(showgrid=False, zeroline=False, tickfont=dict(size=12)),
        margin=dict(t=40, l=10, r=10, b=40),
        height=280,
    )
    return fig


def dropdown_skill_chart(top_by_group, groups_in_order, group_counts, colors, subtitle_prefix):
    """One figure, one trace per group, a dropdown (updatemenus) toggles
    which trace is visible -- genuinely interactive with no backend, since
    Plotly.js runs the visibility toggle entirely client-side."""
    fig = go.Figure()
    for i, group in enumerate(groups_in_order):
        items = top_by_group.get(group, [])
        items = sorted(items, key=lambda r: r["mention_count"])  # ascending for horizontal bar readability
        color = colors.get(group) if isinstance(colors, dict) else colors
        fig.add_trace(
            go.Bar(
                x=[r["mention_count"] for r in items],
                y=[r["skill"] for r in items],
                orientation="h",
                marker=dict(color=color, cornerradius=6),
                visible=(i == 0),
                hovertemplate="<b>%{y}</b><br>%{x} mentions<extra></extra>",
                name=group,
                text=[r["mention_count"] for r in items],
                textposition="outside",
                textfont=dict(color=MUTED, size=12),
            )
        )

    def subtitle(group):
        n = group_counts.get(group, 0)
        return f"{subtitle_prefix} — {group} ({n:,} postings)"

    buttons = [
        dict(
            label=group,
            method="update",
            args=[
                {"visible": [j == i for j in range(len(groups_in_order))]},
                {"title.text": subtitle(group)},
            ],
        )
        for i, group in enumerate(groups_in_order)
    ]

    fig.update_layout(
        title=dict(text=subtitle(groups_in_order[0]), font=dict(size=15, color=MUTED), x=0, xanchor="left"),
        paper_bgcolor=CHART_SURFACE_LIGHT,
        plot_bgcolor=CHART_SURFACE_LIGHT,
        font=CHART_FONT,
        hoverlabel=HOVERLABEL,
        bargap=0.3,
        xaxis=dict(gridcolor=GRID_LIGHT, title="Mentions", zeroline=False),
        yaxis=dict(showgrid=False, zeroline=False, tickfont=dict(size=12)),
        margin=dict(t=90, l=140, r=30, b=45),
        height=440,
        showlegend=False,
        updatemenus=[
            dict(
                buttons=buttons,
                direction="down",
                x=1.0,
                xanchor="right",
                y=1.32,
                yanchor="top",
                bgcolor="#ffffff",
                bordercolor=GRID_LIGHT,
                font=dict(size=12),
            )
        ],
    )
    return fig


def stat_tile(value, label, accent):
    return f"""<div class="tile">
      <div class="tile-value" style="color:{accent}">{value}</div>
      <div class="tile-label">{label}</div>
    </div>"""


def pipeline_step(icon, label, cadence, live):
    status_class = "step-live" if live else "step-planned"
    status_text = "Scheduled" if live else "Manual (next up)"
    return f"""<div class="step {status_class}">
      <div class="step-icon">{icon}</div>
      <div class="step-label">{label}</div>
      <div class="step-cadence">{cadence}</div>
      <div class="step-status">{status_text}</div>
    </div>"""


def render_page(figures, context):
    chart_divs = [
        fig.to_html(
            include_plotlyjs=("cdn" if i == 0 else False),
            full_html=False,
            div_id=f"plot-{i}",
            config={"displaylogo": False, "responsive": True},
        )
        for i, fig in enumerate(figures)
    ]

    tiles = "".join([
        stat_tile(f"{context['total_postings']:,}", "Postings analyzed", "#2a78d6"),
        stat_tile(str(len(DOMAIN_ORDER)), "CS domains tracked", "#1baf7a"),
        stat_tile(str(context["field_count"]), "Non-tech fields tracked", BROAD_FIELD_COLOR),
        stat_tile(str(context["skill_count"]), "Tools & skills tracked", "#eb6834"),
    ])

    pipeline = "".join([
        pipeline_step("\U0001F4E5", "Ingest", "RemoteOK + Arbeitnow", False),
        pipeline_step("\U0001F5C4️", "Bronze", "Raw postings landed", False),
        pipeline_step("\U0001F9F9", "Silver", "Classified & skill-matched", False),
        pipeline_step("\U0001F4CA", "Gold", "Skill-demand aggregates", False),
        pipeline_step("\U0001F310", "This dashboard", "Rebuilt daily", True),
    ])

    html = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Tech Skill Demand Dashboard</title>
<meta name="description" content="Real-time tech job posting skill demand by domain, sourced from RemoteOK and Arbeitnow.">
<style>
  :root {{
    color-scheme: light;
    --page: #f4f5f7;
    --surface: {CHART_SURFACE_LIGHT};
    --text-primary: {INK_LIGHT};
    --text-secondary: #52514e;
    --muted: {MUTED};
    --border: rgba(11,11,11,0.08);
    --shadow: 0 1px 2px rgba(11,11,11,0.04), 0 8px 24px rgba(11,11,11,0.06);
  }}
  @media (prefers-color-scheme: dark) {{
    :root {{
      color-scheme: dark;
      --page: #0d0d0d;
      --surface: #17171a;
      --text-primary: #ffffff;
      --text-secondary: #c3c2b7;
      --muted: #9c9a94;
      --border: rgba(255,255,255,0.08);
      --shadow: 0 1px 2px rgba(0,0,0,0.3), 0 8px 24px rgba(0,0,0,0.4);
    }}
  }}
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0;
    background: var(--page);
    color: var(--text-primary);
    font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
    -webkit-font-smoothing: antialiased;
  }}
  .wrap {{ max-width: 1160px; margin: 0 auto; padding: 0 20px 60px; }}
  .hero {{
    background: linear-gradient(135deg, #14335c 0%, #1c5cab 55%, #2a78d6 100%);
    color: #ffffff;
    padding: 48px 20px 40px;
    margin-bottom: -60px;
  }}
  .hero-inner {{ max-width: 1160px; margin: 0 auto; }}
  .hero h1 {{ font-size: 2rem; margin: 0 0 10px; letter-spacing: -0.02em; }}
  .hero p {{ color: rgba(255,255,255,0.85); margin: 0; max-width: 70ch; line-height: 1.55; font-size: 1.02rem; }}
  .live-badge {{
    display: inline-flex; align-items: center; gap: 7px;
    background: rgba(255,255,255,0.14);
    border: 1px solid rgba(255,255,255,0.22);
    border-radius: 999px;
    padding: 6px 14px;
    font-size: 0.82rem;
    margin-top: 18px;
    color: #ffffff;
  }}
  .live-dot {{
    width: 8px; height: 8px; border-radius: 50%;
    background: #4ade80;
    box-shadow: 0 0 0 rgba(74,222,128,0.6);
    animation: pulse 2s infinite;
  }}
  @keyframes pulse {{
    0% {{ box-shadow: 0 0 0 0 rgba(74,222,128,0.55); }}
    70% {{ box-shadow: 0 0 0 8px rgba(74,222,128,0); }}
    100% {{ box-shadow: 0 0 0 0 rgba(74,222,128,0); }}
  }}

  .tiles {{
    position: relative;
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 16px;
    margin-bottom: 36px;
  }}
  .tile {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 14px;
    box-shadow: var(--shadow);
    padding: 20px 18px;
  }}
  .tile-value {{ font-size: 1.9rem; font-weight: 700; line-height: 1; letter-spacing: -0.02em; }}
  .tile-label {{ color: var(--text-secondary); font-size: 0.85rem; margin-top: 6px; }}

  .automation-card {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 14px;
    box-shadow: var(--shadow);
    padding: 24px;
    margin-bottom: 40px;
  }}
  .automation-card h2 {{ margin: 0 0 4px; font-size: 1.15rem; }}
  .automation-card > p {{ color: var(--text-secondary); margin: 0 0 20px; font-size: 0.92rem; line-height: 1.5; max-width: 75ch; }}
  .pipeline {{
    display: grid;
    grid-template-columns: repeat(5, 1fr);
    gap: 10px;
  }}
  .step {{
    border-radius: 10px;
    padding: 14px 10px;
    text-align: center;
    border: 1px solid var(--border);
    position: relative;
  }}
  .step-planned {{ background: color-mix(in srgb, var(--muted) 8%, transparent); }}
  .step-live {{ background: color-mix(in srgb, #1baf7a 12%, transparent); border-color: color-mix(in srgb, #1baf7a 40%, transparent); }}
  .step-icon {{ font-size: 1.4rem; }}
  .step-label {{ font-weight: 600; font-size: 0.85rem; margin-top: 6px; }}
  .step-cadence {{ color: var(--text-secondary); font-size: 0.72rem; margin-top: 2px; }}
  .step-status {{ font-size: 0.68rem; margin-top: 8px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.02em; }}
  .step-live .step-status {{ color: #1baf7a; }}
  .step-planned .step-status {{ color: var(--muted); }}
  .pipeline-arrow {{ display: none; }}

  section {{ margin-top: 44px; }}
  section .section-head {{ display: flex; align-items: baseline; justify-content: space-between; gap: 12px; flex-wrap: wrap; }}
  section h2 {{ font-size: 1.3rem; margin: 0; }}
  section .section-head .count {{ color: var(--muted); font-size: 0.85rem; }}
  section > p.lede {{ color: var(--text-secondary); max-width: 72ch; line-height: 1.55; margin-top: 6px; }}
  .chart-card {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 14px;
    box-shadow: var(--shadow);
    padding: 18px 18px 8px;
    margin-top: 16px;
    overflow-x: auto;
    transition: box-shadow 0.15s ease;
  }}
  .chart-card:hover {{ box-shadow: 0 2px 4px rgba(11,11,11,0.06), 0 12px 32px rgba(11,11,11,0.09); }}
  .charts-grid {{ display: grid; grid-template-columns: 1fr; gap: 0; }}

  footer {{
    margin-top: 56px;
    padding-top: 18px;
    border-top: 1px solid var(--border);
    color: var(--muted);
    font-size: 0.85rem;
    line-height: 1.6;
  }}
  footer a {{ color: var(--text-secondary); }}

  @media (max-width: 820px) {{
    .tiles {{ grid-template-columns: repeat(2, 1fr); }}
    .pipeline {{ grid-template-columns: repeat(3, 1fr); }}
  }}
  @media (max-width: 520px) {{
    .tiles {{ grid-template-columns: 1fr 1fr; }}
    .pipeline {{ grid-template-columns: 1fr 1fr; }}
    .hero h1 {{ font-size: 1.5rem; }}
  }}
</style>
</head>
<body>
  <div class="hero">
    <div class="hero-inner">
      <h1>Tech Skill Demand Dashboard</h1>
      <p>Real tool/skill demand across live job postings, pooled from RemoteOK and Arbeitnow
      and classified through a Bronze/Silver/Gold pipeline on Databricks. Postings without a
      clear classification are excluded rather than shown as an "unidentified" bucket.</p>
      <div class="live-badge"><span class="live-dot"></span> This page rebuilds automatically &mdash; last generated {context['generated_at']}</div>
    </div>
  </div>

  <div class="wrap">
    <div style="height: 76px"></div>
    <div class="tiles">
      {tiles}
    </div>

    <div class="automation-card">
      <h2>Built to run itself, on a schedule</h2>
      <p>The whole point of this project is a pipeline that keeps itself current without a human
      re-running anything. This dashboard's own rebuild is already fully automated (GitHub Actions,
      cron-scheduled, no server to keep running); wiring the same time-based scheduling into the
      ingestion &rarr; Bronze &rarr; Silver &rarr; Gold steps upstream is the next milestone.</p>
      <div class="pipeline">
        {pipeline}
      </div>
    </div>

    <section>
      <div class="section-head">
        <h2>Tech Skill Demand by Domain</h2>
        <span class="count">{context['total_postings']:,} postings analyzed</span>
      </div>
      <p class="lede">Which tools and languages show up most often in postings for each core
      CS domain &mdash; data engineer, data analyst, frontend, full-stack, backend, and mobile.</p>
      <div class="charts-grid">
        <div class="chart-card">{chart_divs[0]}</div>
        <div class="chart-card">{chart_divs[1]}</div>
      </div>
    </section>

    <section>
      <div class="section-head">
        <h2>Beyond Tech: Tool Demand Across All Fields</h2>
        <span class="count">{context['field_total_postings']:,} postings analyzed</span>
      </div>
      <p class="lede">The same job-posting feed also carries plenty of non-tech roles &mdash;
      sales, finance, IT operations, project management, and more. Same skill-extraction
      pipeline, applied to every field a posting might belong to.</p>
      <div class="charts-grid">
        <div class="chart-card">{chart_divs[2]}</div>
        <div class="chart-card">{chart_divs[3]}</div>
      </div>
    </section>

    <footer>
      <p>Data sourced from <a href="https://remoteok.com" target="_blank" rel="noopener">RemoteOK</a>
      and <a href="https://arbeitnow.com" target="_blank" rel="noopener">Arbeitnow</a>.
      Rebuilt on a schedule via GitHub Actions, querying Databricks Delta tables through the
      SQL warehouse &mdash; no live backend, no server to keep running.</p>
      <p>Generated {context['generated_at']}</p>
    </footer>
  </div>
</body>
</html>"""
    return html


def _connect(host, http_path, token):
    import logging

    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("databricks.sql").setLevel(logging.DEBUG)
    logging.getLogger("urllib3").setLevel(logging.DEBUG)
    try:
        return sql.connect(server_hostname=host, http_path=http_path, access_token=token)
    except Exception as e:
        # databricks-sql-connector's own RequestError message is often just
        # "Error during request to server." with the actual cause buried in
        # __cause__/__context__ -- surface those explicitly so CI logs show
        # what actually failed instead of the generic wrapper message.
        print(f"Connection failed: {type(e).__name__}: {e}")
        cause = e.__cause__ or e.__context__
        depth = 0
        while cause is not None and depth < 5:
            print(f"  caused by: {type(cause).__name__}: {cause}")
            cause = cause.__cause__ or cause.__context__
            depth += 1
        raise


def main():
    host, http_path, token = _connection_params()
    print(f"Connecting to {host}{http_path} ...")

    with _connect(host, http_path, token) as conn:
        with conn.cursor() as cursor:
            domain_summary = fetch_table(
                cursor,
                f"""SELECT domain, posting_count FROM {CATALOG}.gold.domain_summary
                    WHERE domain != 'other/uncategorized'""",
            )
            skill_by_domain = fetch_table(
                cursor,
                f"""SELECT domain, skill, mention_count FROM {CATALOG}.gold.skill_demand_by_domain
                    WHERE domain != 'other/uncategorized'""",
            )
            broad_field_summary = fetch_table(
                cursor,
                f"""SELECT broad_field, posting_count FROM {CATALOG}.gold.broad_field_summary
                    WHERE broad_field != 'Other'""",
            )
            skill_by_broad_field = fetch_table(
                cursor,
                f"""SELECT broad_field, skill, mention_count FROM {CATALOG}.gold.skill_demand_by_broad_field
                    WHERE broad_field != 'Other'""",
            )
            total_postings = fetch_scalar(cursor, f"SELECT count(*) FROM {CATALOG}.silver.cleaned_postings")
            skill_count = fetch_scalar(cursor, f"SELECT count(*) FROM {CATALOG}.silver.skill_dictionary")

    print(
        f"Fetched: {len(domain_summary)} domains, {len(skill_by_domain)} domain-skill rows, "
        f"{len(broad_field_summary)} fields, {len(skill_by_broad_field)} field-skill rows, "
        f"{total_postings} total postings, {skill_count} tracked skills"
    )

    broad_field_order = sorted(
        {r["broad_field"] for r in broad_field_summary},
        key=lambda f: -next((r["posting_count"] for r in broad_field_summary if r["broad_field"] == f), 0),
    )
    domain_counts = {r["domain"]: r["posting_count"] for r in domain_summary}
    field_counts = {r["broad_field"]: r["posting_count"] for r in broad_field_summary}

    fig_domain_overview = overview_bar_chart(
        domain_summary, "domain", DOMAIN_ORDER, DOMAIN_COLORS, "Postings per CS domain",
    )
    fig_domain_skills = dropdown_skill_chart(
        top_n_by_group(skill_by_domain, "domain", "mention_count"),
        DOMAIN_ORDER, domain_counts, DOMAIN_COLORS, "Top skills",
    )
    fig_field_overview = overview_bar_chart(
        broad_field_summary, "broad_field", broad_field_order, BROAD_FIELD_COLOR, "Postings per field",
    )
    fig_field_skills = dropdown_skill_chart(
        top_n_by_group(skill_by_broad_field, "broad_field", "mention_count"),
        broad_field_order, field_counts, BROAD_FIELD_COLOR, "Top skills",
    )

    context = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "total_postings": total_postings,
        "field_count": len(broad_field_order),
        "skill_count": skill_count,
        "field_total_postings": sum(field_counts.values()),
    }
    html = render_page(
        [fig_domain_overview, fig_domain_skills, fig_field_overview, fig_field_skills],
        context,
    )

    OUTPUT_DIR.mkdir(exist_ok=True)
    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"Wrote {OUTPUT_PATH} ({len(html):,} bytes)")


if __name__ == "__main__":
    main()
