# app.py — Banking Dashboard FINAL
# Clean sqlcmd connection to Synapse Serverless SQL
from flask import Flask, jsonify
import os, subprocess, tempfile

app = Flask(__name__)

# ── Mock data — 13 columns matching view exactly ──────────────
MOCK = [
    {"customer_id":"CUST00001","name":"Alice Johnson",
     "segment":"Premium","risk_profile":"Moderate",
     "risk_score":58.3,"risk_category":"Medium",
     "portfolio_value_usd":245000,"diversification_score":0.74,
     "ticker":"AAPL","quantity":150,
     "current_price":167.3,"pnl_percent":17.4,"unrealized_pnl_usd":3720.0},
    {"customer_id":"CUST00002","name":"Bob Smith",
     "segment":"Private Banking","risk_profile":"Aggressive",
     "risk_score":82.1,"risk_category":"High",
     "portfolio_value_usd":890000,"diversification_score":0.45,
     "ticker":"MSFT","quantity":200,
     "current_price":310.5,"pnl_percent":10.9,"unrealized_pnl_usd":6100.0},
    {"customer_id":"CUST00003","name":"Carol White",
     "segment":"Retail","risk_profile":"Conservative",
     "risk_score":22.4,"risk_category":"Low",
     "portfolio_value_usd":45000,"diversification_score":0.91,
     "ticker":"JPM","quantity":50,
     "current_price":142.0,"pnl_percent":5.2,"unrealized_pnl_usd":350.0},
    {"customer_id":"CUST00004","name":"David Lee",
     "segment":"Premium","risk_profile":"Moderate",
     "risk_score":61.7,"risk_category":"Medium",
     "portfolio_value_usd":320000,"diversification_score":0.68,
     "ticker":"GOOGL","quantity":100,
     "current_price":138.0,"pnl_percent":10.4,"unrealized_pnl_usd":1300.0},
    {"customer_id":"CUST00005","name":"Eva Martinez",
     "segment":"Private Banking","risk_profile":"Conservative",
     "risk_score":18.9,"risk_category":"Low",
     "portfolio_value_usd":1200000,"diversification_score":0.95,
     "ticker":"BAC","quantity":500,
     "current_price":36.5,"pnl_percent":14.1,"unrealized_pnl_usd":2250.0},
    {"customer_id":"CUST00006","name":"Frank Zhang",
     "segment":"Retail","risk_profile":"Aggressive",
     "risk_score":79.5,"risk_category":"High",
     "portfolio_value_usd":67000,"diversification_score":0.42,
     "ticker":"GS","quantity":80,
     "current_price":395.0,"pnl_percent":-3.2,"unrealized_pnl_usd":-2560.0},
]

# ── Query Synapse using sqlcmd subprocess ─────────────────────
def query_synapse(where_clause=""):
    server   = os.environ.get("SYNAPSE_SERVER","").strip()
    database = os.environ.get("SYNAPSE_DB","banking_insights").strip()
    user     = os.environ.get("SYNAPSE_USER","").strip()
    password = os.environ.get("SYNAPSE_PASSWORD","").strip()

    if not all([server, user, password]):
        raise Exception("Missing env variables")

    where = f"WHERE {where_clause}" if where_clause else ""

    # Cast ALL numbers to avoid scientific notation (1.19e+007 breaks parsing)
    sql = f"""SET NOCOUNT ON;
SELECT TOP 100
    CAST(customer_id AS VARCHAR(20))                              AS c0,
    CAST(name AS VARCHAR(100))                                    AS c1,
    CAST(segment AS VARCHAR(50))                                  AS c2,
    CAST(risk_profile AS VARCHAR(30))                             AS c3,
    CAST(ROUND(risk_score,1) AS DECIMAL(10,1))                    AS c4,
    CAST(risk_category AS VARCHAR(20))                            AS c5,
    CAST(ROUND(portfolio_value_usd,0) AS BIGINT)                  AS c6,
    CAST(ROUND(diversification_score,3) AS DECIMAL(10,3))         AS c7,
    CAST(ticker AS VARCHAR(10))                                   AS c8,
    CAST(quantity AS BIGINT)                                      AS c9,
    CAST(ROUND(current_price,2) AS DECIMAL(10,2))                 AS c10,
    CAST(ROUND(pnl_percent,2) AS DECIMAL(10,2))                   AS c11,
    CAST(ROUND(unrealized_pnl_usd,0) AS BIGINT)                   AS c12
FROM dbo.vw_customer_portfolio_dashboard
{where}
ORDER BY portfolio_value_usd DESC;
"""

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".sql", delete=False
    ) as f:
        f.write(sql)
        sql_file = f.name

    try:
        result = subprocess.run(
            [
                "sqlcmd",
                "-S", server,
                "-d", database,
                "-U", user,
                "-P", password,
                "-i", sql_file,
                "-s", "~",   # sqlcmd ignores \t — use ~ as separator
                "-W",        # trim whitespace
                "-h", "-1",  # no headers
                "-l", "30",
                "-t", "30",
                "-N",
                "-C",
            ],
            capture_output=True,
            text=True,
            timeout=45
        )

        print(f"[SQLCMD] RC={result.returncode}")
        if result.stderr:
            print(f"[SQLCMD] ERR={result.stderr[:200]}")

        if result.returncode != 0:
            raise Exception(f"sqlcmd RC={result.returncode}: {result.stderr.strip()[:300]}")

        rows = []
        for line in result.stdout.split("\n"):
            line = line.strip()
            if not line:
                continue
            # Skip the Synapse stats header line
            if line.startswith("Statement ID:") or "Query hash" in line:
                continue
            # Skip separator lines (dashes only)
            if all(c in "-~ " for c in line):
                continue

            parts = [p.strip() for p in line.split("~")]

            if len(parts) < 13:
                continue

            # First part must look like a customer ID
            if not parts[0].startswith("CUST"):
                continue

            try:
                rows.append({
                    "customer_id"          : parts[0],
                    "name"                 : parts[1],
                    "segment"              : parts[2],
                    "risk_profile"         : parts[3],
                    "risk_score"           : float(parts[4] or 0),
                    "risk_category"        : parts[5],
                    "portfolio_value_usd"  : float(parts[6] or 0),
                    "diversification_score": float(parts[7] or 0),
                    "ticker"               : parts[8],
                    "quantity"             : int(float(parts[9] or 0)),
                    "current_price"        : float(parts[10] or 0),
                    "pnl_percent"          : float(parts[11] or 0),
                    "unrealized_pnl_usd"   : float(parts[12] or 0),
                })
            except (ValueError, IndexError) as e:
                print(f"[PARSE ERROR] {e} → {line[:80]}")
                continue

        if not rows:
            raise Exception(f"0 rows parsed. Sample: {result.stdout[:200]}")

        print(f"[OK] Parsed {len(rows)} rows from Synapse!")
        return rows

    finally:
        try:
            os.unlink(sql_file)
        except Exception:
            pass

# ── Build HTML table ──────────────────────────────────────────
def build_table(rows):
    if not rows:
        return "<tr><td colspan='11' style='text-align:center;\
color:#888;padding:20px'>No data</td></tr>"
    html = ""
    for r in rows:
        rcat = str(r.get("risk_category",""))
        bg   = {"Low":"#d4edda","Medium":"#fff3cd",
                "High":"#f8d7da"}.get(rcat,"#fff")
        pnl  = float(r.get("pnl_percent",0))
        pc   = "#107c10" if pnl >= 0 else "#d83b01"
        pval = float(r.get("portfolio_value_usd",0))
        upnl = float(r.get("unrealized_pnl_usd",0))
        html += f"""<tr style='background:{bg}'>
            <td>{r.get('customer_id','')}</td>
            <td><b>{r.get('name','')}</b></td>
            <td>{r.get('segment','')}</td>
            <td>{r.get('risk_profile','')}</td>
            <td><b>{rcat}</b></td>
            <td>{r.get('risk_score','')}</td>
            <td><b>${pval:,.0f}</b></td>
            <td>{r.get('ticker','')}</td>
            <td>{r.get('quantity','')}</td>
            <td>${r.get('current_price','')}</td>
            <td style='color:{pc}'><b>{pnl:+.1f}%</b></td>
            <td style='color:{pc}'>${upnl:,.0f}</td>
        </tr>"""
    return html

# ── Build page ────────────────────────────────────────────────
def build_page(rows, title, label, color, pod, err=""):
    ebox = f"""<div style='background:#fff3cd;border:1px solid
        #ffc107;padding:12px;border-radius:6px;margin-bottom:14px;
        font-size:12px;word-break:break-all'>
        ⚠️ <b>Info:</b> {err}</div>""" if err else ""

    nav = """<div style='margin-bottom:16px;display:flex;
        gap:10px;flex-wrap:wrap'>
        <a href='/' style='background:#0078d4;color:white;
           padding:8px 16px;border-radius:6px;
           text-decoration:none;font-size:14px'>📊 All</a>
        <a href='/high-risk' style='background:#d83b01;color:white;
           padding:8px 16px;border-radius:6px;
           text-decoration:none;font-size:14px'>🔴 High Risk</a>
        <a href='/top-portfolios' style='background:#107c10;color:white;
           padding:8px 16px;border-radius:6px;
           text-decoration:none;font-size:14px'>💰 Top 10</a>
        <a href='/data' style='background:#5c2d91;color:white;
           padding:8px 16px;border-radius:6px;
           text-decoration:none;font-size:14px'>🔍 JSON</a>
    </div>"""

    return f"""<!DOCTYPE html><html>
    <head><title>🏦 {title}</title>
    <meta http-equiv='refresh' content='30'>
    <style>
        *      {{box-sizing:border-box;margin:0;padding:0}}
        body   {{font-family:'Segoe UI',Arial;background:#f0f4f8;padding:30px}}
        h1     {{color:#1a1a2e;border-bottom:3px solid #0078d4;
                 padding-bottom:12px;margin-bottom:20px}}
        .arch  {{background:white;padding:16px;border-radius:8px;
                 border-left:5px solid #0078d4;margin-bottom:14px;
                 box-shadow:0 2px 6px rgba(0,0,0,.07)}}
        .badge  {{background:#0078d4;color:white;padding:4px 14px;
                  border-radius:20px;font-size:13px;margin:0 4px}}
        .badge2 {{background:#107c10;color:white;padding:4px 14px;
                  border-radius:20px;font-size:13px;margin:0 4px}}
        .info  {{background:#e8f4fd;padding:10px 16px;border-radius:6px;
                 font-size:13px;margin-bottom:14px;display:flex;
                 gap:20px;flex-wrap:wrap;align-items:center}}
        table  {{width:100%;border-collapse:collapse;background:white;
                 border-radius:8px;overflow:hidden;font-size:13px;
                 box-shadow:0 2px 8px rgba(0,0,0,.09)}}
        th     {{background:#0078d4;color:white;padding:11px 12px;
                 text-align:left;font-weight:600}}
        td     {{padding:9px 12px;border-bottom:1px solid #eee}}
        tr:hover{{filter:brightness(.97)}}
        .foot  {{color:#aaa;font-size:12px;text-align:center;margin-top:14px}}
    </style></head>
    <body>
    <h1>🏦 Retail Banking Portfolio Dashboard</h1>
    <div class='arch'><b>Live Architecture:</b> &nbsp;
        📱 Browser &nbsp;→&nbsp;
        <span class='badge'>⚙️ AKS Pod</span> &nbsp;→&nbsp;
        <span class='badge'>🧠 Synapse SQL</span> &nbsp;→&nbsp;
        <span class='badge2'>💾 ADLS Gen2</span>
    </div>
    <div class='info'>
        <span>⚙️ Pod: <b>{pod}</b></span>
        <span style='color:{color}'><b>{label}</b></span>
        <span>📊 <b>{len(rows)}</b> rows</span>
        <span>🔄 30s refresh</span>
    </div>
    {ebox}{nav}
    <table><thead><tr>
        <th>Customer ID</th><th>Name</th><th>Segment</th>
        <th>Risk Profile</th><th>Risk Category</th><th>Risk Score</th>
        <th>Portfolio Value</th><th>Ticker</th><th>Qty</th>
        <th>Price</th><th>P&L%</th><th>Unrealized P&L</th>
    </tr></thead>
    <tbody>{build_table(rows)}</tbody></table>
    <p class='foot'>✅ AKS → dbo.vw_customer_portfolio_dashboard
    → datahulkadlssws/raw</p>
    </body></html>"""

# ── Routes ────────────────────────────────────────────────────
@app.route("/health")
def health():
    return jsonify({
        "status":"ok","version":"final",
        "pod":os.environ.get("HOSTNAME","local")
    })

@app.route("/data")
def data():
    try:
        rows = query_synapse()
        return jsonify({"source":"Synapse Live",
                        "view":"vw_customer_portfolio_dashboard",
                        "row_count":len(rows),"rows":rows})
    except Exception as e:
        return jsonify({"source":"mock","error":str(e),
                        "row_count":len(MOCK),"rows":MOCK})

@app.route("/")
def dashboard():
    pod = os.environ.get("HOSTNAME","local")
    try:
        rows = query_synapse()
        return build_page(rows,"Banking Dashboard",
            "🟢 LIVE — Azure Synapse Analytics","#107c10",pod)
    except Exception as e:
        return build_page(MOCK,"Banking Dashboard",
            "🟡 Mock — Synapse connecting...","#d83b01",pod,str(e))

@app.route("/high-risk")
def high_risk():
    pod = os.environ.get("HOSTNAME","local")
    try:
        rows = query_synapse("risk_category = 'High'")
        return build_page(rows,"High Risk",
            "🔴 HIGH RISK — Synapse Live","#d83b01",pod)
    except Exception as e:
        m = [r for r in MOCK if r["risk_category"]=="High"]
        return build_page(m,"High Risk",
            "🟡 Mock — High Risk","#d83b01",pod,str(e))

@app.route("/top-portfolios")
def top_portfolios():
    pod = os.environ.get("HOSTNAME","local")
    try:
        rows = query_synapse()
        return build_page(rows,"Top Portfolios",
            "💰 Top Portfolios — Synapse Live","#107c10",pod)
    except Exception as e:
        m = sorted(MOCK,key=lambda x:x["portfolio_value_usd"],reverse=True)
        return build_page(m,"Top Portfolios",
            "🟡 Mock — Top Portfolios","#d83b01",pod,str(e))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)