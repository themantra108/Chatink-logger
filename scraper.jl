using ChromeDevToolsLite, Dates, JSON, DataFrames, CSV

# ==============================================================================
# 1. ⚙️ CONFIGURATION
# ==============================================================================
const TARGET_URLS = [
    "https://chartink.com/dashboard/419640",
    "https://chartink.com/dashboard/208896"
]
const OUTPUT_ROOT = "chartink_data"
const NAV_SLEEP = 8
const SCROLL_STEP = 2000

# Helper for India Standard Time
get_ist() = now(Dates.UTC) + Hour(5) + Minute(30)

# ==============================================================================
# 2. 🧠 TYPE SYSTEM (Strategy Pattern)
# ==============================================================================
abstract type UpdateStrategy end
struct SnapshotStrategy <: UpdateStrategy end
struct TimeSeriesStrategy <: UpdateStrategy end

struct WidgetTable{T <: UpdateStrategy}
    name::String
    clean_name::String
    data::DataFrame
    subfolder::String
    strategy::T 
end

# ==============================================================================
# 3. 🕸️ SCRAPING LOGIC
# ==============================================================================
# ... (Browser interaction logic remains the same, just wrapped cleanly) ...

const JS_PAYLOAD = """
(() => {
    try {
        let out = [];
        const cln = (t) => t ? t.trim().replace(/"/g, '""').replace(/\\n/g, " ") : "";
        const cleanHeader = (txt) => txt ? txt.replace(/Sort table by[\\s\\S]*/i, "").trim() : "";
        
        const scan = (nodes, forcedName) => {
            nodes.forEach((n, i) => {
                let name = forcedName || "Unknown Widget " + i;
                const rows = n.querySelectorAll("tr");
                if (!rows.length) return;
                rows.forEach(r => {
                    if (r.innerText.includes("No data")) return;
                    const cells = Array.from(r.querySelectorAll("th, td"));
                    if (!cells.length) return;
                    const isHead = r.querySelector("th") !== null;
                    const line = cells.map(c => {
                        let val = isHead ? cleanHeader(c.innerText) : cln(c.innerText);
                        return '"' + val + '"';
                    }).join(",");
                    out.push('"' + cln(name) + '",' + line);
                });
            });
        };

        document.querySelectorAll("table, div.dataTables_wrapper").forEach(n => {
            if (n.tagName === "TABLE") scan([n]);
        });
        document.querySelectorAll("div.card").forEach(c => {
            const h = c.querySelector(".card-header, h1, h2, h3, h4, h5, h6");
            const t = c.querySelector("table");
            if (h && t) scan([t], "MANUAL_CATCH_" + cln(h.innerText));
        });
        return [...new Set(out)].join("\\n");
    } catch(e) { return "ERR:" + e; }
})()
"""

function process_url(page, url)
    @info "🧭 Navigating: $url"
    try
        ChromeDevToolsLite.goto(page, url)
        sleep(NAV_SLEEP)
        
        # Scroll to load lazy elements
        h = ChromeDevToolsLite.evaluate(page, "document.body.scrollHeight")
        h_val = isa(h, Dict) ? h["value"] : 5000
        for s in 0:SCROLL_STEP:h_val
            ChromeDevToolsLite.evaluate(page, "window.scrollTo(0, $s)")
            sleep(0.2)
        end
        sleep(2)
        
        # Extract Data
        res = ChromeDevToolsLite.evaluate(page, JS_PAYLOAD)
        raw_csv = isa(res, Dict) ? res["value"] : res
        
        # Parse logic (Simplified for brevity, assumes standard CSV parsing)
        # In production, paste the full parsing logic from previous chat here
        # keeping the strategy determination intact.
        return parse_widgets(raw_csv, get_dashboard_name(page))
    catch e
        @error "Failed to process $url: $e"
        return WidgetTable[]
    end
end

# ... (Insert Parsing & Saving Logic from previous correct version) ...

function main()
    mkpath(OUTPUT_ROOT)
    page = ChromeDevToolsLite.connect_browser()
    @sync for url in TARGET_URLS
        @async begin 
            widgets = process_url(page, url)
            foreach(save_widget, widgets)
        end
    end
end

if abspath(PROGRAM_FILE) == @__FILE__; main(); end