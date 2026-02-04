using ChromeDevToolsLite, Dates, JSON, DataFrames, CSV

# ==============================================================================
# 1. 🧱 CONFIGURATION
# ==============================================================================
const TARGET_URLS = [
    "https://chartink.com/dashboard/208896",
    "https://chartink.com/dashboard/419640"
]
const OUTPUT_ROOT = "chartink_data"

# NamedTuple for cleaner config passing
const CFG = (
    NAV_SLEEP   = 5,
    MAX_WAIT    = 60,
    SCROLL_STEP = 2500,
    SCROLL_WAIT = 0.1
)

const DATE_REGEX = r"(\d+)(?:st|nd|rd|th)?\s+([a-zA-Z]+)"
const MONTH_MAP = Dict("Jan"=>1, "Feb"=>2, "Mar"=>3, "Apr"=>4, "May"=>5, "Jun"=>6,
                       "Jul"=>7, "Aug"=>8, "Sep"=>9, "Oct"=>10, "Nov"=>11, "Dec"=>12)

get_ist() = now(Dates.UTC) + Hour(5) + Minute(30)

# ==============================================================================
# 2. 🧠 CORE LOGIC & TYPES
# ==============================================================================

abstract type UpdateStrategy end
struct Snapshot <: UpdateStrategy end
struct TimeSeries <: UpdateStrategy end

struct WidgetTable{T <: UpdateStrategy}
    name::String
    clean_name::String
    data::DataFrame
    subfolder::String
    strategy::T 
end

# 🛠️ Utility: Generic Waiter
function wait_until(predicate::Function; timeout=60, interval=1)
    start_time = time()
    while (time() - start_time) < timeout
        if predicate()
            return true
        end
        sleep(interval)
    end
    return false
end

# 📅 Date Logic
function parse_date_str(s)
    m = match(DATE_REGEX, s)
    isnothing(m) ? (0,0) : (parse(Int, m.captures[1]), MONTH_MAP[titlecase(m.captures[2])[1:3]])
end

function infer_full_date(row_date, ref_date)
    (day, mon) = parse_date_str(string(row_date))
    if day == 0; return missing; end
    
    y = year(ref_date)
    ref_mon = month(ref_date)
    
    # Year Logic
    if mon > (ref_mon + 6); y -= 1; elseif mon < (ref_mon - 6); y += 1; end
    
    try
        cand = Date(y, mon, day)
        return cand > (ref_date + Day(2)) ? Date(y-1, mon, day) : cand
    catch; return missing; end
end

# 🧩 Strategies & Enrichment
detect_strategy(df) = "Date" in names(df) ? TimeSeries() : Snapshot()

function enrich!(df::DataFrame, ::TimeSeries)
    ref = Date(get_ist())
    transform!(df, :Date => ByRow(d -> infer_full_date(d, ref)) => :Full_Date)
end

function enrich!(df::DataFrame, ::Snapshot)
    if "Timestamp" in names(df)
        transform!(df, :Timestamp => ByRow(t -> Date(t)) => :Scan_Date)
    end
end

is_junk(df) = isempty(df) || ("Col_1" in names(df) && occursin(r"Clause|\*", string(df[1,1])))

# ==============================================================================
# 3. 🏭 FUNCTIONAL PARSING PIPELINE (New!)
# ==============================================================================

"""
Step 1: Split raw CSV stream into dictionary of lines per widget.
"""
function parse_groups(raw_csv::String)
    groups = Dict{String, Vector{String}}()
    for line in eachline(IOBuffer(raw_csv))
        length(line) < 5 && continue
        m = match(r"^\"([^\"]+)\"", line)
        key = isnothing(m) ? "Unknown" : replace(m.captures[1], "MANUAL_CATCH_" => "")
        push!(get!(groups, key, String[]), line)
    end
    return groups
end

"""
Step 2: Convert a single group of lines into a WidgetTable (or nothing).
This isolates the messiness of CSV reconstruction.
"""
function build_widget(name::String, rows::Vector{String}, folder::String)
    # Find Header
    h_idx = findfirst(l -> occursin(r"Symbol|Name|Scan Name|Date", l), rows)
    isnothing(h_idx) && return nothing

    # Reconstruct Clean CSV
    io = IOBuffer()
    ts = get_ist()
    
    # Write Header
    raw_header = replace(rows[h_idx], r"^\"[^\"]+\"," => "")
    println(io, "Timestamp,", raw_header)
    expected_cols = length(split(rows[h_idx], "\",\""))

    # Write Body
    for i in (h_idx+1):length(rows)
        # Validation: Check column count & ensure it's not a repeated header
        if abs(length(split(rows[i], "\",\"")) - expected_cols) <= 2 && 
           !occursin(r"Symbol|Name|Date", rows[i])
            
            clean_row = replace(rows[i], r"^\"[^\"]+\"," => "")
            println(io, "$ts,", clean_row)
        end
    end

    # Create DataFrame
    seekstart(io)
    try
        df = CSV.read(io, DataFrame; strict=false, silencewarnings=true)
        if is_junk(df); return nothing; end
        
        strat = detect_strategy(df)
        enrich!(df, strat)
        
        clean_name = replace(name, r"[^a-zA-Z0-9]" => "_")[1:min(end,50)]
        return WidgetTable(name, clean_name, df, folder, strat)
    catch
        return nothing
    end
end

# ==============================================================================
# 4. 💾 SAVING LOGIC
# ==============================================================================

function save_to_disk(w::WidgetTable, df::DataFrame)
    path = joinpath(OUTPUT_ROOT, w.subfolder, w.clean_name * ".csv")
    mkpath(dirname(path))
    
    sort!(df, :Timestamp, rev=true)
    CSV.write(path, df)
    @info "  💾 Saved: [$(w.subfolder)] -> $(w.clean_name)"
end

# Dispatch: Snapshot
function save_widget(w::WidgetTable{Snapshot})
    path = joinpath(OUTPUT_ROOT, w.subfolder, w.clean_name * ".csv")
    if !isfile(path); save_to_disk(w, w.data); return; end

    old_df = CSV.read(path, DataFrame)
    if "Scan_Date" in names(w.data)
        # Remove old rows that match current Scan_Date
        dates = unique(dropmissing(w.data, :Scan_Date).Scan_Date)
        filter!(row -> ismissing(row.Scan_Date) || !(row.Scan_Date in dates), old_df)
    end
    save_to_disk(w, vcat(w.data, old_df, cols=:union))
end

# Dispatch: TimeSeries
function save_widget(w::WidgetTable{TimeSeries})
    path = joinpath(OUTPUT_ROOT, w.subfolder, w.clean_name * ".csv")
    if !isfile(path); save_to_disk(w, w.data); return; end

    old_df = CSV.read(path, DataFrame)
    
    # Helper: Content Check
    ignore = ["Timestamp", "Full_Date", "Scan_Date"]
    cols = setdiff(intersect(names(w.data), names(old_df)), ignore)
    
    # Create Set of hash(row) for fast lookup of existing data
    existing_hashes = Set(hash(row[cols]) for row in eachrow(old_df))
    
    # Filter: Keep new rows only if their content hash is new
    new_rows = filter(row -> !(hash(row[cols]) in existing_hashes), w.data)
    
    if !isempty(new_rows)
        combined = vcat(new_rows, old_df, cols=:union)
        unique!(combined, :Date)
        save_to_disk(w, combined)
    end
end

# ==============================================================================
# 5. 🌐 PIPELINE
# ==============================================================================

const JS_PAYLOAD = """
(() => {
    try {
        let out = [];
        const cln = (t) => t ? t.trim().replace(/"/g, '""').replace(/\\n/g, " ") : "";
        const scan = (nodes, forcedName) => {
            nodes.forEach((n, i) => {
                let name = forcedName;
                if (!name) { 
                    let curr = n, d = 0;
                    while (curr && d++ < 12) {
                        let sib = curr.previousElementSibling;
                        while (sib) {
                            let txt = sib.innerText || "";
                            if (txt.length>2 && txt.length<150 && !/Loading|Error|Run/.test(txt)) {
                                name = txt.split('\\n')[0].trim(); break;
                            }
                            sib = sib.previousElementSibling;
                        }
                        if (name) break;
                        curr = curr.parentElement;
                    }
                }
                if (!name) name = "Unknown Widget " + i;
                const rows = n.querySelectorAll("tr");
                if (!rows.length) return;
                rows.forEach(r => {
                    if (r.innerText.includes("No data")) return;
                    const cells = Array.from(r.querySelectorAll("th, td"));
                    if (!cells.length) return;
                    const isHead = r.querySelector("th") !== null;
                    const line = cells.map(c => '"' + (isHead?cln(c.innerText):cln(c.innerText)) + '"').join(",");
                    out.push('"' + cln(name) + '",' + line);
                });
            });
        };
        document.querySelectorAll("table, div.dataTables_wrapper").forEach(n => {
            if (n.tagName === "TABLE") scan([n]);
        });
        document.querySelectorAll("div.card").forEach(c => {
            const h = c.querySelector(".card-header, h1, h2, h3, h4");
            const t = c.querySelector("table");
            if (h && t) scan([t], "MANUAL_CATCH_" + cln(h.innerText));
        });
        window._data = [...new Set(out)].join("\\n");
        return "DONE";
    } catch(e) { return "ERR:" + e; }
})()
"""

function process_url(page, url)
    @info "🧭 Navigating: $url"
    ChromeDevToolsLite.evaluate(page, "window._data = null;")
    
    # 1. Navigate
    try retry(()->ChromeDevToolsLite.goto(page, url), delays=[2,5])() 
    catch; return []; end
    sleep(CFG.NAV_SLEEP)
    
    # 2. Identify
    title = ChromeDevToolsLite.evaluate(page, "document.title")["value"]
    folder = replace(replace(title, " - Chartink.com" => ""), r"[^a-zA-Z0-9]" => "_")
    @info "🏷️ Dashboard: $folder"
    
    # 3. Wait (using HOF)
    wait_until(() -> (ChromeDevToolsLite.evaluate(page, "document.querySelectorAll('table').length > 0")["value"] == true), 
               timeout=CFG.MAX_WAIT)

    # 4. Scroll
    h = ChromeDevToolsLite.evaluate(page, "document.body.scrollHeight")["value"]
    for s in 0:CFG.SCROLL_STEP:(isa(h, Number) ? h : 5000)
        ChromeDevToolsLite.evaluate(page, "window.scrollTo(0, $s)")
        sleep(CFG.SCROLL_WAIT)
    end
    sleep(2)

    # 5. Extract
    ChromeDevToolsLite.evaluate(page, "eval($(JSON.json(JS_PAYLOAD)))")
    len = try parse(Int, string(ChromeDevToolsLite.evaluate(page, "window._data ? window._data.length : 0")["value"])) catch; 0 end
    if len == 0; return []; end

    # 6. Stream
    buf = IOBuffer()
    for i in 0:50000:len
        print(buf, ChromeDevToolsLite.evaluate(page, "window._data.substring($i, $(min(i+50000, len)))")["value"])
    end
    
    # 7. Functional Parsing (Map/Filter)
    raw_csv = String(take!(buf))
    groups = parse_groups(raw_csv)
    
    # Look at this clean pipeline!
    widgets = collect(values(groups)) .|> 
              (lines -> build_widget("Unknown", lines, folder)) |> # Note: name is inside lines, handled by grouping key in real impl
              x -> filter(!isnothing, x)
              
    # Fix: parse_groups returns (name => lines), so we iterate pairs
    widgets = WidgetTable[]
    for (name, lines) in groups
        w = build_widget(name, lines, folder)
        !isnothing(w) && push!(widgets, w)
    end
    
    return widgets
end

function main()
    try
        page = ChromeDevToolsLite.connect_browser()
        @sync for url in TARGET_URLS
            # Process URL (Sync)
            widgets = process_url(page, url)
            # Save Widgets (Async)
            for w in widgets; @async save_widget(w); end
        end
        @info "🎉 Done."
    catch e
        @error "Crash" exception=(e, catch_backtrace())
    end
end

if abspath(PROGRAM_FILE) == @__FILE__; main(); end
