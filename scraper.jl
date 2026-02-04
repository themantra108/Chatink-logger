using ChromeDevToolsLite, Dates, JSON, DataFrames, CSV

# ==============================================================================
# 1. 🧱 CONFIGURATION
# ==============================================================================
const TARGET_URLS = [
    "https://chartink.com/dashboard/208896",  # Stocks/Sectors
    "https://chartink.com/dashboard/419640"   # Market Condition
]
const OUTPUT_ROOT = "chartink_data"

# Navigation Safety
const NAV_SLEEP_SEC = 5
const MAX_WAIT_CYCLES = 60
const SCROLL_STEP = 2500
const SCROLL_SLEEP = 0.1

# Regex & Map
const DATE_REGEX = r"(\d+)(?:st|nd|rd|th)?\s+([a-zA-Z]+)"
const MONTH_MAP = Dict("Jan"=>1, "Feb"=>2, "Mar"=>3, "Apr"=>4, "May"=>5, "Jun"=>6,
                       "Jul"=>7, "Aug"=>8, "Sep"=>9, "Oct"=>10, "Nov"=>11, "Dec"=>12)

get_ist() = now(Dates.UTC) + Hour(5) + Minute(30)

# ==============================================================================
# 2. 🧠 LOGIC KERNELS
# ==============================================================================

# Core Date Inference (Pure Function)
function infer_date(raw_str::AbstractString, ref_date::Date)
    m = match(DATE_REGEX, raw_str)
    if isnothing(m); return missing; end
    day, mon = parse(Int, m.captures[1]), MONTH_MAP[titlecase(m.captures[2])[1:3]]
    year_val = year(ref_date)
    ref_mon = month(ref_date)
    
    # Year Rollback Logic (Jan -> Dec)
    if mon > (ref_mon + 6); year_val -= 1; elseif mon < (ref_mon - 6); year_val += 1; end
    
    try
        cand = Date(year_val, mon, day)
        # Future Guard
        return cand > (ref_date + Day(2)) ? Date(year_val - 1, mon, day) : cand
    catch; return missing; end
end

# Strategy Types
abstract type UpdateStrategy end
struct Snapshot <: UpdateStrategy end   # Block Replacement
struct SmartDiff <: UpdateStrategy end  # Row Diffing

struct WidgetTable{T <: UpdateStrategy}
    name::String
    clean_name::String
    data::DataFrame
    subfolder::String
    strategy::T 
end

detect_strategy(df) = "Date" in names(df) ? SmartDiff() : Snapshot()

# Enrichment
function enrich!(df::DataFrame, ::SmartDiff)
    ref = Date(get_ist())
    transform!(df, :Date => ByRow(d -> infer_date(string(d), ref)) => :Full_Date)
end

function enrich!(df::DataFrame, ::Snapshot)
    if "Timestamp" in names(df)
        transform!(df, :Timestamp => ByRow(t -> Date(t)) => :Scan_Date)
    end
end

is_junk(df) = isempty(df) || ("Col_1" in names(df) && occursin(r"Clause|\*", string(df[1,1])))

# ==============================================================================
# 3. 💾 SAVING LOGIC (The Anti-Join Upgrade) 📉
# ==============================================================================

function save_to_disk(w::WidgetTable, final_df::DataFrame)
    folder_path = joinpath(OUTPUT_ROOT, w.subfolder)
    mkpath(folder_path)
    path = joinpath(folder_path, w.clean_name * ".csv")
    
    sort!(final_df, :Timestamp, rev=true)
    CSV.write(path, final_df)
    @info "  💾 Saved: [$(w.subfolder)] -> $(w.clean_name)"
end

function save_widget(w::WidgetTable)
    path = joinpath(OUTPUT_ROOT, w.subfolder, w.clean_name * ".csv")
    
    if isfile(path)
        old_df = CSV.read(path, DataFrame)
        apply_strategy(w.strategy, w, old_df)
    else
        save_to_disk(w, w.data)
    end
end

# STRATEGY 1: SNAPSHOT (Anti-Join on Scan_Date)
function apply_strategy(::Snapshot, w, old_df)
    if "Scan_Date" in names(w.data)
        # "Give me rows from OLD history that are NOT in the NEW batch's dates"
        # This deletes the stale version of today/yesterday instantly.
        history_kept = antijoin(old_df, w.data, on=:Scan_Date)
        save_to_disk(w, vcat(w.data, history_kept, cols=:union))
    else
        save_to_disk(w, vcat(w.data, old_df, cols=:union))
    end
end

# STRATEGY 2: SMART DIFF (Anti-Join on Content)
function apply_strategy(::SmartDiff, w, old_df)
    # Define Content Columns (Ignore Metadata)
    ignore = ["Timestamp", "Full_Date", "Scan_Date"]
    common_cols = intersect(names(w.data), names(old_df))
    compare_cols = setdiff(common_cols, ignore)
    
    # "Give me rows from NEW batch that do NOT exist in OLD history (content-wise)"
    # This automatically finds changed rows or new dates.
    updates = antijoin(w.data, old_df, on=compare_cols)
    
    if isempty(updates); return; end # No changes? Skip write.
    
    combined = vcat(updates, old_df, cols=:union)
    
    # Dedupe: If multiple rows exist for the same logical "Date" (e.g., "2nd Feb"),
    # we want the LATEST one.
    sort!(combined, :Timestamp, rev=true)
    unique!(combined, :Date) 
    
    save_to_disk(w, combined)
end

# ==============================================================================
# 4. 🌐 BROWSER INTERACTION
# ==============================================================================

function wait_for_tables(page)
    for _ in 1:MAX_WAIT_CYCLES
        res = ChromeDevToolsLite.evaluate(page, "document.querySelectorAll('table').length > 0")
        val = isa(res, Dict) ? res["value"] : res
        if val == true; return true; end
        sleep(1)
    end
    return false
end

function scroll_page(page)
    h = ChromeDevToolsLite.evaluate(page, "document.body.scrollHeight")
    h_val = isa(h, Dict) ? h["value"] : h
    h_int = isa(h_val, Number) ? h_val : 5000
    for s in 0:SCROLL_STEP:h_int
        ChromeDevToolsLite.evaluate(page, "window.scrollTo(0, $s)")
        sleep(SCROLL_SLEEP)
    end
    sleep(3)
end

const JS_PAYLOAD = """
(() => {
    try {
        let out = [];
        const cln = (t) => t ? t.trim().replace(/"/g, '""').replace(/\\n/g, " ") : "";
        
        const scanTable = (tbl, forcedName) => {
            let name = forcedName;
            if (!name) { // Auto-name climbing
                let curr = tbl, d = 0;
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
            if (!name) name = "Unknown Widget";
            
            const rows = tbl.querySelectorAll("tr");
            if (!rows.length) return;
            rows.forEach(r => {
                if (r.innerText.includes("No data")) return;
                const cells = Array.from(r.querySelectorAll("th, td"));
                if (!cells.length) return;
                const line = cells.map(c => '"' + cln(c.innerText) + '"').join(",");
                out.push('"' + cln(name) + '",' + line);
            });
        };

        // 1. Standard Tables
        document.querySelectorAll("table, div.dataTables_wrapper").forEach(n => {
            if (n.tagName === "TABLE") scanTable(n);
        });

        // 2. Card Scanner (Market Breadth Fix)
        document.querySelectorAll("div.card").forEach(c => {
            const h = c.querySelector(".card-header, h1, h2, h3, h4");
            const t = c.querySelector("table");
            if (h && t) scanTable(t, "MANUAL_CATCH_" + cln(h.innerText));
        });

        window._data = [...new Set(out)].join("\\n");
        return "DONE";
    } catch(e) { return "ERR:" + e; }
})()
"""

function extract_and_parse(page, folder_name) :: Vector{WidgetTable}
    @info "⚡ Extracting..."
    ChromeDevToolsLite.evaluate(page, "eval($(JSON.json(JS_PAYLOAD)))")
    
    len_res = ChromeDevToolsLite.evaluate(page, "window._data ? window._data.length : 0")
    len_val = isa(len_res, Dict) ? len_res["value"] : len_res
    len = try parse(Int, string(len_val)) catch; 0 end
    
    if len == 0; return WidgetTable[]; end
    
    buf = IOBuffer()
    for i in 0:50000:len
        chunk = ChromeDevToolsLite.evaluate(page, "window._data.substring($i, $(min(i+50000, len)))")
        val = isa(chunk, Dict) ? chunk["value"] : chunk
        print(buf, val)
    end
    
    full_csv = String(take!(buf))
    widgets = WidgetTable[]
    groups = Dict{String, Vector{String}}()
    
    for line in eachline(IOBuffer(full_csv))
        length(line)<5 && continue
        m = match(r"^\"([^\"]+)\"", line)
        key = isnothing(m) ? "Unknown" : replace(m.captures[1], "MANUAL_CATCH_" => "")
        push!(get!(groups, key, String[]), line)
    end

    ts = get_ist()
    for (name, rows) in groups
        h_idx = findfirst(l -> occursin(r"Symbol|Name|Scan Name|Date", l), rows)
        if isnothing(h_idx); continue; end
        
        io = IOBuffer()
        println(io, "Timestamp," * replace(rows[h_idx], r"^\"[^\"]+\"," => "")) 
        cols = length(split(rows[h_idx], "\",\""))
        
        for i in (h_idx+1):length(rows)
             if abs(length(split(rows[i], "\",\"")) - cols) <= 2 && !occursin(r"Symbol|Name|Date", rows[i])
                 println(io, "$ts," * replace(rows[i], r"^\"[^\"]+\"," => ""))
             end
        end
        
        seekstart(io)
        try
            df = CSV.read(io, DataFrame; strict=false, silencewarnings=true)
            if is_junk(df); continue; end
            
            strat = detect_strategy(df)
            enrich!(df, strat)
            
            clean = replace(name, r"[^a-zA-Z0-9]" => "_")[1:min(end,50)]
            push!(widgets, WidgetTable(clean, df, folder_name, strat))
        catch; end
    end
    return widgets
end

function get_dashboard_name(page)
    raw_title = ChromeDevToolsLite.evaluate(page, "document.title") 
    val = isa(raw_title, Dict) ? raw_title["value"] : raw_title
    if isnothing(val) || val == ""; return "Unknown_Dashboard"; end
    
    clean_title = replace(val, " - Chartink.com" => "") |> 
                  x -> replace(x, " - Chartink" => "") |>
                  x -> replace(x, r"[^a-zA-Z0-9 \-_]" => "") |> 
                  x -> replace(strip(x), " " => "_")
    return isempty(clean_title) ? "Dashboard_Unknown" : clean_title
end

# ==============================================================================
# 5. 🚀 MAIN PIPELINE
# ==============================================================================

function process_url(page, url)
    @info "🧭 Navigating to: $url"
    ChromeDevToolsLite.evaluate(page, "window._data = null;")
    retry_nav = retry(() -> ChromeDevToolsLite.goto(page, url), delays=[2.0, 5.0, 10.0])
    try; retry_nav(); catch; @error "Failed to load $url"; return WidgetTable[]; end
    
    sleep(NAV_SLEEP_SEC)
    folder_name = get_dashboard_name(page)
    @info "🏷️ Dashboard: $folder_name"
    
    wait_for_tables(page)
    scroll_page(page)
    
    return extract_and_parse(page, folder_name)
end

function main()
    mkpath(OUTPUT_ROOT)
    try
        @info "🔌 Connecting to Chrome..."
        page = ChromeDevToolsLite.connect_browser()
        
        @sync begin
            for url in TARGET_URLS
                @info "--- [TARGET] $url ---"
                widgets = process_url(page, url)
                
                if !isempty(widgets)
                    for w in widgets
                        @async save_widget(w)
                    end
                else
                    @warn "⚠️ No widgets found for $url"
                end
            end
        end
        @info "🎉 Scrape Cycle Complete."
    catch e
        @error "Crash" exception=(e, catch_backtrace())
        exit(1)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__; main(); end
