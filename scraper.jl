using ChromeDevToolsLite, Dates, JSON, DataFrames, CSV

# ==============================================================================
# 1. CONFIG & UTILS
# ==============================================================================
const TARGET_URLS = [
    "https://chartink.com/dashboard/208896",
    "https://chartink.com/dashboard/419640"
]
const OUTPUT_ROOT = "chartink_data"

# Navigation
const NAV_SLEEP = 5
const MAX_WAIT = 60
const SCROLL_CFG = (step=2500, sleep=0.1)

# Date Parsing
const DATE_REGEX = r"(\d+)(?:st|nd|rd|th)?\s+([a-zA-Z]+)"
const MONTH_MAP = Dict("Jan"=>1, "Feb"=>2, "Mar"=>3, "Apr"=>4, "May"=>5, "Jun"=>6,
                       "Jul"=>7, "Aug"=>8, "Sep"=>9, "Oct"=>10, "Nov"=>11, "Dec"=>12)

get_ist() = now(Dates.UTC) + Hour(5) + Minute(30)

# ==============================================================================
# 2. LOGIC KERNELS (Pure Functions)
# ==============================================================================

# 🧠 Core Date Logic (Decoupled from DataFrame loop)
function infer_date(raw_str::AbstractString, ref_date::Date)
    m = match(DATE_REGEX, raw_str)
    if isnothing(m); return missing; end
    
    day, mon = parse(Int, m.captures[1]), MONTH_MAP[titlecase(m.captures[2])[1:3]]
    year_val = year(ref_date)
    
    # Year Rollback Logic
    ref_mon = month(ref_date)
    if mon > (ref_mon + 6); year_val -= 1;
    elseif mon < (ref_mon - 6); year_val += 1; end
    
    try
        cand = Date(year_val, mon, day)
        # Future Guard: If > Today+2d, assume previous year
        return cand > (ref_date + Day(2)) ? Date(year_val - 1, mon, day) : cand
    catch; return missing; end
end

# ==============================================================================
# 3. STRATEGIES (New Built-in Features)
# ==============================================================================
abstract type Strategy end
struct Snapshot <: Strategy end   # Block Replacement
struct SmartDiff <: Strategy end  # Row Diffing

struct WidgetTable{T<:Strategy}
    clean_name::String
    data::DataFrame
    subfolder::String
    strategy::T 
end

# Dispatcher
detect_strategy(df) = "Date" in names(df) ? SmartDiff() : Snapshot()

# Enrichment (Using 'transform!' instead of loops)
function enrich!(df::DataFrame, ::SmartDiff)
    ref = Date(get_ist())
    transform!(df, :Date => ByRow(d -> infer_date(string(d), ref)) => :Full_Date)
end

function enrich!(df::DataFrame, ::Snapshot)
    if "Timestamp" in names(df)
        transform!(df, :Timestamp => ByRow(t -> Date(t)) => :Scan_Date)
    end
end

# Filter Junk
is_junk(df) = isempty(df) || ("Col_1" in names(df) && occursin(r"Clause|\*", string(df[1,1])))

# ==============================================================================
# 4. SAVING (The Anti-Join Magic ✨)
# ==============================================================================

function save_widget(w::WidgetTable)
    dir = joinpath(OUTPUT_ROOT, w.subfolder)
    mkpath(dir)
    path = joinpath(dir, w.clean_name * ".csv")
    
    if isfile(path)
        old_df = CSV.read(path, DataFrame)
        save_strategy(w.strategy, w.data, old_df, path)
    else
        write_csv(path, w.data)
    end
end

# 1. SNAPSHOT: Remove old rows that match new Scan_Dates
function save_strategy(::Snapshot, new_df, old_df, path)
    if "Scan_Date" in names(new_df)
        # anti-join: Keep rows in old_df that do NOT match Scan_Date in new_df
        history = antijoin(old_df, new_df, on=:Scan_Date)
        final = vcat(new_df, history, cols=:union)
        write_csv(path, final)
    else
        # Fallback for tables without dates
        write_csv(path, vcat(new_df, old_df, cols=:union))
    end
end

# 2. SMART DIFF: Only add rows where content differs
function save_strategy(::SmartDiff, new_df, old_df, path)
    # Compare only common columns, ignore metadata
    ignore = ["Timestamp", "Full_Date", "Scan_Date"]
    common_cols = intersect(names(new_df), names(old_df))
    compare_cols = setdiff(common_cols, ignore)
    
    # ✨ MAGIC: Find rows in new_df that don't exist in old_df (based on content)
    # This automatically handles "Unchanged Data" (returns empty)
    # and "Changed Data" (returns the new row with fresh Timestamp)
    updates = antijoin(new_df, old_df, on=compare_cols)
    
    if isempty(updates); return; end
    
    combined = vcat(updates, old_df, cols=:union)
    sort!(combined, :Timestamp, rev=true)
    unique!(combined, :Date) # Hard dedupe by Date text
    write_csv(path, combined)
end

function write_csv(path, df)
    sort!(df, :Timestamp, rev=true)
    CSV.write(path, df)
    @info "  💾 Saved: $(basename(dirname(path)))/$(basename(path))"
end

# ==============================================================================
# 5. BROWSER AUTOMATION
# ==============================================================================
const JS_EXTRACT = """
(() => {
    try {
        let out = [];
        const cln = (t) => t ? t.trim().replace(/"/g, '""').replace(/\\n/g, " ") : "";
        const scan = (nodes) => {
            nodes.forEach((n, i) => {
                let name = "Widget_" + i, curr = n, d = 0;
                while (curr && d++ < 12) {
                    let sib = curr.previousElementSibling;
                    while (sib) {
                        let txt = sib.innerText || "";
                        if (txt.length>2 && txt.length<150 && !/Loading|Error|Run/.test(txt)) {
                            name = txt.split('\\n')[0].trim(); break;
                        }
                        sib = sib.previousElementSibling;
                    }
                    if (!name.includes("Widget_")) break;
                    curr = curr.parentElement;
                }
                const rows = n.querySelectorAll("tr");
                if (!rows.length) return;
                rows.forEach(r => {
                    if (r.innerText.includes("No data")) return;
                    const cells = Array.from(r.querySelectorAll("th, td"));
                    if (!cells.length) return;
                    const line = cells.map(c => '"' + (r.querySelector("th") ? cln(c.innerText) : cln(c.innerText)) + '"').join(",");
                    out.push('"' + cln(name) + '",' + line);
                });
            });
        };
        scan(document.querySelectorAll("table, div.dataTables_wrapper"));
        // Manual Headers
        const heads = document.querySelectorAll("h1, h2, h3, h4, div.card-header");
        heads.forEach(h => {
            if (/Market|Condition|Breadth|Ratio/i.test(h.innerText)) {
                let c = h.nextElementSibling || h.parentElement.nextElementSibling;
                if (c && c.querySelector("table")) scan([c.querySelector("table")]);
            }
        });
        window._data = [...new Set(out)].join("\\n");
        return "DONE";
    } catch(e) { return "ERR:" + e; }
})()
"""

function process_url(page, url)
    @info "🧭 $url"
    ChromeDevToolsLite.evaluate(page, "window._data = null;")
    try retry(()->ChromeDevToolsLite.goto(page, url), delays=[2,5])() catch; return []; end
    
    sleep(NAV_SLEEP)
    
    # Determine Folder Name
    title_res = ChromeDevToolsLite.evaluate(page, "document.title")
    t_val = isa(title_res, Dict) ? title_res["value"] : title_res
    clean_t = replace(replace(t_val, " - Chartink.com" => ""), r"[^a-zA-Z0-9]" => "_")
    folder = isempty(clean_t) ? "Unknown" : clean_t
    @info "🏷️ $folder"

    # Wait & Scroll
    for _ in 1:MAX_WAIT
        if (ChromeDevToolsLite.evaluate(page, "document.querySelectorAll('table').length > 0") |> x->isa(x,Dict) ? x["value"] : x) == true; break; end
        sleep(1)
    end
    h = ChromeDevToolsLite.evaluate(page, "document.body.scrollHeight") |> x->isa(x,Dict) ? x["value"] : x
    for s in 0:SCROLL_CFG.step:(isa(h, Number) ? h : 5000); ChromeDevToolsLite.evaluate(page, "window.scrollTo(0, $s)"); sleep(SCROLL_CFG.sleep); end
    sleep(2)

    # Extract
    ChromeDevToolsLite.evaluate(page, "eval($(JSON.json(JS_EXTRACT)))")
    len_res = ChromeDevToolsLite.evaluate(page, "window._data ? window._data.length : 0") |> x->isa(x,Dict) ? x["value"] : x
    len = try parse(Int, string(len_res)) catch; 0 end
    if len == 0; return []; end

    buf = IOBuffer()
    for i in 0:50000:len
        chunk = ChromeDevToolsLite.evaluate(page, "window._data.substring($i, $(min(i+50000, len)))")
        print(buf, isa(chunk, Dict) ? chunk["value"] : chunk)
    end
    
    # Parse CSV Stream
    widgets = WidgetTable[]
    for line in eachline(IOBuffer(String(take!(buf))))
        length(line) < 5 && continue
        m = match(r"^\"([^\"]+)\"", line)
        if isnothing(m); continue; end
        name = m.captures[1]
        
        # Grouping Logic Inline
        # (Simplified: In a real stream we'd accumulate, but here we restart loop for clarity or use a dict)
        # For brevity in "Minimalist" version, we assume Grouping happened. 
        # *Restoring Grouping Logic for Correctness:*
    end

    # Correct Grouping Implementation
    raw_csv = String(take!(IOBuffer(String(take!(buf))))) # Reset buffer
    groups = Dict{String, Vector{String}}()
    for line in eachline(IOBuffer(raw_csv))
        length(line)<5 && continue
        m = match(r"^\"([^\"]+)\"", line)
        key = isnothing(m) ? "Unknown" : m.captures[1]
        push!(get!(groups, key, String[]), line)
    end

    ts = get_ist()
    for (name, rows) in groups
        # Header Heuristics
        h_idx = findfirst(l -> occursin(r"Symbol|Name|Scan Name|Date", l), rows)
        if isnothing(h_idx); continue; end
        
        # Reconstruct valid CSV for this widget
        io = IOBuffer()
        println(io, "Timestamp," * replace(rows[h_idx], r"^\"[^\"]+\"," => "")) # Header
        cols = length(split(rows[h_idx], "\",\""))
        
        for i in (h_idx+1):length(rows)
             # Valid Row Check
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
            push!(widgets, WidgetTable(clean, df, folder, strat))
        catch; end
    end
    return widgets
end

function main()
    try
        page = ChromeDevToolsLite.connect_browser()
        @sync for url in TARGET_URLS
            w = process_url(page, url)
            if !isempty(w); for x in w; @async save_widget(x); end; end
        end
        @info "🎉 Done."
    catch e; @error "Crash" exception=(e, catch_backtrace()); exit(1); end
end

if abspath(PROGRAM_FILE) == @__FILE__; main(); end