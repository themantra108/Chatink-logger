using ChromeDevToolsLite, Dates, JSON, DataFrames, CSV

# ==============================================================================
# 1. CONFIG
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

# Regex
const DATE_REGEX = r"(\d+)(?:st|nd|rd|th)?\s+([a-zA-Z]+)"
const MONTH_MAP = Dict("Jan"=>1, "Feb"=>2, "Mar"=>3, "Apr"=>4, "May"=>5, "Jun"=>6,
                       "Jul"=>7, "Aug"=>8, "Sep"=>9, "Oct"=>10, "Nov"=>11, "Dec"=>12)

get_ist() = now(Dates.UTC) + Hour(5) + Minute(30)

# ==============================================================================
# 2. LOGIC KERNELS
# ==============================================================================

function infer_date(raw_str::AbstractString, ref_date::Date)
    m = match(DATE_REGEX, raw_str)
    if isnothing(m); return missing; end
    day, mon = parse(Int, m.captures[1]), MONTH_MAP[titlecase(m.captures[2])[1:3]]
    year_val = year(ref_date)
    ref_mon = month(ref_date)
    if mon > (ref_mon + 6); year_val -= 1; elseif mon < (ref_mon - 6); year_val += 1; end
    try
        cand = Date(year_val, mon, day)
        return cand > (ref_date + Day(2)) ? Date(year_val - 1, mon, day) : cand
    catch; return missing; end
end

abstract type Strategy end
struct Snapshot <: Strategy end
struct SmartDiff <: Strategy end

struct WidgetTable{T<:Strategy}
    clean_name::String
    data::DataFrame
    subfolder::String
    strategy::T 
end

detect_strategy(df) = "Date" in names(df) ? SmartDiff() : Snapshot()

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
# 3. SAVING
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

function save_strategy(::Snapshot, new_df, old_df, path)
    if "Scan_Date" in names(new_df)
        history = antijoin(old_df, new_df, on=:Scan_Date)
        write_csv(path, vcat(new_df, history, cols=:union))
    else
        write_csv(path, vcat(new_df, old_df, cols=:union))
    end
end

function save_strategy(::SmartDiff, new_df, old_df, path)
    ignore = ["Timestamp", "Full_Date", "Scan_Date"]
    common = intersect(names(new_df), names(old_df))
    cols = setdiff(common, ignore)
    
    updates = antijoin(new_df, old_df, on=cols)
    if isempty(updates); return; end
    
    combined = vcat(updates, old_df, cols=:union)
    unique!(combined, :Date)
    write_csv(path, combined)
end

function write_csv(path, df)
    sort!(df, :Timestamp, rev=true)
    CSV.write(path, df)
    @info "  💾 Saved: $(basename(dirname(path)))/$(basename(path))"
end

# ==============================================================================
# 4. BROWSER AUTOMATION
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
    
    title_res = ChromeDevToolsLite.evaluate(page, "document.title")
    t_val = isa(title_res, Dict) ? title_res["value"] : title_res
    clean_t = replace(replace(t_val, " - Chartink.com" => ""), r"[^a-zA-Z0-9]" => "_")
    folder = isempty(clean_t) ? "Unknown" : clean_t
    @info "🏷️ $folder"

    for _ in 1:MAX_WAIT
        if (ChromeDevToolsLite.evaluate(page, "document.querySelectorAll('table').length > 0") |> x->isa(x,Dict) ? x["value"] : x) == true; break; end
        sleep(1)
    end
    h = ChromeDevToolsLite.evaluate(page, "document.body.scrollHeight") |> x->isa(x,Dict) ? x["value"] : x
    for s in 0:SCROLL_CFG.step:(isa(h, Number) ? h : 5000); ChromeDevToolsLite.evaluate(page, "window.scrollTo(0, $s)"); sleep(SCROLL_CFG.sleep); end
    sleep(2)

    ChromeDevToolsLite.evaluate(page, "eval($(JSON.json(JS_EXTRACT)))")
    len_res = ChromeDevToolsLite.evaluate(page, "window._data ? window._data.length : 0") |> x->isa(x,Dict) ? x["value"] : x
    len = try parse(Int, string(len_res)) catch; 0 end
    
    @info "📊 JS found $(len) bytes of data." # Debug Log
    if len == 0; return []; end

    buf = IOBuffer()
    for i in 0:50000:len
        chunk = ChromeDevToolsLite.evaluate(page, "window._data.substring($i, $(min(i+50000, len)))")
        print(buf, isa(chunk, Dict) ? chunk["value"] : chunk)
    end
    
    # 🩹 FIX: Read to string ONCE
    full_csv_str = String(take!(buf))
    widgets = WidgetTable[]
    groups = Dict{String, Vector{String}}()
    
    for line in eachline(IOBuffer(full_csv_str))
        length(line)<5 && continue
        m = match(r"^\"([^\"]+)\"", line)
        key = isnothing(m) ? "Unknown" : m.captures[1]
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
            push!(widgets, WidgetTable(clean, df, folder, strat))
        catch; end
    end
    
    @info "📦 Parsed $(length(widgets)) valid widgets."
    return widgets
end

function main()
    mkpath(OUTPUT_ROOT)
    try
        @info "🔌 Connecting..."
        page = ChromeDevToolsLite.connect_browser()
        
        # ⚡ FIXED ASYNC STRUCTURE
        @sync begin
            for url in TARGET_URLS
                w = process_url(page, url)
                if !isempty(w)
                    for x in w
                        @async save_widget(x)
                    end
                else
                    @warn "⚠️ No widgets found for $url"
                end
            end
        end
        @info "🎉 Done. Data saved to: $(abspath(OUTPUT_ROOT))"
    catch e; @error "Crash" exception=(e, catch_backtrace()); exit(1); end
end

if abspath(PROGRAM_FILE) == @__FILE__; main(); end
