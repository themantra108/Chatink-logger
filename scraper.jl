using ChromeDevToolsLite, Dates, JSON, DataFrames, CSV

# ==============================================================================
# 1. 🧱 CONFIGURATION & CONSTANTS
# ==============================================================================
const TARGET_URLS = [
    "https://chartink.com/dashboard/208896",  # Stocks/Sectors
    "https://chartink.com/dashboard/419640"   # Market Condition
]
const OUTPUT_ROOT = "chartink_data"

const NAV_SLEEP_SEC = 8 
const MAX_WAIT_CYCLES = 60
const SCROLL_STEP = 2500
const SCROLL_SLEEP = 0.2

const DATE_REGEX = r"(\d+)(?:st|nd|rd|th)?\s+([a-zA-Z]+)"
const MONTH_MAP = Dict("Jan"=>1, "Feb"=>2, "Mar"=>3, "Apr"=>4, "May"=>5, "Jun"=>6,
                       "Jul"=>7, "Aug"=>8, "Sep"=>9, "Oct"=>10, "Nov"=>11, "Dec"=>12)

get_ist() = now(Dates.UTC) + Hour(5) + Minute(30)

# ==============================================================================
# 2. 🧠 TYPE SYSTEM
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
# 3. 📅 PARSING LOGIC
# ==============================================================================

function parse_chartink_date(date_str::AbstractString)
    m = match(DATE_REGEX, date_str)
    if isnothing(m); return (0, 0); end
    day = parse(Int, m.captures[1])
    mon = get(MONTH_MAP, titlecase(m.captures[2])[1:3], 0)
    return (day, mon)
end

function determine_strategy(df::DataFrame)
    return "Date" in names(df) ? TimeSeriesStrategy() : SnapshotStrategy()
end

function enrich_dataframe!(df::DataFrame, ::TimeSeriesStrategy)
    nrows = nrow(df)
    full_dates = Vector{Union{Date, Missing}}(missing, nrows)
    scrape_date = Date(get_ist())
    current_year = year(scrape_date)
    last_month = month(scrape_date)
    date_col = df.Date
    
    @inbounds for i in 1:nrows
        raw_val = string(date_col[i])
        (day, mon) = parse_chartink_date(raw_val)
        if day == 0 || mon == 0; continue; end
        if last_month < 3 && mon > 10; current_year -= 1;
        elseif last_month > 10 && mon < 3; current_year += 1; end
        try
            cand = Date(current_year, mon, day)
            if cand > (scrape_date + Day(2)); cand = Date(current_year - 1, mon, day); current_year -= 1; end
            full_dates[i] = cand
        catch; end
        last_month = mon
    end
    df[!, :Full_Date] = full_dates
end

function enrich_dataframe!(df::DataFrame, ::SnapshotStrategy)
    if "Timestamp" in names(df)
        df[!, :Scan_Date] = Date.(df[!, :Timestamp])
    end
end

function is_junk_widget(df::DataFrame)
    if nrow(df) == 0; return true; end
    if "Col_1" in names(df)
        val = string(df[1,1])
        return occursin("Clause", val) || occursin("*", val)
    end
    return false
end

# ==============================================================================
# 4. 💾 SAVING LOGIC
# ==============================================================================

function save_to_disk(w::WidgetTable, final_df::DataFrame)
    folder_path = joinpath(OUTPUT_ROOT, w.subfolder)
    mkpath(folder_path)
    path = joinpath(folder_path, w.clean_name * ".csv")
    sort!(final_df, :Timestamp, rev=true)
    CSV.write(path, final_df)
    @info "  💾 Saved: [$(w.subfolder)] -> $(w.clean_name)"
end

function save_widget(w::WidgetTable{SnapshotStrategy})
    path = joinpath(OUTPUT_ROOT, w.subfolder, w.clean_name * ".csv")
    if !isfile(path); save_to_disk(w, w.data); return; end
    old_df = CSV.read(path, DataFrame)
    if "Scan_Date" in names(w.data)
        active_dates = unique(dropmissing(w.data, :Scan_Date).Scan_Date)
        filter!(row -> ismissing(row.Scan_Date) || !(row.Scan_Date in active_dates), old_df)
    end
    save_to_disk(w, vcat(w.data, old_df, cols=:union))
end

function has_row_changed(new_row, old_row, check_cols)
    for col in check_cols
        val_new = hasproperty(new_row, col) ? new_row[col] : missing
        val_old = hasproperty(old_row, col) ? old_row[col] : missing
        if !isequal(val_new, val_old); return true; end
    end
    return false
end

function save_widget(w::WidgetTable{TimeSeriesStrategy})
    path = joinpath(OUTPUT_ROOT, w.subfolder, w.clean_name * ".csv")
    if !isfile(path); save_to_disk(w, w.data); return; end
    old_df = CSV.read(path, DataFrame)
    new_df = w.data
    meta_cols = ["Timestamp", "Full_Date", "Scan_Date"]
    content_cols = filter(n -> !(n in meta_cols), names(new_df))
    old_map = Dict{String, DataFrameRow}()
    for row in eachrow(old_df)
        d = string(row.Date)
        if !haskey(old_map, d); old_map[d] = row; end
    end
    rows_to_keep = Int[]
    for i in 1:nrow(new_df)
        new_row = new_df[i, :]
        d_key = string(new_row.Date)
        if !haskey(old_map, d_key) || has_row_changed(new_row, old_map[d_key], content_cols)
            push!(rows_to_keep, i)
        end
    end
    if isempty(rows_to_keep); return; end
    combined = vcat(new_df[rows_to_keep, :], old_df, cols=:union)
    sort!(combined, :Timestamp, rev=true)
    unique!(combined, :Date)
    save_to_disk(w, combined)
end

# ==============================================================================
# 5. 🌐 BROWSER INTERACTION
# ==============================================================================

function wait_for_data(page)
    for _ in 1:MAX_WAIT_CYCLES
        res = ChromeDevToolsLite.evaluate(page, "document.querySelectorAll('td').length > 10")
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

# Updated JS: Guarantees "WidgetName","Col1","Col2"... format
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
                    // Force Header detection
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

function extract_and_parse(page, folder_name) :: Vector{WidgetTable}
    len = 0
    for attempt in 1:3
        ChromeDevToolsLite.evaluate(page, "eval($(JSON.json(JS_PAYLOAD)))")
        sleep(0.5)
        len_res = ChromeDevToolsLite.evaluate(page, "window._data ? window._data.length : 0")
        len_val = isa(len_res, Dict) ? len_res["value"] : len_res
        len = try parse(Int, string(len_val)) catch; 0 end
        if len > 500; break; else; sleep(4); end
    end
    
    if len == 0; return WidgetTable[]; end
    
    buf = IOBuffer()
    for i in 0:50000:len
        chunk = ChromeDevToolsLite.evaluate(page, "window._data.substring($i, $(min(i+50000, len)))")
        val = isa(chunk, Dict) ? chunk["value"] : chunk
        print(buf, val)
    end
    
    raw_csv = String(take!(buf))
    widgets = WidgetTable[]
    groups = Dict{String, Vector{String}}()
    
    # 🔥 ROBUST PARSER (Fixes the Regex Issue)
    for line in eachline(IOBuffer(raw_csv))
        # Skip empty or malformed lines
        if length(line) < 5 || !occursin(",", line); continue; end
        
        # 1. Extract Widget Name (First Column)
        # We split by first comma, assuming "Name","Col1","Col2"...
        # We use a custom split logic to handle quotes safely
        quote_end = findnext("\"", line, 2)
        if isnothing(quote_end); continue; end
        
        raw_key = line[2:prevind(line, quote_end)] # Strip quotes
        key = replace(raw_key, "MANUAL_CATCH_" => "")
        
        # JUNK FILTER at Source: Skip "ATLAS" clause lines
        if key == "ATLAS" || occursin("Clause", line); continue; end
        
        push!(get!(groups, key, String[]), line)
    end
    
    @info "📊 Identified $(length(groups)) potential widgets."

    ts = get_ist()
    for (name, rows) in groups
        # Find Header Row (Must contain relevant keywords)
        h_idx = findfirst(l -> occursin(r"Symbol|Name|Scan Name|Date", l), rows)
        if isnothing(h_idx)
             @warn "  ⚠️ Skipping [$name]: No valid header found."
             continue
        end
        
        io = IOBuffer()
        # Remove the first column (Widget Name) from header and body
        # Because we want the CSV to start with "Timestamp,Symbol,..."
        
        # Helper to strip first col
        strip_first_col(l) = replace(l, r"^\"[^\"]+\"," => "")
        
        println(io, "Timestamp," * strip_first_col(rows[h_idx]))
        cols = length(split(rows[h_idx], "\",\""))
        
        for i in (h_idx+1):length(rows)
             # Basic column count check + ignore repeated headers
             if abs(length(split(rows[i], "\",\"")) - cols) <= 2 && !occursin(r"Symbol|Name|Date", rows[i])
                 println(io, "$ts," * strip_first_col(rows[i]))
             end
        end
        
        seekstart(io)
        try
            df = CSV.read(io, DataFrame; strict=false, silencewarnings=true)
            if is_junk_widget(df); continue; end
            strat = determine_strategy(df)
            enrich!(df, strat)
            clean = replace(name, r"[^a-zA-Z0-9]" => "_")[1:min(end,50)]
            push!(widgets, WidgetTable(name, clean, df, folder_name, strat))
        catch e
             @warn "  ❌ Failed to parse table [$name]: $e"
        end
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
    
    if !wait_for_data(page); @warn "  ⚠️ Timed out waiting for data."; end
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
                    for w in widgets; @async save_widget(w); end
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
