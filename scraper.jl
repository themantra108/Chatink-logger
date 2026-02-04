using ChromeDevToolsLite, Dates, JSON, DataFrames, CSV

# ==============================================================================
# 1. 🧱 CONFIGURATION & CONSTANTS
# ==============================================================================
const TARGET_URLS = [
    "https://chartink.com/dashboard/208896",  # Stocks/Sectors
    "https://chartink.com/dashboard/419640"   # Market Condition
]
const OUTPUT_ROOT = "chartink_data"

# Navigation Safety Settings
const NAV_SLEEP_SEC = 5
const MAX_WAIT_CYCLES = 60
const SCROLL_STEP = 2500
const SCROLL_SLEEP = 0.1

# Regex & Map (Pre-compiled for speed)
const DATE_REGEX = r"(\d+)(?:st|nd|rd|th)?\s+([a-zA-Z]+)"
const MONTH_MAP = Dict{SubString{String}, Int}(
    "Jan" => 1, "Feb" => 2, "Mar" => 3, "Apr" => 4, "May" => 5, "Jun" => 6,
    "Jul" => 7, "Aug" => 8, "Sep" => 9, "Oct" => 10, "Nov" => 11, "Dec" => 12
)

get_ist() = now(Dates.UTC) + Hour(5) + Minute(30)

# ==============================================================================
# 2. 🧠 TYPE SYSTEM & STRATEGIES
# ==============================================================================
abstract type UpdateStrategy end

"""
SnapshotStrategy: For lists that change completely (e.g., "Top Gainers").
Logic: Replaces/Merges rows for the current scan date.
"""
struct SnapshotStrategy <: UpdateStrategy end

"""
TimeSeriesStrategy: For history tracking (e.g., "Market Breadth").
Logic: Anti-Join Upsert. Only appends truly new rows.
"""
struct TimeSeriesStrategy <: UpdateStrategy end

struct WidgetTable{T <: UpdateStrategy}
    name::String
    clean_name::String
    data::DataFrame
    subfolder::String
    strategy::T 
end

# ==============================================================================
# 3. 📅 PARSING & LOGIC
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

# Enrichment: Time Series (Robust Year Inference)
function enrich_dataframe!(df::DataFrame, ::TimeSeriesStrategy)
    nrows = nrow(df)
    full_dates = Vector{Union{Date, Missing}}(missing, nrows)
    scrape_date = Date(get_ist())
    current_year = year(scrape_date)
    last_month = month(scrape_date) # Start with today's month
    
    date_col = df.Date
    
    @inbounds for i in 1:nrows
        raw_val = string(date_col[i])
        (day, mon) = parse_chartink_date(raw_val)
        if day == 0 || mon == 0; continue; end
        
        # Year Rollback Logic (Detect Jan -> Dec transition)
        if last_month < 3 && mon > 10
            current_year -= 1
        elseif last_month > 10 && mon < 3
            current_year += 1 
        end
        
        try
            cand = Date(current_year, mon, day)
            # Future Guard: If inferred date is > 2 days in future, rollback year
            if cand > (scrape_date + Day(2))
                 cand = Date(current_year - 1, mon, day)
                 current_year -= 1
            end
            full_dates[i] = cand
        catch; end
        last_month = mon
    end
    df[!, :Full_Date] = full_dates
end

# Enrichment: Snapshot (Scan_Date creation)
function enrich_dataframe!(df::DataFrame, ::SnapshotStrategy)
    if "Timestamp" in names(df)
        df[!, :Scan_Date] = Date.(df[!, :Timestamp])
    end
end

# 🔥 JUNK FILTER
function is_junk_widget(df::DataFrame)
    if nrow(df) == 0; return true; end
    cols = names(df)
    if "Col_1" in cols || "Col_2" in cols
        first_row_str = join(string.(values(df[1, :])), " ")
        if occursin("Clause", first_row_str) || occursin("*", first_row_str)
            return true
        end
    end
    return false
end

# ==============================================================================
# 4. 💾 SAVING LOGIC (The Julian Way)
# ==============================================================================

function save_to_disk(w::WidgetTable, final_df::DataFrame; append=false)
    folder_path = joinpath(OUTPUT_ROOT, w.subfolder)
    mkpath(folder_path)
    path = joinpath(folder_path, w.clean_name * ".csv")
    
    # Sort nicely if it's a new write
    if !append
        sort!(final_df, :Timestamp, rev=true)
    end
    
    CSV.write(path, final_df, append=append)
    mode_str = append ? "Appended" : "Saved"
    @info "  💾 $mode_str: [$(w.subfolder)] -> $(w.clean_name)"
end

# --- Snapshot Logic (Simple Merge) ---
function save_widget(w::WidgetTable{SnapshotStrategy})
    path = joinpath(OUTPUT_ROOT, w.subfolder, w.clean_name * ".csv")
    if !isfile(path); save_to_disk(w, w.data); return; end

    old_df = CSV.read(path, DataFrame)
    
    # Remove existing data for THIS Scan_Date to prevent duplicates
    if "Scan_Date" in names(w.data)
        active_dates = unique(dropmissing(w.data, :Scan_Date).Scan_Date)
        filter!(row -> ismissing(row.Scan_Date) || !(row.Scan_Date in active_dates), old_df)
    end
    
    # Union Merge
    save_to_disk(w, vcat(w.data, old_df, cols=:union))
end

# --- Time Series Logic (Anti-Join Optimization) ---
function save_widget(w::WidgetTable{TimeSeriesStrategy})
    path = joinpath(OUTPUT_ROOT, w.subfolder, w.clean_name * ".csv")
    
    # 1. No file? Write fresh.
    if !isfile(path)
        save_to_disk(w, w.data)
        return
    end

    # 2. Read Schema for comparison
    # We enforce date parsing to ensure the Join works correctly
    old_df = CSV.read(path, DataFrame, types=Dict(:Full_Date => Date))
    new_df = w.data
    
    # 3. Define Composite Keys (The "Year Bug" Fix)
    # If we have a Symbol, we need (Date + Symbol) to be unique.
    # If not (market breadth), just (Date) is unique.
    keys = "Symbol" in names(new_df) ? [:Full_Date, :Symbol] : [:Full_Date]
    
    # Sanity Check: Schema mismatch protection
    if !all(k -> k in names(new_df), keys) || !all(k -> k in names(old_df), keys)
        @warn "  ⚠️ Schema mismatch for $(w.clean_name). Overwriting file safely."
        save_to_disk(w, new_df)
        return
    end

    # 4. The Anti-Join: "Give me rows in NEW that are NOT in OLD"
    rows_to_add = antijoin(new_df, old_df, on = keys)
    
    if nrow(rows_to_add) > 0
        @info "  ✨ Found $(nrow(rows_to_add)) new records."
        # Append ONLY the new rows (High Performance)
        save_to_disk(w, rows_to_add, append=true)
    else
        @info "  💤 No new data for $(w.clean_name)"
    end
end

# ==============================================================================
# 5. 🌐 BROWSER INTERACTION
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
    sleep(2)
end

# Robust JS Payload (Resistant to DOM changes)
const JS_PAYLOAD = """
(() => {
    try {
        let output = [];
        const cleanBody = (txt) => txt ? txt.trim().replace(/"/g, '""').replace(/\\n/g, " ") : "";
        const cleanHeader = (txt) => txt ? txt.replace(/Sort table by.*/gi, "").trim().replace(/"/g, '""').replace(/\\n/g, " ") : "";
        
        const nodes = document.querySelectorAll("table, div.dataTables_wrapper");
        nodes.forEach((node, i) => {
            let name = "Unknown Widget " + i;
            // Attempt 1: Look for previous header
            let curr = node, depth = 0;
            while (curr && depth++ < 12) {
                let sib = curr.previousElementSibling;
                while (sib) {
                    let txt = sib.innerText ? sib.innerText.trim() : "";
                    if (txt.length > 2 && txt.length < 150 && !/Loading|Error|Run Scan/.test(txt)) {
                        name = txt.split('\\n')[0].trim(); break;
                    }
                    sib = sib.previousElementSibling;
                }
                if (!name.includes("Unknown")) break;
                curr = curr.parentElement;
            }
            
            // Extract Rows
            let rows = (node.tagName === "TABLE") ? node.querySelectorAll("tr") : node.querySelectorAll("table tr");
            if (!rows.length) return;
            Array.from(rows).forEach(row => {
                if (row.innerText.includes("No data")) return;
                const cells = Array.from(row.querySelectorAll("th, td"));
                if (!cells.length) return;
                const isHeader = row.querySelector("th") !== null;
                let line = cells.map(c => '"' + (isHeader ? cleanHeader(c.innerText) : cleanBody(c.innerText)) + '"').join(",");
                output.push('"' + cleanBody(name) + '",' + line);
            });
        });

        // Attempt 2: Catch "Card" style widgets
        const headings = document.querySelectorAll("h1, h2, h3, h4, h5, h6, div.card-header");
        headings.forEach(h => {
            let title = h.innerText.trim();
            if (/Market|Condition|Breadth|Ratio|Indicator|Scan/i.test(title)) {
                let container = h.nextElementSibling;
                if (!container && h.parentElement) container = h.parentElement.nextElementSibling;
                if (container) {
                    let table = container.querySelector("table");
                    if (table) {
                        Array.from(table.querySelectorAll("tr")).forEach(row => {
                             let cells = row.querySelectorAll("td, th");
                             if (cells.length > 0) {
                                 let line = Array.from(cells).map(c => '"' + (c.tagName==="TH" ? cleanHeader(c.innerText) : cleanBody(c.innerText)) + '"').join(",");
                                 output.push('"MANUAL_CATCH_' + cleanBody(title) + '",' + line);
                             }
                        });
                    }
                }
            }
        });
        window._data = [...new Set(output)].join("\\n");
        return "DONE";
    } catch (e) { return "ERROR: " + e.toString(); }
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
    
    raw_csv = String(take!(buf))
    widgets = WidgetTable[]
    groups = Dict{String, Vector{String}}()
    
    # Simple CSV Line Splitter
    for line in eachline(IOBuffer(raw_csv))
        if length(line) < 5 || !startswith(line, "\""); continue; end
        m = match(r"^\"([^\"]+)\"", line)
        key = isnothing(m) ? "Unknown" : replace(m.captures[1], "MANUAL_CATCH_" => "")
        push!(get!(groups, key, String[]), line)
    end

    for (name, rows) in groups
        clean_name = replace(name, r"[^a-zA-Z0-9]" => "_")[1:min(end,50)]
        header_idx = findfirst(l -> occursin(r"\",\"(Symbol|Name|Scan Name|Date)\"", l), rows)
        io = IOBuffer()
        start_row, expected_cols = 1, 0
        
        if !isnothing(header_idx)
            raw_header = replace(rows[header_idx], r"^\"[^\"]+\"," => "")
            println(io, "\"Timestamp\"," * raw_header)
            start_row = header_idx + 1
            expected_cols = length(split(rows[header_idx], "\",\""))
        else
            cols_count = length(split(replace(rows[1], r"^\"[^\"]+\"," => ""), "\",\""))
            println(io, "\"Timestamp\"," * join(["\"Col_$i\"" for i in 1:cols_count], ","))
            expected_cols = cols_count + 1
        end
        
        current_ts = get_ist()
        valid_count = 0
        for i in start_row:length(rows)
            # Basic validation: Column count must match (+/- 2 tolerance)
            if abs(length(split(rows[i], "\",\"")) - expected_cols) > 2; continue; end
            
            clean_row = replace(rows[i], r"^\"[^\"]+\"," => "")
            # Skip rows that look like repeated headers
            if !occursin(r"\"(Symbol|Name|Date)\"", clean_row)
                 println(io, "\"$current_ts\"," * clean_row)
                 valid_count += 1
            end
        end
        
        if valid_count > 0
            seekstart(io)
            try
                df = CSV.read(io, DataFrame; strict=false, silencewarnings=true)
                
                # JUNK FILTER
                if is_junk_widget(df)
                    @info "  🗑️ Skipped Junk Widget: $clean_name"
                    continue 
                end
                
                strat = determine_strategy(df)
                enrich_dataframe!(df, strat)
                push!(widgets, WidgetTable(name, clean_name, df, folder_name, strat))
            catch e
                @warn "Failed to parse CSV for $clean_name: $e"
            end
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
# 6. 🚀 MAIN PIPELINE
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
        # Ensure Chrome is running: google-chrome --remote-debugging-port=9222
        page = ChromeDevToolsLite.connect_browser()
        
        @sync begin
            for url in TARGET_URLS
                @info "--- [TARGET] $url ---"
                widgets = process_url(page, url)
                
                if !isempty(widgets)
                    for w in widgets
                        # Async Save
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
