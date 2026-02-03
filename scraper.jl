using ChromeDevToolsLite, Dates, JSON, DataFrames, CSV

# ==============================================================================
# 1. 🧱 CONFIGURATION & CONSTANTS
# ==============================================================================
const TARGET_URLS = [
    "https://chartink.com/dashboard/208896",  # Stocks/Sectors
    "https://chartink.com/dashboard/419640"   # Market Condition
]
const OUTPUT_ROOT = "chartink_data"

const NAV_TIMEOUT_RETRIES = 3
const NAV_SLEEP_SEC = 5
const SCROLL_STEP = 2500
const SCROLL_SLEEP = 0.1

const DATE_REGEX = r"(\d+)(?:st|nd|rd|th)?\s+([a-zA-Z]+)"
const MONTH_MAP = Dict{SubString{String}, Int}(
    "Jan" => 1, "Feb" => 2, "Mar" => 3, "Apr" => 4, "May" => 5, "Jun" => 6,
    "Jul" => 7, "Aug" => 8, "Sep" => 9, "Oct" => 10, "Nov" => 11, "Dec" => 12
)

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

# Enrichment: Time Series (Smart Year Inference)
function enrich_dataframe!(df::DataFrame, ::TimeSeriesStrategy)
    nrows = nrow(df)
    full_dates = Vector{Union{Date, Missing}}(missing, nrows)
    scrape_date = Date(get_ist())
    current_year = year(scrape_date)
    last_month = 0
    date_col = df.Date
    
    @inbounds for i in 1:nrows
        raw_val = string(date_col[i])
        (day, mon) = parse_chartink_date(raw_val)
        if day == 0 || mon == 0; continue; end
        
        if last_month == 0; last_month = mon; end
        if mon > (last_month + 6); current_year -= 1;
        elseif mon < (last_month - 6); current_year += 1; end
        
        try
            cand = Date(current_year, mon, day)
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

# 🔥 FILTER: DETECTS JUNK WIDGETS
function is_junk_widget(df::DataFrame)
    if nrow(df) == 0; return true; end
    
    # 1. Check for Generic Headers (Col_1, Col_2)
    cols = names(df)
    if "Col_1" in cols || "Col_2" in cols
        # 2. Check content for "Clause" or garbage symbols
        # Example Junk: "Clause", "*", "#", "Filter"
        first_row_str = join(string.(values(df[1, :])), " ")
        if occursin("Clause", first_row_str) || occursin("*", first_row_str)
            return true
        end
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
        let output = [];
        const cleanBody = (txt) => txt ? txt.trim().replace(/"/g, '""').replace(/\\n/g, " ") : "";
        const cleanHeader = (txt) => txt ? txt.replace(/Sort table by.*/gi, "").trim().replace(/"/g, '""').replace(/\\n/g, " ") : "";
        
        const nodes = document.querySelectorAll("table, div.dataTables_wrapper");
        nodes.forEach((node, i) => {
            let name = "Unknown Widget " + i;
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
            if abs(length(split(rows[i], "\",\"")) - expected_cols) > 2; continue; end
            clean_row = replace(rows[i], r"^\"[^\"]+\"," => "")
            if !occursin(r"\"(Symbol|Name|Date)\"", clean_row)
                 println(io, "\"$current_ts\"," * clean_row)
                 valid_count += 1
            end
        end
        
        if valid_count > 0
            seekstart(io)
            try
                df = CSV.read(io, DataFrame; strict=false, silencewarnings=true)
                
                # 🔥 JUNK FILTER: Skip tables that are just scan clauses (Atlas.csv)
                if is_junk_widget(df)
                    @info "  🗑️ Skipped Junk Widget: $clean_name"
                    continue 
                end
                
                strat = determine_strategy(df)
                enrich_dataframe!(df, strat)
                push!(widgets, WidgetTable(name, clean_name, df, folder_name, strat))
            catch; end
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
