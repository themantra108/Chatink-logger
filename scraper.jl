using ChromeDevToolsLite, Dates, JSON, DataFrames, CSV

# ==============================================================================
# 🧱 CONFIGURATION
# ==============================================================================
const TARGET_URLS = [
    "https://chartink.com/dashboard/208896",
    "https://chartink.com/dashboard/419640"
]
const OUTPUT_ROOT = "chartink_data"

# Pre-compiled Regex for date parsing
const DATE_REGEX = r"(\d+)(?:st|nd|rd|th)?\s+([a-zA-Z]+)"
const MONTH_MAP = Dict{SubString{String}, Int}(
    "Jan" => 1, "Feb" => 2, "Mar" => 3, "Apr" => 4, "May" => 5, "Jun" => 6,
    "Jul" => 7, "Aug" => 8, "Sep" => 9, "Oct" => 10, "Nov" => 11, "Dec" => 12
)

struct WidgetTable
    name::String
    clean_name::String
    data::DataFrame
    subfolder::String 
end

get_ist() = now(Dates.UTC) + Hour(5) + Minute(30)

# ==============================================================================
# 📅 DATE LOGIC
# ==============================================================================

function parse_chartink_date(date_str::AbstractString)
    m = match(DATE_REGEX, date_str)
    if isnothing(m); return (0, 0); end
    day = parse(Int, m.captures[1])
    mon_str = titlecase(m.captures[2])[1:3]
    mon = get(MONTH_MAP, mon_str, 0)
    return (day, mon)
end

function add_derived_columns!(df::DataFrame)
    if "Timestamp" in names(df)
        df[!, :Scan_Date] = Date.(df[!, :Timestamp])
    end

    if "Date" in names(df)
        nrows = nrow(df)
        full_dates = Vector{Union{Date, Missing}}(missing, nrows)
        current_year = year(get_ist()) 
        last_month = 0
        
        for i in 1:nrows
            raw_val = string(df[i, "Date"])
            (day, mon) = parse_chartink_date(raw_val)
            if day == 0 || mon == 0; continue; end
            
            if last_month == 0; last_month = mon; end
            
            if mon > (last_month + 6); current_year -= 1;
            elseif mon < (last_month - 6); current_year += 1; end
            
            try; full_dates[i] = Date(current_year, mon, day); catch; end
            last_month = mon
        end
        df[!, :Full_Date] = full_dates
    end
end

# ==============================================================================
# 🧠 JAVASCRIPT PAYLOAD
# ==============================================================================
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
                        name = txt.split('\\n')[0].trim();
                        break;
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
                let line = cells.map(c => {
                    let raw = c.innerText;
                    return '"' + (isHeader ? cleanHeader(raw) : cleanBody(raw)) + '"';
                }).join(",");
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

# ==============================================================================
# 🛠️ PIPELINE FUNCTIONS
# ==============================================================================

function safe_unwrap(res)
    isa(res, Dict) ? (haskey(res,"value") ? res["value"] : (haskey(res,"result") ? safe_unwrap(res["result"]) : res)) : res
end

function parse_widgets(raw_csv::String, folder_name::String) :: Vector{WidgetTable}
    @info "🧠 Parsing widgets for folder: [$folder_name]"
    widgets = WidgetTable[]
    current_ts = get_ist()
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
                add_derived_columns!(df) 
                push!(widgets, WidgetTable(name, clean_name, df, folder_name))
            catch; end
        end
    end
    return widgets
end

# 🔥 UPSERT ENGINE (Logic verified for 2nd Feb/1st Feb update scenario)
function save_widget(w::WidgetTable)
    folder_path = joinpath(OUTPUT_ROOT, w.subfolder)
    mkpath(folder_path)
    path = joinpath(folder_path, w.clean_name * ".csv")
    
    try
        if isfile(path)
            old_df = CSV.read(path, DataFrame)
            
            # 1. UNION: Combine Old History + New Updates
            combined_df = vcat(old_df, w.data, cols=:union)
            
            # 2. SORT: Newest Scrapes go to the top
            sort!(combined_df, :Timestamp, rev=true)
            
            # 3. DEDUPE: Define what makes a row "unique"
            dedupe_cols = Symbol[]
            
            if "Date" in names(combined_df)
                # Scenario: Market Condition (Date-based)
                # If "2nd Feb" appears in both Old and New, 'unique!' will see 
                # the New one first (due to Sort) and discard the Old one.
                push!(dedupe_cols, :Date)
            else
                # Scenario: Stock List (Symbol-based)
                # Key = Symbol + Date (e.g., RELIANCE on 2026-02-04)
                col2_name = names(combined_df)[2] # Usually Symbol
                push!(dedupe_cols, Symbol(col2_name))
                if "Scan_Date" in names(combined_df)
                    push!(dedupe_cols, :Scan_Date)
                end
            end
            
            # 4. EXECUTE REPLACE
            unique!(combined_df, dedupe_cols)
            
            CSV.write(path, combined_df)
        else
            sort!(w.data, :Timestamp, rev=true)
            CSV.write(path, w.data)
        end
        @info "  💾 Saved: [$(w.subfolder)] -> $(w.clean_name)"
    catch e
        @warn "Schema Conflict or Save Error for $(w.clean_name). Overwriting."
        CSV.write(path, w.data)
    end
end

function get_dashboard_name(page)
    raw_title = ChromeDevToolsLite.evaluate(page, "document.title") |> safe_unwrap
    if isnothing(raw_title) || raw_title == ""; return "Unknown_Dashboard"; end
    
    clean_title = replace(raw_title, " - Chartink.com" => "") |> 
                  x -> replace(x, " - Chartink" => "") |>
                  x -> replace(x, r"[^a-zA-Z0-9 \-_]" => "") |> 
                  x -> replace(strip(x), " " => "_")
                  
    return isempty(clean_title) ? "Dashboard_Unknown" : clean_title
end

function process_dashboard(page, url)
    @info "🧭 Navigating to: $url"
    ChromeDevToolsLite.evaluate(page, "window._data = null;")
    
    retry_nav = retry(() -> ChromeDevToolsLite.goto(page, url), delays=[2.0, 5.0, 10.0])
    try; retry_nav(); catch; @error "Failed to load $url"; return WidgetTable[]; end
    
    sleep(5) 
    folder_name = get_dashboard_name(page)
    @info "🏷️ Identified Dashboard: $folder_name"
    
    for i in 1:60
        res = ChromeDevToolsLite.evaluate(page, "document.querySelectorAll('table').length > 0") |> safe_unwrap
        if res == true; break; end
        sleep(1)
    end
    
    h = ChromeDevToolsLite.evaluate(page, "document.body.scrollHeight") |> safe_unwrap
    h = isa(h, Number) ? h : 5000
    for s in 0:1000:h; ChromeDevToolsLite.evaluate(page, "window.scrollTo(0, $s)"); sleep(0.3); end
    sleep(3) 
    
    @info "⚡ Extracting..."
    ChromeDevToolsLite.evaluate(page, "eval($(JSON.json(JS_PAYLOAD)))")
    
    len_res = ChromeDevToolsLite.evaluate(page, "window._data ? window._data.length : 0") |> safe_unwrap
    len = try parse(Int, string(len_res)) catch; 0 end
    
    if len == 0; @warn "⚠️ No data found on $url."; return WidgetTable[]; end
    
    buf = IOBuffer()
    for i in 0:50000:len
        chunk = ChromeDevToolsLite.evaluate(page, "window._data.substring($i, $(min(i+50000, len)))") |> safe_unwrap
        print(buf, chunk)
    end
    return parse_widgets(String(take!(buf)), folder_name)
end

function print_summary()
    @info "📊 --- MISSION REPORT ---"
    if !isdir(OUTPUT_ROOT); @info "No data directory found."; return; end
    
    for (root, dirs, files) in walkdir(OUTPUT_ROOT)
        level = count(c -> c == '/', replace(root, "\\" => "/")) - count(c -> c == '/', OUTPUT_ROOT)
        indent = "  " ^ level
        folder = basename(root)
        if folder != basename(OUTPUT_ROOT)
            println("📁 $indent$folder/")
        end
        for file in files
            println("   $indent  📄 $file")
        end
    end
    @info "-------------------------"
end

function main()
    mkpath(OUTPUT_ROOT)
    try
        @info "🔌 Connecting to Chrome..."
        page = ChromeDevToolsLite.connect_browser()
        
        for url in TARGET_URLS
            @info "--- [TARGET] $url ---"
            widgets = process_dashboard(page, url)
            if !isempty(widgets)
                widgets .|> save_widget
                @info "✅ Dashboard Complete."
            end
        end
        
        print_summary()
        @info "🎉 Scrape Cycle Complete."
    catch e
        @error "Crash" exception=(e, catch_backtrace())
        exit(1)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
