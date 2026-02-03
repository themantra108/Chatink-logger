using ChromeDevToolsLite, Dates, JSON, DataFrames, CSV

# --- 🧱 Configuration & Types ---
const TARGET_URL = "https://chartink.com/dashboard/208896"
const OUTPUT_DIR = "chartink_data"

struct WidgetTable
    name::String
    clean_name::String
    data::DataFrame
end

get_ist() = now(Dates.UTC) + Hour(5) + Minute(30)

# --- 🧠 JS Logic (The Dragnet) ---
# This JS payload hunts for tables, specific titles, and handles non-standard grids.
const JS_PAYLOAD = """
(() => {
    try {
        let output = [];
        
        // 1️⃣ STANDARD TABLE SCAN
        const nodes = document.querySelectorAll("table, div.dataTables_wrapper");
        
        nodes.forEach((node, i) => {
            let name = "Unknown Widget " + i;
            let curr = node, depth = 0;
            
            // Climb up 12 levels to find the Title
            while (curr && depth++ < 12) {
                let sib = curr.previousElementSibling;
                while (sib) {
                    let txt = sib.innerText ? sib.innerText.trim() : "";
                    // Filter out noise like "Loading..." or buttons
                    if (txt.length > 2 && txt.length < 150 && !/Loading|Error|Run Scan/.test(txt)) {
                        name = txt.split('\\n')[0].trim();
                        break;
                    }
                    sib = sib.previousElementSibling;
                }
                if (!name.includes("Unknown")) break;
                curr = curr.parentElement;
            }

            // Extract Rows (Handle standard tables and DataTables divs)
            let rows = (node.tagName === "TABLE") ? node.querySelectorAll("tr") : node.querySelectorAll("table tr");
            if (!rows.length) return;

            Array.from(rows).forEach(row => {
                let txt = row.innerText;
                if (txt.includes("No data") || txt.includes("Clause")) return;
                
                const cells = Array.from(row.querySelectorAll("th, td"));
                if (!cells.length) return;

                // Escape quotes and join columns
                let line = cells.map(c => {
                    let t = c.innerText.trim().replace(/"/g, '""');
                    return '"' + t + '"';
                }).join(",");
                
                // Prefix with Widget Name
                output.push('"' + name.replace(/"/g, '""') + '",' + line);
            });
        });

        // 2️⃣ SPECIAL HUNTER: MARKET CONDITION / BREADTH
        // Explicitly looks for headers like "Market Condition" if missed by scan
        const headings = document.querySelectorAll("h1, h2, h3, h4, h5, h6, div.card-header");
        headings.forEach(h => {
            let title = h.innerText.trim();
            if (/Market|Condition|Breadth|Ratio/i.test(title)) {
                let container = h.nextElementSibling;
                if (!container && h.parentElement) container = h.parentElement.nextElementSibling;
                
                if (container) {
                    let table = container.querySelector("table");
                    if (table) {
                        let rows = table.querySelectorAll("tr");
                        Array.from(rows).forEach(row => {
                             let cells = row.querySelectorAll("td, th");
                             if (cells.length > 0) {
                                 let line = Array.from(cells).map(c => '"' + c.innerText.trim().replace(/"/g, '""') + '"').join(",");
                                 // Add "MANUAL_CATCH" prefix to handle later
                                 output.push('"MANUAL_CATCH_' + title.replace(/"/g, '""') + '",' + line);
                             }
                        });
                    }
                }
            }
        });

        // Deduplicate lines
        let uniqueOutput = [...new Set(output)];
        window._data = uniqueOutput.join("\\n");
        return "DONE";
    } catch (e) { return "ERROR: " + e.toString(); }
})()
"""

# --- 🛠️ Pipeline Functions ---

function safe_unwrap(res)
    isa(res, Dict) ? (haskey(res,"value") ? res["value"] : (haskey(res,"result") ? safe_unwrap(res["result"]) : res)) : res
end

function setup_page()
    @info "🔌 Connecting..."
    page = ChromeDevToolsLite.connect_browser()
    ChromeDevToolsLite.goto(page, TARGET_URL)
    
    @info "👀 Waiting for Tables to Render..."
    # Robust wait loop (Max 60s)
    for _ in 1:60
        res = ChromeDevToolsLite.evaluate(page, "document.querySelectorAll('table').length > 0") |> safe_unwrap
        if res == true; break; end
        sleep(1)
    end

    @info "📜 Scrolling to Trigger Lazy Loading..."
    h = ChromeDevToolsLite.evaluate(page, "document.body.scrollHeight") |> safe_unwrap
    h = isa(h, Number) ? h : 5000
    
    for s in 0:800:h
        ChromeDevToolsLite.evaluate(page, "window.scrollTo(0, $s)")
        sleep(0.3)
    end
    sleep(3) # Final settling time
    return page
end

function extract_data(page)
    @info "⚡ Executing JS Payload..."
    ChromeDevToolsLite.evaluate(page, "eval($(JSON.json(JS_PAYLOAD)))")
    
    len = ChromeDevToolsLite.evaluate(page, "window._data ? window._data.length : 0") |> safe_unwrap
    len = try parse(Int, string(len)) catch; 0 end
    
    @info "📦 Payload Size: $len chars"
    if len == 0; error("No Data Found (Page might be blank)"); end
    
    buf = IOBuffer()
    # Chunked fetch to avoid socket limits
    for i in 0:50000:len
        chunk = ChromeDevToolsLite.evaluate(page, "window._data.substring($i, $(min(i+50000, len)))") |> safe_unwrap
        print(buf, chunk)
    end
    return String(take!(buf))
end

function parse_widgets(raw_csv::String) :: Vector{WidgetTable}
    @info "🧠 Parsing Data..."
    widgets = WidgetTable[]
    current_ts = get_ist()
    
    lines = split(raw_csv, "\n")
    groups = Dict{String, Vector{String}}()
    
    # 1. Group raw lines by Widget Name
    for line in lines
        length(line) < 5 && continue
        m = match(r"^\"([^\"]+)\"", line)
        key = isnothing(m) ? "Unknown" : m.captures[1]
        
        # Merge manual catches
        key = replace(key, "MANUAL_CATCH_" => "")
        
        push!(get!(groups, key, String[]), line)
    end

    # 2. Process each group into a DataFrame
    for (name, rows) in groups
        clean_name = replace(name, r"[^a-zA-Z0-9]" => "_")
        if length(clean_name) > 50; clean_name = clean_name[1:50]; end
        
        # 🔑 CRITICAL FIX: Header Detection
        # Looks for "Symbol", "Name", "Scan Name", OR "Date" (for Mkt Condition)
        header_idx = findfirst(l -> occursin(r"\",\"(Symbol|Name|Scan Name|Date)\"", l), rows)
        
        # Fallback if no header found
        if isnothing(header_idx); header_idx = 1; end
        
        io = IOBuffer()
        
        # Extract Clean Header (Remove the first "WidgetName" column)
        raw_header = rows[header_idx]
        clean_header = replace(raw_header, r"^\"[^\"]+\"," => "")
        println(io, "\"Timestamp\"," * clean_header)
        
        for i in (header_idx+1):length(rows)
            # Strip Widget Name column from data row
            clean_row = replace(rows[i], r"^\"[^\"]+\"," => "")
            
            # Filter Repeats (skip lines that look like headers)
            if !occursin(r"\"(Symbol|Name|Date)\"", clean_row)
                 println(io, "\"$current_ts\"," * clean_row)
            end
        end
        
        seekstart(io)
        try
            df = CSV.read(io, DataFrame)
            if nrow(df) > 0
                push!(widgets, WidgetTable(name, clean_name, df))
            end
        catch e
            @warn "Parse Fail: $name"
        end
    end
    return widgets
end

function save_widget(w::WidgetTable)
    path = joinpath(OUTPUT_DIR, w.clean_name * ".csv")
    
    if isfile(path)
        try
            old_df = CSV.read(path, DataFrame)
            # Merge Old + New
            final_df = vcat(old_df, w.data, cols=:union)
            # Deduplicate (Timestamp + Symbol/Date)
            unique!(final_df) 
            # Sort by Time
            sort!(final_df, :Timestamp)
            CSV.write(path, final_df)
        catch
            # If old file is corrupt, overwrite
            CSV.write(path, w.data)
        end
    else
        CSV.write(path, w.data)
    end
    @info "  💾 Saved: $(w.clean_name) ($(nrow(w.data)) new rows)"
end

# --- 🏃 Main Execution ---
function main()
    mkpath(OUTPUT_DIR)
    
    try
        # The Enthusiast Pipeline 🟣
        page = setup_page()
        data = extract_data(page)
        widgets = parse_widgets(data)
        
        if isempty(widgets)
            @warn "No widgets parsed!"
            exit(0)
        end

        # Broadcast save across all widgets
        widgets .|> save_widget
        
        @info "✅ Pipeline Success. ($(length(widgets)) widgets processed)"
    catch e
        @error "Pipeline Failed" exception=(e, catch_backtrace())
        exit(1)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
