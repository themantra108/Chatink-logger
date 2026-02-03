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

# --- 🧠 JS Logic ---
const JS_PAYLOAD = """
(() => {
    try {
        let output = [];
        const clean = (txt) => txt ? txt.trim().replace(/"/g, '""').replace(/\\n/g, " ") : "";

        // 1️⃣ STANDARD SCAN
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
                let line = Array.from(cells).map(c => '"' + clean(c.innerText) + '"').join(",");
                output.push('"' + clean(name) + '",' + line);
            });
        });

        // 2️⃣ SPECIAL HUNTER (Breadth/Condition)
        const headings = document.querySelectorAll("h1, h2, h3, h4, h5, h6, div.card-header");
        headings.forEach(h => {
            let title = h.innerText.trim();
            if (/Market|Condition|Breadth|Ratio/i.test(title)) {
                let container = h.nextElementSibling;
                if (!container && h.parentElement) container = h.parentElement.nextElementSibling;
                if (container) {
                    let table = container.querySelector("table");
                    if (table) {
                        Array.from(table.querySelectorAll("tr")).forEach(row => {
                             let cells = row.querySelectorAll("td, th");
                             if (cells.length > 0) {
                                 let line = Array.from(cells).map(c => '"' + clean(c.innerText) + '"').join(",");
                                 output.push('"MANUAL_CATCH_' + clean(title) + '",' + line);
                             }
                        });
                    }
                }
            }
        });

        let uniqueOutput = [...new Set(output)];
        window._data = uniqueOutput.join("\\n");
        return "DONE";
    } catch (e) { return "ERROR: " + e.toString(); }
})()
"""

# --- 🛠️ Pipeline ---

function safe_unwrap(res)
    isa(res, Dict) ? (haskey(res,"value") ? res["value"] : (haskey(res,"result") ? safe_unwrap(res["result"]) : res)) : res
end

function setup_page()
    @info "🔌 Connecting..."
    page = ChromeDevToolsLite.connect_browser()
    ChromeDevToolsLite.goto(page, TARGET_URL)
    
    @info "👀 Waiting for Render..."
    for _ in 1:60
        res = ChromeDevToolsLite.evaluate(page, "document.querySelectorAll('table').length > 0") |> safe_unwrap
        if res == true; break; end
        sleep(1)
    end

    @info "📜 Scrolling..."
    h = ChromeDevToolsLite.evaluate(page, "document.body.scrollHeight") |> safe_unwrap
    h = isa(h, Number) ? h : 5000
    for s in 0:800:h
        ChromeDevToolsLite.evaluate(page, "window.scrollTo(0, $s)")
        sleep(0.3)
    end
    sleep(3)
    return page
end

function extract_data(page)
    @info "⚡ Extracting..."
    ChromeDevToolsLite.evaluate(page, "eval($(JSON.json(JS_PAYLOAD)))")
    len = ChromeDevToolsLite.evaluate(page, "window._data ? window._data.length : 0") |> safe_unwrap
    len = try parse(Int, string(len)) catch; 0 end
    
    if len == 0; error("No Data Found"); end
    
    buf = IOBuffer()
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
    
    lines = split(replace(raw_csv, "\r" => ""), "\n")
    groups = Dict{String, Vector{String}}()
    
    for line in lines
        if length(line) < 5 || !startswith(line, "\""); continue; end
        m = match(r"^\"([^\"]+)\"", line)
        if isnothing(m); continue; end
        
        key = replace(m.captures[1], "MANUAL_CATCH_" => "")
        push!(get!(groups, key, String[]), line)
    end

    @info "🔍 Found $(length(groups)) potential widgets."

    for (name, rows) in groups
        clean_name = replace(name, r"[^a-zA-Z0-9]" => "_")
        if length(clean_name) > 50; clean_name = clean_name[1:50]; end
        
        # 🧠 HEADER LOGIC: The Fix
        header_idx = findfirst(l -> occursin(r"\",\"(Symbol|Name|Scan Name|Date)\"", l), rows)
        
        io = IOBuffer()
        start_row = 1
        expected_cols = 0
        
        if !isnothing(header_idx)
            # CASE A: Explicit Header Found
            raw_header = replace(rows[header_idx], r"^\"[^\"]+\"," => "")
            println(io, "\"Timestamp\"," * raw_header)
            
            start_row = header_idx + 1
            expected_cols = length(split(rows[header_idx], "\",\""))
        else
            # CASE B: No Header Found (Likely Single Row Data)
            # Create dummy header: Col1, Col2, ...
            # Get column count from first data row
            first_row_clean = replace(rows[1], r"^\"[^\"]+\"," => "")
            cols_count = length(split(first_row_clean, "\",\""))
            
            dummy_header = join(["\"Col_$i\"" for i in 1:cols_count], ",")
            println(io, "\"Timestamp\"," * dummy_header)
            
            start_row = 1 # Start from the very first row!
            expected_cols = cols_count + 1 # +1 for widget name col in raw
        end
        
        # Write Data
        valid_count = 0
        for i in start_row:length(rows)
            # Loose Validation: Allow +/- 1 column variance
            curr_cols = length(split(rows[i], "\",\""))
            if abs(curr_cols - expected_cols) > 2; continue; end

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
                push!(widgets, WidgetTable(name, clean_name, df))
            catch e
                @warn "Parse Fail: $name"
            end
        else
            @warn "Skipped $name: No valid rows found."
        end
    end
    return widgets
end

function save_widget(w::WidgetTable)
    path = joinpath(OUTPUT_DIR, w.clean_name * ".csv")
    try
        if isfile(path)
            old_df = CSV.read(path, DataFrame)
            final_df = vcat(old_df, w.data, cols=:union)
            unique!(final_df, [:Timestamp, names(final_df)[2]]) # Dedupe
            sort!(final_df, :Timestamp)
            CSV.write(path, final_df)
        else
            CSV.write(path, w.data)
        end
        @info "  💾 Saved: $(w.clean_name) ($(nrow(w.data)) rows)"
    catch e
        @warn "Save Error for $(w.clean_name)"
    end
end

function main()
    mkpath(OUTPUT_DIR)
    try
        page = setup_page()
        data = extract_data(page)
        widgets = parse_widgets(data)
        widgets .|> save_widget
        @info "✅ Pipeline Success."
    catch e
        @error "Failed" exception=(e, catch_backtrace())
        exit(1)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
