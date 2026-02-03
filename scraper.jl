using ChromeDevToolsLite, Dates, JSON, DataFrames, CSV

# --- 🧱 Configuration ---
# Now a Vector of URLs instead of a single string
const TARGET_URLS = [
    "https://chartink.com/dashboard/208896",
    "https://chartink.com/dashboard/419640"
]
const OUTPUT_DIR = "chartink_data"

struct WidgetTable
    name::String
    clean_name::String
    data::DataFrame
end

get_ist() = now(Dates.UTC) + Hour(5) + Minute(30)

# --- 🧠 JS Logic (Header Cleaner + Strict Newlines) ---
const JS_PAYLOAD = """
(() => {
    try {
        let output = [];
        const cleanBody = (txt) => txt ? txt.trim().replace(/"/g, '""').replace(/\\n/g, " ") : "";
        const cleanHeader = (txt) => txt ? txt.replace(/Sort table by.*/gi, "").trim().replace(/"/g, '""').replace(/\\n/g, " ") : "";

        // 1️⃣ STANDARD TABLE SCAN
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
                             const isHeader = row.querySelector("th") !== null;
                             if (cells.length > 0) {
                                 let line = Array.from(cells).map(c => '"' + (isHeader ? cleanHeader(c.innerText) : cleanBody(c.innerText)) + '"').join(",");
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

# --- 🛠️ Pipeline Functions ---

function safe_unwrap(res)
    isa(res, Dict) ? (haskey(res,"value") ? res["value"] : (haskey(res,"result") ? safe_unwrap(res["result"]) : res)) : res
end

# Modified to accept a URL argument
function process_dashboard(page, url)
    @info "🧭 Navigating to: $url"
    ChromeDevToolsLite.goto(page, url)
    
    # Wait for tables
    for _ in 1:60
        res = ChromeDevToolsLite.evaluate(page, "document.querySelectorAll('table').length > 0") |> safe_unwrap
        if res == true; break; end
        sleep(1)
    end

    @info "📜 Scrolling..."
    h = ChromeDevToolsLite.evaluate(page, "document.body.scrollHeight") |> safe_unwrap
    h = isa(h, Number) ? h : 5000
    for s in 0:1000:h
        ChromeDevToolsLite.evaluate(page, "window.scrollTo(0, $s)")
        sleep(0.3)
    end
    sleep(3)
    
    # Extract
    @info "⚡ Extracting..."
    ChromeDevToolsLite.evaluate(page, "eval($(JSON.json(JS_PAYLOAD)))")
    
    len_res = ChromeDevToolsLite.evaluate(page, "window._data ? window._data.length : 0") |> safe_unwrap
    len = try parse(Int, string(len_res)) catch; 0 end
    
    if len == 0
        @warn "No data found on $url"
        return WidgetTable[]
    end
    
    buf = IOBuffer()
    for i in 0:50000:len
        chunk = ChromeDevToolsLite.evaluate(page, "window._data.substring($i, $(min(i+50000, len)))") |> safe_unwrap
        print(buf, chunk)
    end
    
    return parse_widgets(String(take!(buf)))
end

function parse_widgets(raw_csv::String) :: Vector{WidgetTable}
    @info "🧠 Parsing $(length(raw_csv)) bytes..."
    widgets = WidgetTable[]
    current_ts = get_ist()
    lines = split(replace(raw_csv, "\r" => ""), "\n")
    groups = Dict{String, Vector{String}}()
    
    for line in lines
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
                push!(widgets, WidgetTable(name, clean_name, df))
            catch; end
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
            unique!(final_df, [names(final_df)[1], names(final_df)[2]])
            sort!(final_df, :Timestamp)
            CSV.write(path, final_df)
        else
            CSV.write(path, w.data)
        end
        @info "  💾 Saved: $(w.clean_name)"
    catch e
        @warn "Schema Conflict for $(w.clean_name). Resetting file."
        CSV.write(path, w.data)
    end
end

function main()
    mkpath(OUTPUT_DIR)
    
    try
        @info "🔌 Connecting to Browser..."
        page = ChromeDevToolsLite.connect_browser()
        
        # 🔄 Loop through all Target URLs
        for url in TARGET_URLS
            @info "--- Processing Dashboard: $url ---"
            widgets = process_dashboard(page, url)
            
            if !isempty(widgets)
                widgets .|> save_widget
                @info "✅ Dashboard Complete."
            else
                @warn "⚠️ No widgets found on $url"
            end
        end
        
        @info "🎉 All Dashboards Scraped Successfully."
        
    catch e
        @error "Critical Failure" exception=(e, catch_backtrace())
        exit(1)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
