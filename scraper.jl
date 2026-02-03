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

# --- 🧠 JS Logic (The Brain) ---
const JS_PAYLOAD = """
(() => {
    try {
        const nodes = document.querySelectorAll("table, div.dataTables_wrapper");
        if (nodes.length === 0) return "NODATA";

        let output = [];
        nodes.forEach((node, i) => {
            // 1. Find Widget Name (Climb up up to 12 parents)
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

            // 2. Extract Rows
            let rows = (node.tagName === "TABLE") ? node.querySelectorAll("tr") : node.querySelectorAll("table tr");
            if (!rows.length) return;

            Array.from(rows).forEach(row => {
                let txt = row.innerText;
                if (txt.includes("No data") || txt.includes("Clause")) return;
                
                const cells = Array.from(row.querySelectorAll("th, td"));
                if (!cells.length) return;

                const isHeader = row.querySelector("th");
                let line = cells.map(c => {
                    let t = c.innerText.trim().replace(/"/g, '""');
                    if (isHeader) t = t.split('\\n')[0].trim().replace(/Sort table by/gi, "").trim();
                    return '"' + t + '"';
                }).join(",");
                
                output.push('"' + name.replace(/"/g, '""') + '",' + line);
            });
        });
        window._data = output.join("\\n");
        return "DONE";
    } catch (e) { return "ERROR: " + e.toString(); }
})()
"""

# --- 🛠️ Pipeline Stages ---

function safe_unwrap(res)
    isa(res, Dict) ? (haskey(res,"value") ? res["value"] : (haskey(res,"result") ? safe_unwrap(res["result"]) : res)) : res
end

function setup_page()
    @info "🔌 Connecting..."
    page = ChromeDevToolsLite.connect_browser()
    ChromeDevToolsLite.goto(page, TARGET_URL)
    
    @info "👀 Waiting for Tables..."
    # 🔴 CRITICAL FIX: Explicit Wait Loop
    for _ in 1:60
        res = ChromeDevToolsLite.evaluate(page, "document.querySelectorAll('table').length > 0") |> safe_unwrap
        if res == true; break; end
        sleep(1)
    end

    @info "📜 Scrolling to Trigger Lazy Load..."
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
    @info "⚡ Executing Extraction JS..."
    ChromeDevToolsLite.evaluate(page, "eval($(JSON.json(JS_PAYLOAD)))")
    
    len = ChromeDevToolsLite.evaluate(page, "window._data ? window._data.length : 0") |> safe_unwrap
    len = try parse(Int, string(len)) catch; 0 end
    
    @info "📦 Payload Size: $len chars"
    if len == 0; error("No Data Found (Page might be blank)"); end
    
    buf = IOBuffer()
    for i in 0:50000:len
        chunk = ChromeDevToolsLite.evaluate(page, "window._data.substring($i, $(min(i+50000, len)))") |> safe_unwrap
        print(buf, chunk)
    end
    return String(take!(buf))
end

function parse_widgets(raw_csv::String) :: Vector{WidgetTable}
    @info "🧠 Parsing Raw Data..."
    widgets = WidgetTable[]
    current_ts = get_ist()
    
    lines = split(raw_csv, "\n")
    groups = Dict{String, Vector{String}}()
    
    for line in lines
        length(line) < 5 && continue
        m = match(r"^\"([^\"]+)\"", line)
        key = isnothing(m) ? "Unknown" : m.captures[1]
        push!(get!(groups, key, String[]), line)
    end

    for (name, rows) in groups
        clean_name = replace(name, r"[^a-zA-Z0-9]" => "_")
        # Truncate long names
        if length(clean_name) > 50; clean_name = clean_name[1:50]; end
        
        # Header Heuristics
        header_idx = findfirst(l -> occursin(r"\",\"(Symbol|Name|Scan Name)\"", l), rows)
        header_idx = isnothing(header_idx) ? 1 : header_idx
        
        io = IOBuffer()
        println(io, "\"Timestamp\"," * rows[header_idx])
        for i in (header_idx+1):length(rows)
             # Skip repeated headers
             if !occursin(r"\",\"(Symbol|Name|Scan Name)\"", rows[i])
                 println(io, "\"$current_ts\"," * rows[i])
             end
        end
        seekstart(io)
        
        try
            df = CSV.read(io, DataFrame)
            push!(widgets, WidgetTable(name, clean_name, df))
        catch e
            @warn "Parse Fail: $name"
        end
    end
    return widgets
end

function save_widget(w::WidgetTable)
    path = joinpath(OUTPUT_DIR, w.clean_name * ".csv")
    
    if isfile(path)
        old_df = CSV.read(path, DataFrame)
        # 🦄 DataFrames Power: Union Merge + Unique + Sort
        final_df = vcat(old_df, w.data, cols=:union)
        unique!(final_df, [:Timestamp, :Symbol]) # Dedupe
        sort!(final_df, :Timestamp)
        CSV.write(path, final_df)
    else
        CSV.write(path, w.data)
    end
    @info "  💾 Saved: $(w.clean_name) ($(nrow(w.data)) new rows)"
end

# --- 🏃 Main Execution ---
function main()
    mkpath(OUTPUT_DIR)
    
    try
        # The "Enthusiast Pipeline" 🟣
        page = setup_page()
        data = extract_data(page)
        widgets = parse_widgets(data)
        
        if isempty(widgets)
            @warn "No widgets parsed!"
            exit(0)
        end

        # Broadcasting save over the vector
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
