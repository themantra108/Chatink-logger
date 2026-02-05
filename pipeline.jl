using ChromeDevToolsLite, DataFrames, CSV, Dates, JSON, Printf, Mustache

# ==============================================================================
# 1. 🛠️ CORE UTILS & TYPES
# ==============================================================================
module Utils
    using Dates
    
    # Custom Exception for Retry Logic
    struct PageLoadError <: Exception; msg::String; end

    # 🇮🇳 Time Helper
    get_ist() = now(Dates.UTC) + Hour(5) + Minute(30)
    fmt_ist(dt) = Dates.format(dt, "yyyy-mm-dd I:MM p") * " IST"

    # 🔁 Robust Retry Macro
    macro retry(n, delay, expr)
        quote
            local result
            local success = false
            for i in 1:$n
                try
                    result = $(esc(expr))
                    success = true
                    break
                catch e
                    @warn "Attempt $i failed: $(e)"
                    sleep($delay)
                end
            end
            if !success; error("Operation failed after $($n) attempts"); end
            result
        end
    end
end

# ==============================================================================
# 2. 🧱 DOMAIN CONFIG
# ==============================================================================
module Config
    const TARGETS = [
        "https://chartink.com/dashboard/419640",
        "https://chartink.com/dashboard/208896"
    ]
    const DATA_DIR = "chartink_data"
    const PUBLIC_DIR = "public"
    const TEMPLATE_FILE = "dashboard_template.html"
end

# ==============================================================================
# 3. 🕸️ SCRAPER ENGINE
# ==============================================================================
module Scraper
    using ..Utils, ..Config
    using ChromeDevToolsLite, DataFrames, CSV, Dates, JSON

    struct Widget
        name::String
        clean_name::String
        data::DataFrame
    end

    # JS Payload (Minified & Optimized)
    const EXTRACTOR_JS = """
    (() => {
        const clean = t => t ? t.trim().replace(/"/g, '""').replace(/\\n/g, " ") : "";
        let csv = [];
        
        const scanTable = (node, forcedTitle) => {
            let title = forcedTitle || "Unknown";
            if(!forcedTitle) {
                // Heuristic: Find nearest heading above table
                let curr = node, d=0;
                while(curr && d++ < 10) {
                    let sib = curr.previousElementSibling;
                    while(sib) {
                        if(/H[1-6]|DIV/.test(sib.tagName) && sib.innerText.length > 3) {
                            title = sib.innerText.split('\\n')[0]; break;
                        }
                        sib = sib.previousElementSibling;
                    }
                    if(title !== "Unknown") break;
                    curr = curr.parentElement;
                }
            }
            
            node.querySelectorAll("tr").forEach(row => {
                if(row.innerText.includes("No data")) return;
                const cells = Array.from(row.querySelectorAll("th, td"));
                if(!cells.length) return;
                const rowStr = cells.map(c => '"' + clean(c.innerText) + '"').join(",");
                csv.push('"' + clean(title) + '",' + rowStr);
            });
        };

        document.querySelectorAll("table").forEach(t => scanTable(t));
        document.querySelectorAll(".card").forEach(c => {
            let h = c.querySelector(".card-header");
            let t = c.querySelector("table");
            if(h && t) scanTable(t, h.innerText);
        });
        
        return [...new Set(csv)].join("\\n");
    })()
    """

    function is_valid(df::DataFrame)
        nrow(df) > 0 && ncol(df) >= 2 && !occursin(r"(#|\*|Clause)", string(df[1,1]))
    end

    function process_page(page, url)
        @info "🧭 Navigating: $url"
        Utils.@retry 3 5 begin
            ChromeDevToolsLite.goto(page, url)
        end
        sleep(10) # Let Chartink JS settle
        
        # Scroll for lazy loading
        h = ChromeDevToolsLite.evaluate(page, "document.body.scrollHeight")
        h_val = isa(h, Dict) ? h["value"] : h
        for s in 0:2000:h_val
            ChromeDevToolsLite.evaluate(page, "window.scrollTo(0, $s)")
            sleep(0.2)
        end

        # Extract CSV Data
        raw_res = ChromeDevToolsLite.evaluate(page, EXTRACTOR_JS)
        raw_csv = isa(raw_res, Dict) ? raw_res["value"] : raw_res
        
        if isempty(raw_csv); return Widget[]; end
        
        # Parse CSV Blob into DataFrames
        widgets = Widget[]
        rows = split(raw_csv, "\n")
        grouped = Dict{String, Vector{String}}()
        
        for r in rows
            m = match(r"^\"([^\"]+)\"", r)
            k = isnothing(m) ? "Unknown" : m.captures[1]
            push!(get!(grouped, k, String[]), r)
        end

        for (title, lines) in grouped
            # Reconstruct CSV for this widget
            # Remove the first column (Widget Name) from the lines
            clean_lines = map(l -> replace(l, r"^\"[^\"]+\"," => ""), lines)
            
            # Identify Header
            header_idx = findfirst(l -> occursin(r"\"(Symbol|Name|Date)\"", l), clean_lines)
            isnothing(header_idx) && continue
            
            # Prepare Buffer: Timestamp + Header + Data
            io = IOBuffer()
            println(io, "\"Timestamp\"," * clean_lines[header_idx])
            ts = Utils.get_ist()
            for i in (header_idx+1):length(clean_lines)
                println(io, "\"$ts\"," * clean_lines[i])
            end
            
            try
                seekstart(io)
                df = CSV.read(io, DataFrame; strict=false, silencewarnings=true)
                if is_valid(df)
                    clean_id = replace(title, r"[^a-zA-Z0-9]" => "_")
                    push!(widgets, Widget(title, clean_id, df))
                end
            catch e
                @warn "Failed to parse widget: $title"
            end
        end
        return widgets
    end

    function run()
        page = ChromeDevToolsLite.connect_browser()
        all_widgets = Widget[]
        for url in Config.TARGETS
            append!(all_widgets, process_page(page, url))
        end
        return all_widgets
    end
end

# ==============================================================================
# 4. 📊 DASHBOARD RENDERER
# ==============================================================================
module Dashboard
    using ..Utils, ..Config, ..Scraper
    using DataFrames, CSV, Printf, Mustache

    # 🎨 Styling Rules
    const RULES = [
        (r"4\.5r",          v -> v > 400 ? "background:#f1c40f80" : v >= 200 ? "background:#2ecc7180" : v < 50 ? "background:#e74c3c80" : ""),
        (r"(4\.5|20|50)chg",v -> v < -20 ? "background:#e74c3c80" : v > 20   ? "background:#2ecc7180" : ""),
        (r"20r",            v -> v < 50  ? "background:#e74c3c80" : v > 75   ? "background:#2ecc7180" : ""),
        (r"50r",            v -> v < 60  ? "background:#e74c3c80" : v > 85   ? "background:#2ecc7180" : "")
    ]

    function get_style(col, val)
        v = tryparse(Float64, string(val))
        isnothing(v) && return ""
        c = replace(lowercase(string(col)), " " => "")
        for (p, f) in RULES; occursin(p, c) && return f(v); end
        return ""
    end

    function save_history(w::Scraper.Widget)
        path = joinpath(Config.DATA_DIR, w.clean_name * ".csv")
        mkpath(dirname(path))
        
        # Merge if exists
        final_df = w.data
        if isfile(path)
            old_df = CSV.read(path, DataFrame)
            # Simple Union (Snapshot strategy)
            final_df = vcat(w.data, old_df, cols=:union)
            # Dedup based on Timestamp + Symbol (if available)
            if "Symbol" in names(final_df)
                unique!(final_df, ["Timestamp", "Symbol"])
            end
        end
        
        # Limit history to prevent bloat (optional, keep last 5000 rows)
        if nrow(final_df) > 5000; final_df = first(final_df, 5000); end
        
        CSV.write(path, final_df)
        return final_df
    end

    function df_to_html(df::DataFrame, id::String)
        # Cleanup
        "Timestamp" in names(df) && select!(df, Not("Timestamp"))
        "Col_1" in names(df) && rename!(df, "Col_1" => "Symbol")
        
        # Headers
        headers = names(df)
        safe_headers = [replace(string(h), r"[^a-zA-Z0-9]" => "") for h in headers]
        
        io = IOBuffer()
        print(io, "<table id='$id' class='display compact stripe nowrap' style='width:100%'><thead><tr>")
        foreach(h -> print(io, "<th>$h</th>"), safe_headers)
        print(io, "</tr></thead><tbody>")
        
        for row in eachrow(df)
            print(io, "<tr>")
            for (col, val) in pairs(row)
                sty = get_style(col, val)
                fmt = val isa Real ? @sprintf("%.2f", val) : ismissing(val) ? "-" : val
                print(io, "<td style='$sty'>$fmt</td>")
            end
            print(io, "</tr>")
        end
        print(io, "</tbody></table>")
        return String(take!(io))
    end

    function build(widgets::Vector{Scraper.Widget})
        mkpath(Config.PUBLIC_DIR)
        view_data = Dict{String, Any}[]
        
        for w in widgets
            # 1. Save History
            full_df = save_history(w)
            
            # 2. Prepare View (Show only latest snapshot for dashboard speed)
            # Or show full history? Let's show the data we just scraped (Fresh)
            # If you want full history on dashboard, change w.data to full_df
            view_df = w.data 
            
            id = "tbl_" * w.clean_name
            push!(view_data, Dict(
                "title" => w.name,
                "id" => id,
                "content" => df_to_html(view_df, id)
            ))
        end
        
        # 3. Render
        tpl = read(Config.TEMPLATE_FILE, String)
        out = Mustache.render(tpl, Dict(
            "time" => Utils.fmt_ist(Utils.get_ist()), 
            "tables" => view_data
        ))
        
        write(joinpath(Config.PUBLIC_DIR, "index.html"), out)
        @info "✅ Dashboard Built with $(length(widgets)) widgets."
    end
end

# ==============================================================================
# 5. 🚀 MAIN ENTRY POINT
# ==============================================================================
function main()
    try
        @info "🚀 Starting Pipeline..."
        widgets = Scraper.run()
        
        if isempty(widgets)
            @error "No widgets found! Check URLs or Selectors."
        else
            Dashboard.build(widgets)
        end
        
    catch e
        @error "Pipeline Crashed" exception=(e, catch_backtrace())
        exit(1)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__; main(); end
