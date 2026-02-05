using ChromeDevToolsLite, DataFrames, CSV, Dates, JSON, Printf, Mustache

# ==============================================================================
# 1. 🛠️ CORE UTILS
# ==============================================================================
module Utils
    using Dates
    get_ist() = now(Dates.UTC) + Hour(5) + Minute(30)
    fmt_ist(dt) = Dates.format(dt, "yyyy-mm-dd I:MM p") * " IST"

    function clean_filename(s::String)
        s = replace(s, r"[^a-zA-Z0-9\-\.]" => "_")
        s = replace(s, r"__+" => "_")
        return strip(s, '_')
    end

    function clean_header(h::String)
        # Aggressive cleaning of "Sort table by..." artifacts
        h = replace(h, r"(?i)(Sort\s*(table|tab).*)" => "")
        h = replace(h, r"[^a-zA-Z0-9%\.]" => "")
        return strip(h)
    end

    macro retry(n, delay, expr)
        quote
            local result, success = nothing, false
            for i in 1:$n
                try
                    result = $(esc(expr)); success = true; break
                catch e; sleep($delay); end
            end
            if !success; error("Failed after $($n) attempts"); end
            result
        end
    end
end

# ==============================================================================
# 2. 🧱 CONFIG
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
# 3. 🕸️ SCRAPER
# ==============================================================================
module Scraper
    using ..Utils, ..Config
    using ChromeDevToolsLite, DataFrames, CSV, Dates, JSON

    struct Widget
        dashboard::String
        name::String
        clean_name::String
        data::DataFrame
    end

    # 🔥 TITLE EXTRACTOR v4 (The "Sniper")
    const EXTRACTOR_JS = """
    (() => {
        const clean = t => t ? t.trim().replace(/"/g, '""').replace(/\\n/g, " ") : "";
        let csv = [];
        let seenTitles = {};

        const isValidTitle = (t) => {
            if (!t || t.length < 2 || t.length > 80) return false;
            const lower = t.toLowerCase();
            if (lower.startsWith("note")) return false;
            if (lower.startsWith("disclaimer")) return false;
            if (lower.includes("delayed")) return false;
            if (lower.includes("generated")) return false;
            return true;
        };

        const findTitle = (node) => {
            // Strategy 1: Look for explicit Chartink widget containers
            let container = node.closest('.card, .widget, .panel, .box, .portlet');
            if(container) {
                // Try to find the header inside this container
                let candidates = container.querySelectorAll('.card-header, .panel-heading, .widget-header, .portlet-title, h3, h4, .caption');
                for (let c of candidates) {
                    if (isValidTitle(c.innerText)) return c.innerText;
                }
            }

            // Strategy 2: Walk backwards in the DOM tree (The "Scanner")
            let curr = node;
            let depth = 0;
            while(curr && depth++ < 25) {
                let sib = curr.previousElementSibling;
                while(sib) {
                    // Check the sibling itself
                    if ((/H[1-6]|B|STRONG/.test(sib.tagName) || sib.classList.contains('font-bold')) && isValidTitle(sib.innerText)) {
                        return sib.innerText;
                    }
                    
                    // Check children of the sibling (e.g., a div containing an h3)
                    let inner = sib.querySelector('h1, h2, h3, h4, b, strong, .card-title, .caption-subject');
                    if(inner && isValidTitle(inner.innerText)) return inner.innerText;
                    
                    sib = sib.previousElementSibling;
                }
                curr = curr.parentElement;
                if (!curr || curr.tagName === 'BODY') break;
            }
            return "Unknown_Widget";
        };

        const processTable = (table, rawTitle) => {
            let baseTitle = clean(rawTitle);
            
            // If title is bad, hunt for a new one
            if (!baseTitle || baseTitle.includes("Misc_Table") || !isValidTitle(baseTitle)) {
                 baseTitle = clean(findTitle(table));
            }

            // Fallback: Use First Column Header (Last Resort)
            if ((!baseTitle || baseTitle.includes("Unknown")) && table.rows.length > 0) {
                 const firstTh = table.querySelector("th");
                 if(firstTh) {
                    let txt = clean(firstTh.innerText).replace(/sort.*by/i, "").trim();
                    if(txt.length > 2) baseTitle = "Widget_" + txt.substring(0, 15);
                    else baseTitle = "Widget_" + Math.floor(Math.random() * 1000);
                 }
            }

            // 🧼 Final Clean: Remove "Sort table by" from the title itself if it leaked in
            baseTitle = baseTitle.replace(/Sort\\s*table\\s*by.*/i, "").trim();
            baseTitle = baseTitle.replace(/Sort\\s*tab.*/i, "").trim();

            if (seenTitles[baseTitle]) {
                seenTitles[baseTitle]++;
                baseTitle = baseTitle + "_" + seenTitles[baseTitle];
            } else {
                seenTitles[baseTitle] = 1;
            }

            const rows = table.querySelectorAll("tr");
            rows.forEach(row => {
                if(row.innerText.includes("No data")) return;
                const cells = Array.from(row.querySelectorAll("th, td"));
                if(cells.length < 2) return;
                
                const rowStr = cells.map(c => '"' + clean(c.innerText) + '"').join(",");
                csv.push('"' + baseTitle + '",' + rowStr);
            });
        };

        // Main Loop
        document.querySelectorAll("table").forEach(table => {
            // Check specific card header first (High Confidence)
            let card = table.closest('.card, .portlet');
            let initialTitle = "";
            if(card) {
                let h = card.querySelector(".card-header, .portlet-title");
                if(h) initialTitle = h.innerText;
            }
            processTable(table, initialTitle);
        });
        
        return csv.join("\\n");
    })()
    """

    function is_valid(df::DataFrame)
        nrow(df) > 0 && ncol(df) >= 2 && !occursin(r"(#|\*|Clause)", string(df[1,1]))
    end

    function get_dashboard_name(page)
        raw = ChromeDevToolsLite.evaluate(page, "document.title")
        val = isa(raw, Dict) ? raw["value"] : raw
        clean = replace(val, r"(?i)\s*-\s*Chartink.*" => "")
        clean = Utils.clean_filename(clean)
        return isempty(clean) ? "Unknown_Dashboard" : clean
    end

    function process_page(page, url)
        @info "🧭 Navigating: $url"
        Utils.@retry 3 5 begin
            ChromeDevToolsLite.goto(page, url)
        end
        sleep(8) 
        
        dash_folder = get_dashboard_name(page)
        @info "📂 Dashboard Detected: $dash_folder"

        h = ChromeDevToolsLite.evaluate(page, "document.body.scrollHeight")
        h_val = isa(h, Dict) ? h["value"] : h
        for s in 0:1000:h_val
            ChromeDevToolsLite.evaluate(page, "window.scrollTo(0, $s)")
            sleep(0.5) 
        end
        sleep(2)

        raw_res = ChromeDevToolsLite.evaluate(page, EXTRACTOR_JS)
        raw_csv = isa(raw_res, Dict) ? raw_res["value"] : raw_res
        
        if isempty(raw_csv); return Widget[]; end
        
        widgets = Widget[]
        rows = split(raw_csv, "\n")
        grouped = Dict{String, Vector{String}}()
        
        for r in rows
            m = match(r"^\"([^\"]+)\"", r)
            k = isnothing(m) ? "Unknown" : m.captures[1]
            push!(get!(grouped, k, String[]), r)
        end

        for (title, lines) in grouped
            clean_lines = map(l -> replace(l, r"^\"[^\"]+\"," => ""), lines)
            header_idx = findfirst(l -> occursin(r"\"(Symbol|Name|Date|Scan Name)\"", l), clean_lines)
            if isnothing(header_idx) && length(clean_lines) > 0; header_idx = 1; end
            isnothing(header_idx) && continue

            raw_header = clean_lines[header_idx]
            cleaned_header = replace(raw_header, r"(?i)Sort\s*table\s*by[^,\"]*" => "")
            
            io = IOBuffer()
            println(io, "\"Timestamp\"," * cleaned_header)
            ts = Utils.get_ist()
            for i in (header_idx+1):length(clean_lines)
                if clean_lines[i] != raw_header
                    println(io, "\"$ts\"," * clean_lines[i])
                end
            end
            
            try
                seekstart(io)
                df = CSV.read(io, DataFrame; strict=false, silencewarnings=true)
                if is_valid(df)
                    clean_id = Utils.clean_filename(title)
                    push!(widgets, Widget(dash_folder, title, clean_id, df))
                end
            catch e
                @warn "Parsing failed: $title"
            end
        end
        return widgets
    end

    function run()
        page = ChromeDevToolsLite.connect_browser()
        widgets = Widget[]
        for url in Config.TARGETS; append!(widgets, process_page(page, url)); end
        return widgets
    end
end

# ==============================================================================
# 4. 📊 DASHBOARD
# ==============================================================================
module Dashboard
    using ..Utils, ..Config, ..Scraper
    using DataFrames, CSV, Printf, Mustache

    # 🔥 FLEXIBLE RULES (Catch spacing variations like "4.5 r")
    const RULES = [
        (r"(?i)4\.5\s*r",          v -> v > 400 ? "background:#f1c40f80" : v >= 200 ? "background:#2ecc7180" : v < 50 ? "background:#e74c3c80" : ""),
        (r"(?i)(4\.5|20|50)\s*chg",v -> v < -20 ? "background:#e74c3c80" : v > 20   ? "background:#2ecc7180" : ""),
        (r"(?i)20\s*r",            v -> v < 50  ? "background:#e74c3c80" : v > 75   ? "background:#2ecc7180" : ""),
        (r"(?i)50\s*r",            v -> v < 60  ? "background:#e74c3c80" : v > 85   ? "background:#2ecc7180" : "")
    ]

    function get_style(col, val)
        v = tryparse(Float64, string(val))
        isnothing(v) && return ""
        c = replace(string(col), " " => "")
        for (p, f) in RULES; occursin(p, c) && return f(v); end
        return ""
    end

    function save_history(w::Scraper.Widget)
        folder_path = joinpath(Config.DATA_DIR, w.dashboard)
        mkpath(folder_path)
        path = joinpath(folder_path, w.clean_name * ".csv")
        
        final_df = w.data
        if isfile(path)
            old_df = CSV.read(path, DataFrame)
            if "Column22" in names(old_df); select!(old_df, Not("Column22")); end
            final_df = vcat(w.data, old_df, cols=:union)
            cols = intersect(["Timestamp", "Symbol", "Date"], names(final_df))
            !isempty(cols) && unique!(final_df, cols)
        end
        if nrow(final_df) > 5000; final_df = first(final_df, 5000); end
        CSV.write(path, final_df)
        return final_df
    end

    function df_to_html(df::DataFrame, id::String)
        "Timestamp" in names(df) && select!(df, Not("Timestamp"))
        "Col_1" in names(df) && rename!(df, "Col_1" => "Symbol")
        select!(df, Not(filter(n -> occursin(r"Column\d+", string(n)), names(df))))
        
        headers = names(df)
        safe_headers = map(Utils.clean_header, headers)
        
        # 🔥 FIX: Ensure ID is clean and class is forced
        io = IOBuffer()
        print(io, "<div style='overflow-x:auto'><table id='$id' class='display compact stripe nowrap' style='width:100%'><thead><tr>")
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
        print(io, "</tbody></table></div>")
        return String(take!(io))
    end

    function build(widgets::Vector{Scraper.Widget})
        mkpath(Config.PUBLIC_DIR)
        view_data = Dict{String, Any}[]
        
        grouped_widgets = Dict{String, Vector{Scraper.Widget}}()
        for w in widgets; push!(get!(grouped_widgets, w.dashboard, Scraper.Widget[]), w); end

        for (dash_name, dash_widgets) in grouped_widgets
            for w in dash_widgets
                save_history(w)
                display_title = "$dash_name / $(w.name)"
                unique_id = "tbl_" * Utils.clean_filename(dash_name) * "_" * w.clean_name
                
                push!(view_data, Dict(
                    "title" => display_title,
                    "id" => unique_id,
                    "content" => df_to_html(w.data, unique_id)
                ))
            end
        end
        
        tpl = read(Config.TEMPLATE_FILE, String)
        out = Mustache.render(tpl, Dict("time" => Utils.fmt_ist(Utils.get_ist()), "tables" => view_data))
        write(joinpath(Config.PUBLIC_DIR, "index.html"), out)
        @info "✅ Built $(length(widgets)) tables."
    end
end

# ==============================================================================
# 5. 🚀 MAIN
# ==============================================================================
function main()
    try
        widgets = Scraper.run()
        isempty(widgets) ? @error("No widgets found") : Dashboard.build(widgets)
    catch e
        @error "Crash" exception=(e, catch_backtrace())
        exit(1)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__; main(); end
