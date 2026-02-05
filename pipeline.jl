using ChromeDevToolsLite, DataFrames, CSV, Dates, JSON, Printf, Mustache, Gumbo, Cascadia

# ==============================================================================
# 1. 🛠️ CORE UTILS
# ==============================================================================
module Utils
    using Dates
    get_ist() = now(Dates.UTC) + Hour(5) + Minute(30)
    fmt_ist(dt) = Dates.format(dt, "yyyy-mm-dd I:MM p") * " IST"

    function clean_filename(s::AbstractString)
        s = replace(s, r"[^a-zA-Z0-9\-\.]" => "_")
        s = replace(s, r"__+" => "_")
        return strip(s, '_')
    end

    function clean_header(h::AbstractString)
        # Kill "Sort table by..." and all its variants
        h = replace(h, r"(?i)Sort\s*(table|tab|column).*$" => "")
        h = replace(h, r"[^a-zA-Z0-9%\.]" => "")
        return strip(h)
    end
    
    function clean_text(s::AbstractString)
        s = replace(s, r"\s+" => " ")
        return strip(s)
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
# 3. 🕸️ SCRAPER (PURE JULIA EDITION)
# ==============================================================================
module Scraper
    using ..Utils, ..Config
    using ChromeDevToolsLite, DataFrames, CSV, Dates, JSON, Gumbo, Cascadia

    struct Widget
        dashboard::String
        name::String
        clean_name::String
        data::DataFrame
    end

    function is_valid_title(t::AbstractString)
        length(t) < 2 && return false
        length(t) > 80 && return false
        low = lowercase(t)
        startswith(low, "note") && return false
        startswith(low, "disclaimer") && return false
        occursin("delayed", low) && return false
        occursin("generated", low) && return false
        # If it contains "Sort table by", it's garbage
        occursin("sort table", low) && return false
        return true
    end

    function hunt_title(table_node::HTMLNode)
        curr = table_node
        for _ in 1:15
            parent = getattr(curr, "parent", nothing)
            if isnothing(parent) || !isa(parent, HTMLElement); break; end
            
            # 1. Check Container Classes (Card/Portlet)
            if haskey(parent.attributes, "class")
                cls = parent.attributes["class"]
                if occursin("card", cls) || occursin("portlet", cls) || occursin("widget", cls)
                    headers = eachmatch(Selector(".card-header, .portlet-title, .widget-header, h3, h4, .caption"), parent)
                    for h in headers
                        htxt = Utils.clean_text(nodeText(h))
                        if is_valid_title(htxt); return htxt; end
                    end
                end
            end

            # 2. Check Previous Siblings (Aggressive Text Scan)
            siblings = parent.children
            idx = findfirst(==(curr), siblings)
            if !isnothing(idx)
                for i in (idx-1):-1:1
                    sib = siblings[i]
                    if !isa(sib, HTMLElement); continue; end
                    
                    # Check the sibling text directly
                    # If it's a Header tag OR Bold tag OR just a Div/Span with text
                    tag_name = uppercase(string(tag(sib)))
                    
                    if tag_name in ["H1","H2","H3","H4","B","STRONG","DIV","SPAN","P"]
                        txt = Utils.clean_text(nodeText(sib))
                        if is_valid_title(txt)
                            # Extra check: If it's a P/Div/Span, ensure it looks like a title (short, capitalized)
                            if tag_name in ["DIV","SPAN","P"] && length(txt) > 50; continue; end
                            return txt
                        end
                    end
                    
                    # Check inside the sibling
                    nested_headers = eachmatch(Selector("h1, h2, h3, h4, b, strong, .card-title, .caption-subject"), sib)
                    for nh in nested_headers
                        ntxt = Utils.clean_text(nodeText(nh))
                        if is_valid_title(ntxt); return ntxt; end
                    end
                end
            end
            curr = parent
        end
        return "Unknown_Widget"
    end

    function extract_table_data(table_node::HTMLElement)
        rows = eachmatch(Selector("tr"), table_node)
        if isempty(rows); return DataFrame(); end
        
        header_cells = eachmatch(Selector("th, td"), rows[1])
        if isempty(header_cells); return DataFrame(); end
        
        raw_headers = [Utils.clean_text(nodeText(c)) for c in header_cells]
        col_names = map(Utils.clean_header, raw_headers)
        
        df = DataFrame()
        for col in col_names
            safe_col = col
            c = 1
            while safe_col in names(df) || isempty(safe_col)
                safe_col = isempty(col) ? "Col_$c" : "$(col)_$c"
                c += 1
            end
            df[!, safe_col] = String[]
        end
        
        ts = Utils.get_ist()
        
        for i in 2:length(rows)
            cells = eachmatch(Selector("td, th"), rows[i])
            if length(cells) < length(col_names); continue; end
            
            row_data = Dict{String, Any}()
            skip_row = false
            
            for (j, cell) in enumerate(cells)
                if j > length(col_names); break; end
                txt = Utils.clean_text(nodeText(cell))
                
                # Filter out junk rows
                if j == 1 && txt == col_names[1]; skip_row = true; break; end
                if occursin("No data", txt); skip_row = true; break; end
                
                row_data[names(df)[j]] = txt
            end
            
            if !skip_row; push!(df, row_data); end
        end
        
        insertcols!(df, 1, :Timestamp => fill(string(ts), nrow(df)))
        return df
    end

    function is_valid_df(df::DataFrame)
        nrow(df) > 0 && ncol(df) >= 3 
    end

    function get_dashboard_name(page)
        raw = ChromeDevToolsLite.evaluate(page, "document.title")
        val = isa(raw, Dict) ? raw["value"] : raw
        clean = replace(val, r"(?i)\s*-\s*Chartink.*" => "")
        return Utils.clean_filename(clean)
    end

    function process_page(page, url)
        @info "🧭 Navigating: $url"
        Utils.@retry 3 5 begin
            ChromeDevToolsLite.goto(page, url)
        end
        sleep(8) 
        
        dash_folder = get_dashboard_name(page)
        @info "📂 Dashboard: $dash_folder"

        h = ChromeDevToolsLite.evaluate(page, "document.body.scrollHeight")
        h_val = isa(h, Dict) ? h["value"] : h
        for s in 0:1000:h_val
            ChromeDevToolsLite.evaluate(page, "window.scrollTo(0, $s)")
            sleep(0.2) 
        end
        sleep(2)

        html_content = ChromeDevToolsLite.evaluate(page, "document.documentElement.outerHTML")
        html_str = isa(html_content, Dict) ? html_content["value"] : html_content
        dom = parsehtml(html_str)
        tables = eachmatch(Selector("table"), dom.root)
        
        widgets = Widget[]
        seen_titles = Dict{String, Int}()

        for t in tables
            title = hunt_title(t)
            df = extract_table_data(t)
            
            if is_valid_df(df)
                if haskey(seen_titles, title)
                    seen_titles[title] += 1
                    title = "$(title)_$(seen_titles[title])"
                else
                    seen_titles[title] = 1
                end
                
                clean_id = Utils.clean_filename(title)
                push!(widgets, Widget(dash_folder, title, clean_id, df))
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
        
        io = IOBuffer()
        print(io, "<div style='overflow-x:auto'><table id='$id' class='display compact stripe nowrap' style='width:100%'><thead><tr>")
        foreach(h -> print(io, "<th>$h</th>"), safe_headers)
        print(io, "</tr></thead><tbody>")
        
        for row in eachrow(df)
            print(io, "<tr>")
            for (col, val) in pairs(row)
                sty = get_style(col, val)
                raw_val = ismissing(val) ? "-" : val
                fmt = raw_val
                if raw_val isa Real
                     fmt = @sprintf("%.2f", raw_val)
                elseif raw_val isa String && tryparse(Float64, raw_val) !== nothing
                     fmt = @sprintf("%.2f", parse(Float64, raw_val))
                end
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
