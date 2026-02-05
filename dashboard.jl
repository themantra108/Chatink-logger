using CSV, DataFrames, Dates, Printf, Mustache

# ==============================================================================
# 1. 🎨 LOGIC & RULES
# ==============================================================================
const STYLE_R = "background-color: rgba(231, 76, 60, 0.5); color: white;"
const STYLE_G = "background-color: rgba(46, 204, 113, 0.5); color: white;"
const STYLE_Y = "background-color: rgba(241, 196, 15, 0.5); color: white;"

const COLOR_RULES = [
    (r"4\.5r",          v -> v > 400 ? STYLE_Y : v >= 200 ? STYLE_G : v < 50 ? STYLE_R : ""),
    (r"(4\.5|20|50)chg",v -> v < -20 ? STYLE_R : v > 20   ? STYLE_G : ""),
    (r"20r",            v -> v < 50  ? STYLE_R : v > 75   ? STYLE_G : ""),
    (r"50r",            v -> v < 60  ? STYLE_R : v > 85   ? STYLE_G : "")
]

function get_style(col::String, val)
    v = tryparse(Float64, string(val))
    isnothing(v) && return ""
    c = replace(lowercase(col), " " => "")
    for (pattern, rule) in COLOR_RULES; occursin(pattern, c) && return rule(v); end
    return ""
end

function build_html_table(df::DataFrame, id::String)
    # 1. Cleanup
    "Timestamp" in names(df) && select!(df, Not("Timestamp"))
    "Col_1" in names(df) && rename!(df, "Col_1" => "Symbol")

    # 2. Header Sanitization (Safe for JS)
    headers = names(df)
    safe_headers = String[]
    seen = Set{String}()
    for h in headers
        base = replace(string(h), r"[^a-zA-Z0-9\s_\-\%\.]" => "")
        safe = base
        c = 1
        while safe in seen; safe = "$(base)_$c"; c += 1; end
        push!(seen, safe); push!(safe_headers, safe)
    end

    # 3. Build Table HTML
    io = IOBuffer()
    print(io, "<table id='$id' class='display compact stripe nowrap' style='width:100%'><thead><tr>")
    foreach(h -> print(io, "<th>$h</th>"), safe_headers)
    print(io, "</tr></thead><tbody>")
    
    for row in eachrow(df)
        print(io, "<tr>")
        for (col, val) in pairs(row)
            sty = get_style(string(col), val)
            fmt_val = val isa Real ? @sprintf("%.2f", val) : ismissing(val) ? "-" : val
            print(io, "<td style='$sty'>$fmt_val</td>")
        end
        print(io, "</tr>")
    end
    print(io, "</tbody></table>")
    return String(take!(io))
end

# ==============================================================================
# 2. 🚀 MAIN EXECUTION
# ==============================================================================
function main()
    mkpath("public")
    data_dir = "chartink_data"
    tables = Dict{String, String}[]

    files = []
    if isdir(data_dir)
        for (root, _, fs) in walkdir(data_dir)
            append!(files, joinpath.(root, filter(endswith(".csv"), fs)))
        end
    end

    for file in files
        try
            df = CSV.read(file, DataFrame; strict=false, silencewarnings=true)
            
            # Validation Filters
            (nrow(df) == 0 || ncol(df) < 2) && continue
            fv = string(df[1,1])
            (occursin("#", fv) || occursin("*", fv) || occursin("Clause", fv)) && continue
            
            clean_name = replace(basename(file), ".csv" => "")
            id = "tbl_" * replace(clean_name, r"[^a-zA-Z0-9]" => "")
            
            println("Processing: $clean_name")
            push!(tables, Dict(
                "title" => clean_name, 
                "id" => id, 
                "content" => build_html_table(df, id)
            ))
        catch e
            @warn "Skipping $file: $e"
        end
    end

    # Render Final Page using External Template
    # We read the HTML file from disk instead of hardcoding it
    template_str = try read("dashboard_template.html", String) catch; "" end
    
    if isempty(template_str)
        println("⚠️ Error: dashboard_template.html not found! Run the setup first.")
        return
    end

    ist_time = Dates.format(now(Dates.UTC) + Hour(5) + Minute(30), "yyyy-mm-dd I:MM p") * " IST"
    
    # Mustache injects the {{tables}} list into the HTML file
    final_html = render(template_str, Dict("time" => ist_time, "tables" => tables))
    
    write("public/index.html", final_html)
    println("✅ Dashboard Generated.")
end

main()
