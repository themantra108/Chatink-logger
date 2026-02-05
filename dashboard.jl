using CSV, DataFrames, Dates, Printf, Mustache

# ==============================================================================
# 1. 🎨 RULES & STYLES
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
    for (pattern, rule) in COLOR_RULES
        occursin(pattern, c) && return rule(v)
    end
    return ""
end

# ==============================================================================
# 2. 🧱 HTML GENERATION
# ==============================================================================
const TEMPLATE = mt"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Chartink Pro</title>
    <link rel="stylesheet" href="https://cdn.datatables.net/1.13.4/css/jquery.dataTables.min.css">
    <link rel="stylesheet" href="https://cdn.datatables.net/fixedcolumns/4.2.2/css/fixedColumns.dataTables.min.css">
    <style>
        body { font-family: -apple-system, sans-serif; background: #121212; color: #e0e0e0; padding: 10px; }
        .card { background: #1e1e1e; border: 1px solid #333; border-radius: 8px; padding: 10px; margin-bottom: 20px; overflow: hidden; }
        h3 { color: #bb86fc; border-bottom: 1px solid #333; padding-bottom: 10px; margin-top: 0; }
        .ts { text-align: center; color: #4db6ac; margin-bottom: 20px; font-weight: bold; }
        
        table.dataTable thead th { background: #2c2c2c !important; color: #fff; position: sticky; top: 0; z-index: 100; }
        table.dataTable tbody tr td:first-child { position: sticky; left: 0; z-index: 50; border-right: 2px solid #444; font-weight: 500; }
        table.dataTable tbody tr.even, table.dataTable tbody tr.even td:first-child { background: #1e1e1e !important; }
        table.dataTable tbody tr.odd, table.dataTable tbody tr.odd td:first-child { background: #2a2a2a !important; }
        table.dataTable td { white-space: nowrap; max-width: 300px; overflow: hidden; text-overflow: ellipsis; padding: 8px 10px; border-bottom: 1px solid #333; }
        .dataTables_scrollBody { resize: vertical !important; border-bottom: 3px solid #444; min-height: 200px; }
    </style>
    <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
    <script src="https://cdn.datatables.net/1.13.4/js/jquery.dataTables.min.js"></script>
    <script src="https://cdn.datatables.net/fixedcolumns/4.2.2/js/dataTables.fixedColumns.min.js"></script>
</head>
<body>
    <div style="text-align:center"><h1>📊 Market Dashboard</h1><div class="ts">Last Updated: {{time}}</div></div>
    {{#tables}}
    <div class="card"><h3>{{title}}</h3>{{{content}}}</div>
    <script>
        $(document).ready(() => {
            $.fn.dataTable.ext.errMode = 'none';
            if(document.getElementById('{{id}}')) {
                try { $('#{{id}}').DataTable({ paging: false, info: false, scrollX: true, scrollY: '50vh', scrollCollapse: true, fixedColumns: {left: 1}, stripeClasses: ['odd','even'] }); } catch(e){}
            }
        });
    </script>
    {{/tables}}
</body>
</html>
"""

function build_html_table(df::DataFrame, id::String)
    # Cleanup Columns
    "Timestamp" in names(df) && select!(df, Not("Timestamp"))
    
    # 🩹 FIX: Rename generic "Col_1" to "Symbol" (Fixes Backtest/History tables)
    if "Col_1" in names(df)
        rename!(df, "Col_1" => "Symbol")
    end

    # Header Sanitization
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
# 3. 🚀 MAIN PIPELINE
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
        println("Processing $file...")
        try
            df = CSV.read(file, DataFrame; strict=false, silencewarnings=true)
            
            # 🛑 CRITICAL FILTERS
            (nrow(df) == 0 || ncol(df) < 2) && continue
            
            # 1. Kill the "Clause" table (Junk data)
            first_val = string(df[1,1])
            (occursin("#", first_val) || occursin("*", first_val) || occursin("Clause", first_val)) && continue
            
            clean_name = replace(basename(file), ".csv" => "")
            id = "tbl_" * replace(clean_name, r"[^a-zA-Z0-9]" => "")
            
            push!(tables, Dict(
                "title" => clean_name, 
                "id" => id, 
                "content" => build_html_table(df, id)
            ))
        catch e
            @warn "Failed $file: $e"
        end
    end

    ist_time = Dates.format(now(Dates.UTC) + Hour(5) + Minute(30), "yyyy-mm-dd I:MM p") * " IST"
    write("public/index.html", render(TEMPLATE, Dict("time" => ist_time, "tables" => tables)))
    println("✅ Dashboard Generated (Junk Removed).")
end

main()
