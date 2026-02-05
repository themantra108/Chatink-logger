using CSV, DataFrames, Dates, Printf
using Mustache

# ==============================================================================
# 1. 📄 THE TEMPLATE
# ==============================================================================
const DASHBOARD_TEMPLATE = mt"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>Chartink Pro</title>
    
    <link rel="stylesheet" href="https://cdn.datatables.net/1.13.4/css/jquery.dataTables.min.css">
    <link rel="stylesheet" href="https://cdn.datatables.net/fixedcolumns/4.2.2/css/fixedColumns.dataTables.min.css">
    
    <style>
        body { font-family: -apple-system, sans-serif; background: #121212; color: #e0e0e0; padding: 10px; }
        .card { background: #1e1e1e; border: 1px solid #333; border-radius: 8px; padding: 10px; margin-bottom: 20px; box-shadow: 0 4px 6px rgba(0,0,0,0.4); }
        h3 { margin-top: 0; color: #bb86fc; border-bottom: 1px solid #333; padding-bottom: 10px; }
        .timestamp { text-align: center; color: #666; font-size: 0.8rem; margin-bottom: 20px; }

        /* Table Styles */
        table.dataTable { font-size: 0.85rem; }
        table.dataTable td { padding: 6px 8px; border-bottom: 1px solid #2d2d2d; }
        table.dataTable thead th { background-color: #2c2c2c; border-bottom: 2px solid #555; }
    </style>
    
    <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
    <script src="https://cdn.datatables.net/1.13.4/js/jquery.dataTables.min.js"></script>
    <script src="https://cdn.datatables.net/fixedcolumns/4.2.2/js/dataTables.fixedColumns.min.js"></script>
</head>
<body>
    <h1 style="text-align:center">📊 Market Dashboard</h1>
    <div class="timestamp">Last Updated: {{last_updated}}</div>

    {{#tables}}
    <div class="card">
        <h3>{{title}}</h3>
        {{{content}}}
    </div>
    <script>
        $(document).ready(function () {
            $('#{{id}}').DataTable({
                "order": [[ 0, "desc" ]],
                "pageLength": 25,
                "scrollX": true,
                "scrollY": "60vh",
                "scrollCollapse": true,
                "fixedColumns": { left: 1 },
                "language": { "search": "", "searchPlaceholder": "Search..." }
            });
        });
    </script>
    {{/tables}}
</body>
</html>
"""

# ==============================================================================
# 2. 🎨 COLOR LOGIC
# ==============================================================================
function get_cell_style(val)
    # Try to parse as number
    try
        num = val isa Number ? val : parse(Float64, string(val))
        if num > 0
            # Green (with transparency)
            return "background-color: rgba(46, 204, 113, 0.3); color: white;"
        elseif num < 0
            # Red (with transparency)
            return "background-color: rgba(231, 76, 60, 0.3); color: white;"
        end
    catch
        # Not a number, ignore
    end
    return ""
end

# ==============================================================================
# 3. 🏗️ BUILDER (No Dependencies)
# ==============================================================================
function build_table_html(df::DataFrame, id::String)
    io = IOBuffer()
    # Write Table Header
    println(io, """<table id="$id" class="display compact stripe nowrap" style="width:100%">""")
    println(io, "<thead><tr>")
    for col in names(df)
        println(io, "<th>$col</th>")
    end
    println(io, "</tr></thead>")
    
    # Write Body
    println(io, "<tbody>")
    for row in eachrow(df)
        println(io, "<tr>")
        for col in names(df)
            val = row[col]
            style = get_cell_style(val)
            
            # Format numbers neatly
            clean_val = val
            if val isa Real
                 clean_val = @sprintf("%.2f", val)
            end
            
            println(io, "<td style='$style'>$clean_val</td>")
        end
        println(io, "</tr>")
    end
    println(io, "</tbody></table>")
    return String(take!(io))
end

function main()
    mkpath("public")
    data_dir = "chartink_data"
    table_list = Dict{String, String}[]
    
    if isdir(data_dir)
        for (root, dirs, files) in walkdir(data_dir)
            for file in files
                if endswith(file, ".csv")
                    clean_name = replace(file, ".csv" => "")
                    id = "tbl_" * replace(clean_name, r"[^a-zA-Z0-9]" => "")
                    
                    println("Processing $file ...")
                    try
                        df = CSV.read(joinpath(root, file), DataFrame)
                        
                        # Generate HTML Manually (Safe & Fast)
                        html_content = build_table_html(df, id)
                        
                        push!(table_list, Dict(
                            "title" => clean_name,
                            "id" => id,
                            "content" => html_content
                        ))
                    catch e
                        println("Error: $e")
                    end
                end
            end
        end
    end
    
    final_html = render(DASHBOARD_TEMPLATE, Dict(
        "last_updated" => string(now(Dates.UTC)) * " UTC",
        "tables" => table_list
    ))
    
    open("public/index.html", "w") do io
        write(io, final_html)
    end
    println("✅ Dashboard generated (Manual Mode).")
end

main()
