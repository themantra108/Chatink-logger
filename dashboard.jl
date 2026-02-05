using CSV, DataFrames, Dates, Printf
using Mustache

# ==============================================================================
# 1. 📄 THE TEMPLATE (Performance Tuned)
# ==============================================================================
const DASHBOARD_TEMPLATE = mt"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>Chartink Pro (Fast)</title>
    
    <link rel="stylesheet" href="https://cdn.datatables.net/1.13.4/css/jquery.dataTables.min.css">
    <link rel="stylesheet" href="https://cdn.datatables.net/fixedcolumns/4.2.2/css/fixedColumns.dataTables.min.css">
    
    <style>
        body { font-family: -apple-system, sans-serif; background: #121212; color: #e0e0e0; padding: 10px; }
        
        /* PERFORMANCE: 'contain' isolates the card rendering */
        .card { 
            background: #1e1e1e; 
            border: 1px solid #333; 
            border-radius: 8px; 
            padding: 10px; 
            margin-bottom: 20px; 
            box-shadow: 0 4px 6px rgba(0,0,0,0.4); 
            contain: content; 
        }
        h3 { margin-top: 0; color: #bb86fc; border-bottom: 1px solid #333; padding-bottom: 10px; }
        
        .timestamp { 
            text-align: center; color: #4db6ac; font-size: 0.9rem; font-weight: bold;
            margin-bottom: 20px; border: 1px solid #333; display: inline-block;
            padding: 5px 15px; border-radius: 20px; background: #1a1a1a;
        }
        .header-container { text-align: center; }

        table.dataTable { font-size: 0.85rem; }
        table.dataTable td { padding: 6px 8px; border-bottom: 1px solid #2d2d2d; }
        table.dataTable thead th { background-color: #2c2c2c; border-bottom: 2px solid #555; }
        
        /* Force GPU Acceleration for scrolling */
        .dataTables_scrollBody {
            -webkit-overflow-scrolling: touch;
            will-change: transform;
        }
    </style>
    
    <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
    <script src="https://cdn.datatables.net/1.13.4/js/jquery.dataTables.min.js"></script>
    <script src="https://cdn.datatables.net/fixedcolumns/4.2.2/js/dataTables.fixedColumns.min.js"></script>
</head>
<body>
    <div class="header-container">
        <h1>📊 Market Dashboard</h1>
        <div class="timestamp">Last Updated: {{last_updated}}</div>
    </div>

    {{#tables}}
    <div class="card">
        <h3>{{title}}</h3>
        {{{content}}}
    </div>
    <script>
        $(document).ready(function () {
            $.fn.dataTable.ext.errMode = 'none';

            if (document.getElementById('{{id}}')) {
                try {
                    $('#{{id}}').DataTable({
                        "order": [[ 0, "desc" ]],
                        "pageLength": 25,
                        
                        // ⚡ PERFORMANCE SETTINGS ⚡
                        "deferRender": true,    // CRITICAL: Only render visible HTML
                        "processing": true,     // Show "Processing..." indicator
                        "orderClasses": false,  // Don't highlight sorted columns (Slow CSS)
                        "autoWidth": false,     // Disable expensive math
                        
                        // Scroll Settings
                        "scrollX": true,
                        "scrollY": "60vh",
                        "scrollCollapse": true,
                        
                        // Mobile Sticky Column
                        "fixedColumns": { left: 1 },
                        
                        "language": { "search": "", "searchPlaceholder": "Search..." }
                    });
                } catch(e) { console.log("Error: " + e); }
            }
        });
    </script>
    {{/tables}}
</body>
</html>
"""

# ==============================================================================
# 2. 🎨 UTILS
# ==============================================================================
get_ist() = now(Dates.UTC) + Hour(5) + Minute(30)

function get_cell_style(val)
    try
        num = val isa Number ? val : parse(Float64, string(val))
        if num > 0; return "background-color: rgba(46, 204, 113, 0.3); color: white;"
        elseif num < 0; return "background-color: rgba(231, 76, 60, 0.3); color: white;"
        end
    catch; end
    return ""
end

function is_valid_table(df::DataFrame)
    if nrow(df) == 0; return false, "Empty Dataset (0 Rows)"; end
    if ncol(df) < 2; return false, "Malformed Data (< 2 Columns)"; end
    return true, "OK"
end

# ==============================================================================
# 3. 🏗️ BUILDER
# ==============================================================================
function build_table_html(df::DataFrame, id::String)
    io = IOBuffer()
    println(io, """<table id="$id" class="display compact stripe nowrap" style="width:100%">""")
    println(io, "<thead><tr>")
    
    seen_headers = Set{String}()
    for col in names(df)
        base_name = replace(string(col), r"[^a-zA-Z0-9\s_\-\%]" => "")
        safe_col = base_name
        count = 1
        while safe_col in seen_headers
            count += 1
            safe_col = "$(base_name)_$count"
        end
        push!(seen_headers, safe_col)
        println(io, "<th>$safe_col</th>")
    end
    println(io, "</tr></thead>")
    
    println(io, "<tbody>")
    for row in eachrow(df)
        println(io, "<tr>")
        for col in names(df)
            val = row[col]
            style = get_cell_style(val)
            clean_val = val isa Real ? @sprintf("%.2f", val) : val
            if ismissing(clean_val); clean_val = "-"; end
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
                        df = CSV.read(joinpath(root, file), DataFrame; strict=false, silencewarnings=true)
                        valid, msg = is_valid_table(df)
                        
                        if valid
                            content = build_table_html(df, id)
                            push!(table_list, Dict("title" => clean_name, "id" => id, "content" => content))
                        else
                            @warn "Skipping $file"
                        end
                    catch e; println("Error: $e"); end
                end
            end
        end
    end
    
    ist_time = get_ist()
    formatted_time = Dates.format(ist_time, "yyyy-mm-dd I:MM p") * " IST"
    
    final_html = render(DASHBOARD_TEMPLATE, Dict(
        "last_updated" => formatted_time,
        "tables" => table_list
    ))
    
    open("public/index.html", "w") do io; write(io, final_html); end
    println("✅ Fast Dashboard generated.")
end

main()
