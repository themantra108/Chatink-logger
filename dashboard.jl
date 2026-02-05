using CSV, DataFrames, Dates, Printf

# ==============================================================================
# 1. 🎨 THEME: RESPONSIVE & DARK
# ==============================================================================
const CSS = """
<style>
    /* --- Base Styles --- */
    body { 
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, sans-serif; 
        background: #121212; 
        color: #e0e0e0; 
        margin: 0; 
        padding: 10px; /* Reduced padding for mobile */
    }
    h1 { color: #bb86fc; font-size: 1.5rem; margin: 10px 0 5px 0; text-align: center; }
    .timestamp { text-align: center; color: #666; font-size: 0.8rem; margin-bottom: 20px; }
    
    /* --- Card Container --- */
    .card { 
        background: #1e1e1e; 
        border: 1px solid #333;
        border-radius: 8px; 
        padding: 10px; 
        margin-bottom: 20px; 
        box-shadow: 0 4px 6px rgba(0,0,0,0.4); 
        overflow: hidden; /* Contains the scroll */
    }
    h3 { margin-top: 0; color: #eee; font-size: 1.1rem; border-bottom: 1px solid #333; padding-bottom: 10px; }

    /* --- DataTable Customization --- */
    table.dataTable { 
        font-size: 0.85rem; 
        border-collapse: separate; /* Required for sticky columns */
    }
    table.dataTable tbody td { padding: 6px 8px; border-bottom: 1px solid #2d2d2d; }
    table.dataTable thead th { 
        padding: 8px 10px; 
        background: #2c2c2c; 
        border-bottom: 2px solid #555; 
        white-space: nowrap; 
    }
    
    /* Color coding text logic */
    td { font-feature-settings: "tnum"; font-variant-numeric: tabular-nums; } /* Monospace numbers */

    /* --- MOBILE OPTIMIZATIONS (The Julia Way) --- */
    @media screen and (max-width: 768px) {
        body { padding: 5px; }
        .card { padding: 5px; border-radius: 4px; }
        h1 { font-size: 1.2rem; }
        
        /* Force horizontal scroll container to be smooth on touch */
        .dataTables_scrollBody {
            -webkit-overflow-scrolling: touch; 
        }
        
        /* Adjust font for density */
        table.dataTable { font-size: 0.75rem; }
        table.dataTable tbody td { padding: 4px 5px; }
        
        /* Hide non-essential controls on tiny screens */
        .dataTables_length { display: none; } 
    }
    
    /* Input Styling */
    .dataTables_filter input { background: #333; border: none; color: white; padding: 5px; border-radius: 4px; }
    .dataTables_wrapper .dataTables_paginate .paginate_button { color: #888 !important; }
    .dataTables_wrapper .dataTables_paginate .paginate_button.current { background: #444 !important; border: none; color: white !important; }
</style>
"""

# Heatmap Logic
function get_color(val::Number)
    if val > 0
        intensity = min(abs(val) * 10, 150) + 50
        return "rgba(46, 204, 113, $(intensity/255))"
    elseif val < 0
        intensity = min(abs(val) * 10, 150) + 50
        return "rgba(231, 76, 60, $(intensity/255))"
    end
    return "transparent"
end
get_color(val) = "transparent"

# ==============================================================================
# 2. 🏗️ HTML GENERATOR
# ==============================================================================

function generate_html_table(df::DataFrame, table_id::String)
    io = IOBuffer()
    # "compact" = less padding, "stripe" = easier to read rows
    println(io, """<table id="$table_id" class="display compact stripe nowrap" style="width:100%">""")
    
    println(io, "<thead><tr>")
    for col in names(df); println(io, "<th>$col</th>"); end
    println(io, "</tr></thead>")
    
    println(io, "<tbody>")
    for row in eachrow(df)
        println(io, "<tr>")
        for col in names(df)
            val = row[col]
            num_val = tryparse(Float64, string(val))
            bg_style = isnothing(num_val) ? "" : "style='background-color: $(get_color(num_val)); color: white;'"
            clean_val = val isa Real ? @sprintf("%.2f", val) : val
            println(io, "<td $bg_style>$clean_val</td>")
        end
        println(io, "</tr>")
    end
    println(io, "</tbody></table>")
    return String(take!(io))
end

function main()
    mkpath("public")
    data_dir = "chartink_data"
    html_content = IOBuffer()
    
    # Header with Mobile Viewport Meta Tag
    println(html_content, """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
        <title>Chartink Mobile</title>
        
        <link rel="stylesheet" href="https://cdn.datatables.net/1.13.4/css/jquery.dataTables.min.css">
        <link rel="stylesheet" href="https://cdn.datatables.net/fixedcolumns/4.2.2/css/fixedColumns.dataTables.min.css">
        $CSS
        
        <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
        <script src="https://cdn.datatables.net/1.13.4/js/jquery.dataTables.min.js"></script>
        <script src="https://cdn.datatables.net/fixedcolumns/4.2.2/js/dataTables.fixedColumns.min.js"></script>
    </head>
    <body>
        <h1>📊 Market Dashboard</h1>
        <div class="timestamp">Last Updated: $(now()) UTC</div>
    """)
    
    if isdir(data_dir)
        for (root, dirs, files) in walkdir(data_dir)
            for file in files
                if endswith(file, ".csv")
                    clean_name = replace(file, ".csv" => "")
                    id = "tbl_" * replace(clean_name, r"[^a-zA-Z0-9]" => "") 
                    
                    println("Processing $file ...")
                    try
                        df = CSV.read(joinpath(root, file), DataFrame)
                        tbl_html = generate_html_table(df, id)
                        
                        # JS Logic: Fixed First Column + Horizontal Scroll
                        println(html_content, """
                        <div class="card">
                            <h3>$clean_name</h3>
                            $tbl_html
                        </div>
                        <script>
                            \$(document).ready(function () {
                                \$('#$id').DataTable({
                                    "order": [[ 0, "desc" ]],
                                    "pageLength": 25,
                                    "scrollX": true,             // 📱 Vital for mobile
                                    "scrollY": "60vh",           // 📱 60% of screen height
                                    "scrollCollapse": true,
                                    "paging": true,
                                    "fixedColumns": {
                                        left: 1                  // ⚓ Locks the 1st column (Stock Name/Date)
                                    },
                                    "language": {
                                        "search": "",            // Minimalist search box
                                        "searchPlaceholder": "Search..."
                                    }
                                });
                            });
                        </script>
                        """)
                    catch e; println("Error: $e"); end
                end
            end
        end
    end
    
    println(html_content, "</body></html>")
    open("public/index.html", "w") do io; write(io, String(take!(html_content))); end
    println("✅ Mobile-Ready Dashboard generated.")
end

main()
