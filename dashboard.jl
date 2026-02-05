using CSV, DataFrames, Dates, Printf

# ==============================================================================
# 1. 🎨 THEME & STYLING (The "Heatmap" Logic)
# ==============================================================================
const CSS = """
<style>
    body { font-family: -apple-system, sans-serif; background: #1a1a1a; color: #e0e0e0; padding: 20px; }
    h1 { color: #9b59b6; }
    .card { background: #2d2d2d; border-radius: 8px; padding: 15px; margin-bottom: 20px; box-shadow: 0 4px 6px rgba(0,0,0,0.3); }
    table { width: 100%; border-collapse: collapse; font-size: 0.9rem; }
    th, td { padding: 8px 12px; text-align: right; border-bottom: 1px solid #404040; }
    th { text-align: left; background: #333; color: #aaa; position: sticky; top: 0; }
    tr:hover { filter: brightness(1.2); }
    .timestamp { font-size: 0.8rem; color: #666; margin-bottom: 10px; }
    
    /* DataTables Overrides */
    .dataTables_wrapper .dataTables_length, .dataTables_wrapper .dataTables_filter, 
    .dataTables_wrapper .dataTables_info, .dataTables_wrapper .dataTables_paginate {
        color: #aaa !important;
    }
    input, select { background: #404040 !important; border: none; color: white !important; padding: 5px; border-radius: 4px; }
</style>
"""

# Helper to map a value to a Green/Red color scale
function get_color(val::Number)
    # Customize your Logic Here! 
    # Example: Positive = Green, Negative = Red
    if val > 0
        intensity = min(abs(val) * 10, 150) + 50 # Scale intensity
        return "rgba(46, 204, 113, $(intensity/255))" # Green
    elseif val < 0
        intensity = min(abs(val) * 10, 150) + 50
        return "rgba(231, 76, 60, $(intensity/255))"  # Red
    end
    return "transparent"
end

get_color(val) = "transparent" # Fallback for strings

# ==============================================================================
# 2. 🏗️ HTML GENERATOR
# ==============================================================================

function generate_html_table(df::DataFrame, table_id::String)
    io = IOBuffer()
    
    # Start Table
    println(io, """<table id="$table_id" class="display">""")
    
    # Header
    println(io, "<thead><tr>")
    for col in names(df)
        println(io, "<th>$col</th>")
    end
    println(io, "</tr></thead>")
    
    # Body
    println(io, "<tbody>")
    for row in eachrow(df)
        println(io, "<tr>")
        for col in names(df)
            val = row[col]
            
            # 🔥 APPLY COLOR CODING LOGIC HERE
            # Try to parse as number for heatmap
            num_val = tryparse(Float64, string(val))
            bg_style = isnothing(num_val) ? "" : "style='background-color: $(get_color(num_val)); color: white;'"
            
            clean_val = val
            if val isa Real; clean_val = @sprintf("%.2f", val); end
            
            println(io, "<td $bg_style>$clean_val</td>")
        end
        println(io, "</tr>")
    end
    println(io, "</tbody></table>")
    
    return String(take!(io))
end

function main()
    # 1. Setup Public Folder
    mkpath("public")
    
    # 2. Find CSV Files
    data_dir = "chartink_data"
    html_content = IOBuffer()
    
    println(html_content, """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Chartink Dashboard</title>
        <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.13.4/css/jquery.dataTables.min.css">
        $CSS
        <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
        <script src="https://cdn.datatables.net/1.13.4/js/jquery.dataTables.min.js"></script>
    </head>
    <body>
        <h1>📊 Market Dashboard</h1>
        <div class="timestamp">Last Updated: $(now()) UTC</div>
    """)
    
    # 3. Process Each Folder/CSV
    if isdir(data_dir)
        for (root, dirs, files) in walkdir(data_dir)
            for file in files
                if endswith(file, ".csv")
                    clean_name = replace(file, ".csv" => "")
                    id = replace(clean_name, r"[^a-zA-Z]" => "")
                    
                    println("Processing $file ...")
                    
                    try
                        df = CSV.read(joinpath(root, file), DataFrame)
                        
                        # Generate Table HTML
                        tbl_html = generate_html_table(df, id)
                        
                        println(html_content, """
                        <div class="card">
                            <h3>$clean_name</h3>
                            $tbl_html
                        </div>
                        <script>
                            \$(document).ready(function () {
                                \$('#$id').DataTable({
                                    "order": [[ 0, "desc" ]], # Sort by first column (Date)
                                    "pageLength": 25
                                });
                            });
                        </script>
                        """)
                    catch e
                        println("Error processing $file: $e")
                    end
                end
            end
        end
    end
    
    println(html_content, "</body></html>")
    
    # 4. Write Index.html
    open("public/index.html", "w") do io
        write(io, String(take!(html_content)))
    end
    println("✅ Dashboard generated in public/index.html")
end

main()
