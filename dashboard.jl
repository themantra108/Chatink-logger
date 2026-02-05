using CSV, DataFrames, Dates, Printf
using PrettyTables
using Mustache

# ==============================================================================
# 1. 📄 THE TEMPLATE (Mustache)
# ==============================================================================
# This separates your HTML/JS from your Julia Code. Cleaner & safer.
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

        /* PrettyTables Output Fixes */
        table.dataTable { font-size: 0.85rem; }
        table.dataTable td { padding: 6px 8px; border-bottom: 1px solid #2d2d2d; }
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
# 2. 🎨 HEATMAP LOGIC (The PrettyTables Way)
# ==============================================================================

# Helper to check if a value is a number and positive/negative
function is_pos(data, i, j)
    try
        val = data[i, j]
        return val isa Number && val > 0
    catch; return false; end
end

function is_neg(data, i, j)
    try
        val = data[i, j]
        return val isa Number && val < 0
    catch; return false; end
end

# Define Highlighters
# Green Background for Positive
hl_pos = Highlighter(
    (data, i, j) -> is_pos(data, i, j),
    HTMLDecoration(background = "rgba(46, 204, 113, 0.3)", color = "white")
)

# Red Background for Negative
hl_neg = Highlighter(
    (data, i, j) -> is_neg(data, i, j),
    HTMLDecoration(background = "rgba(231, 76, 60, 0.3)", color = "white")
)

# ==============================================================================
# 3. 🏗️ BUILD PIPELINE
# ==============================================================================

function main()
    mkpath("public")
    data_dir = "chartink_data"
    
    # Store table objects for Mustache
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
                        
                        # GENERATE HTML USING PRETTYTABLES
                        # This replaces the manual loop we wrote before.
                        html_str = pretty_table(
                            String, 
                            df; 
                            backend = Val(:html), 
                            highlighters = (hl_pos, hl_neg),
                            table_class = "display compact stripe nowrap",
                            table_style = Dict("width" => "100%"),
                            html_id = id,
                            standalone = false, # We only want the <table> tag, not <html>
                            show_row_number = false
                        )
                        
                        push!(table_list, Dict(
                            "title" => clean_name,
                            "id" => id,
                            "content" => html_str
                        ))
                    catch e
                        println("Error: $e")
                    end
                end
            end
        end
    end
    
    # RENDER THE PAGE
    # Combine the Template + The Data
    final_html = render(DASHBOARD_TEMPLATE, Dict(
        "last_updated" => string(now(Dates.UTC)) * " UTC",
        "tables" => table_list
    ))
    
    open("public/index.html", "w") do io
        write(io, final_html)
    end
    println("✅ Pro Dashboard generated.")
end

main()
