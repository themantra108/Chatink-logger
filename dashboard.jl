using CSV, DataFrames, Dates, Printf
using Mustache

# ==============================================================================
# 1. 🎨 STYLE CONSTANTS & TEMPLATE
# ==============================================================================
const COL_RED    = "background-color: rgba(231, 76, 60, 0.5); color: white;"
const COL_GREEN  = "background-color: rgba(46, 204, 113, 0.5); color: white;"
const COL_YELLOW = "background-color: rgba(241, 196, 15, 0.5); color: white;"

# Signal Strength Colors (First Column)
const SIGNAL_STRONG_GREEN = "background-color: rgba(46, 204, 113, 0.9) !important; color: white; font-weight: bold;"
const SIGNAL_STRONG_RED   = "background-color: rgba(231, 76, 60, 0.9) !important; color: white; font-weight: bold;"

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
        .card { background: #1e1e1e; border: 1px solid #333; border-radius: 8px; padding: 10px; margin-bottom: 20px; overflow: hidden; }
        h3 { margin-top: 0; color: #bb86fc; border-bottom: 1px solid #333; padding-bottom: 10px; }
        .timestamp { text-align: center; color: #4db6ac; margin-bottom: 20px; font-weight: bold; }
        
        /* Sticky Headers & Columns */
        table.dataTable thead th { background-color: #2c2c2c !important; color: #fff; z-index: 100; position: sticky; top: 0; }
        table.dataTable tbody tr td:first-child { position: sticky; left: 0; z-index: 50; border-right: 2px solid #444; color: #fff; font-weight: 500; }
        
        /* Zebra Striping */
        table.dataTable tbody tr.even td:first-child { background-color: #1e1e1e; }
        table.dataTable tbody tr.odd td:first-child { background-color: #2a2a2a; }
        table.dataTable tbody tr.even { background-color: #1e1e1e !important; }
        table.dataTable tbody tr.odd { background-color: #2a2a2a !important; }

        /* Formatting */
        table.dataTable td, table.dataTable th { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 300px; padding: 8px 10px; border-bottom: 1px solid #333; }
        table.dataTable { width: 100% !important; border-collapse: separate; border-spacing: 0; }
        
        /* Resizable Scroll */
        .dataTables_scrollBody { resize: vertical !important; border-bottom: 3px solid #444; transform: translateZ(0); -webkit-overflow-scrolling: touch; }
        .dataTables_scrollBody::-webkit-scrollbar { width: 12px; height: 12px; }
        .dataTables_scrollBody::-webkit-scrollbar-thumb { background: #444; border-radius: 6px; }
    </style>
    <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
    <script src="https://cdn.datatables.net/1.13.4/js/jquery.dataTables.min.js"></script>
    <script src="https://cdn.datatables.net/fixedcolumns/4.2.2/js/dataTables.fixedColumns.min.js"></script>
</head>
<body>
    <div style="text-align:center"><h1>📊 Market Dashboard</h1><div class="timestamp">Last Updated: {{last_updated}}</div></div>
    {{#tables}}
    <div class="card"><h3>{{title}}</h3>{{{content}}}</div>
    <script>
        $(document).ready(function () {
            if (document.getElementById('{{id}}')) {
                try {
                    $('#{{id}}').DataTable({
                        "order": [], "paging": false, "info": false, "deferRender": false, "processing": false,
                        "scrollX": true, "scrollY": "50vh", "scrollCollapse": true, "fixedColumns": { left: 1 },
                        "stripeClasses": ['odd', 'even'], "language": { "search": "", "searchPlaceholder": "Search..." }
                    });
                } catch(e) {}
            }
        });
    </script>
    {{/tables}}
</body>
</html>
"""

# ==============================================================================
# 2. 🧠 BUSINESS LOGIC (The "Signal" Engine)
# ==============================================================================

# A Struct to hold the result of our analysis
struct Signal
    score::Int      # +1, 0, or -1
    style::String   # CSS string
end

# Default constructor for neutral cells
Signal() = Signal(0, "")

function analyze_signal(col_name::String, val)::Signal
    # 1. Parse Number safely
    num = tryparse(Float64, string(val))
    if isnothing(num); return Signal(); end

    # 2. Clean Column Name
    col = replace(lowercase(col_name), " " => "")

    # 3. Apply Rules (The Business Logic)
    
    # Rule: 4.5r
    if occursin("4.5r", col)
        if num > 400
            return Signal(1, COL_YELLOW) # Yellow visually, but +1 score
        elseif num >= 200
            return Signal(1, COL_GREEN)
        elseif num < 50
            return Signal(-1, COL_RED)
        end
        
    # Rule: % Changes
    elseif any(x -> occursin(x, col), ["4.5chg", "20chg", "50chg"])
        if num > 20
            return Signal(1, COL_GREEN)
        elseif num < -20
            return Signal(-1, COL_RED)
        end
        
    # Rule: 20r
    elseif occursin("20r", col)
        if num > 75
            return Signal(1, COL_GREEN)
        elseif num < 50
            return Signal(-1, COL_RED)
        end

    # Rule: 50r
    elseif occursin("50r", col)
        if num > 85
            return Signal(1, COL_GREEN)
        elseif num < 60
            return Signal(-1, COL_RED)
        end
    end

    # Default Neutral
    return Signal()
end

# ==============================================================================
# 3. 🏗️ BUILDER
# ==============================================================================
get_ist() = now(Dates.UTC) + Hour(5) + Minute(30)

function build_table_html(df::DataFrame, id::String)
    if "Timestamp" in names(df); select!(df, Not("Timestamp")); end
    
    io = IOBuffer()
    # Write Header
    println(io, """<table id="$id" class="display compact stripe nowrap" style="width:100%">""")
    println(io, "<thead><tr>")
    
    col_names = names(df)
    seen_headers = Set{String}()
    
    for col in col_names
        # Allow dots in headers
        base_name = replace(string(col), r"[^a-zA-Z0-9\s_\-\%\.]" => "")
        safe_col = base_name
        count = 1
        while safe_col in seen_headers
            count += 1
            safe_col = "$(base_name)_$count"
        end
        push!(seen_headers, safe_col)
        println(io, "<th>$safe_col</th>")
    end
    println(io, "</tr></thead><tbody>")
    
    # Write Rows
    for row in eachrow(df)
        # Pass 1: Calculate Total Score for this row
        row_total_score = 0
        row_signals = Vector{Signal}(undef, length(col_names))
        
        for (i, col) in enumerate(col_names)
            sig = analyze_signal(string(col), row[col])
            row_signals[i] = sig
            row_total_score += sig.score
        end
        
        println(io, "<tr>")
        
        # Pass 2: Render Cells
        for (i, col) in enumerate(col_names)
            val = row[col]
            style = row_signals[i].style # Use pre-calculated style
            
            # OVERRIDE: First Column Signal Strength
            if i == 1
                if row_total_score >= 3
                    style = SIGNAL_STRONG_GREEN
                elseif row_total_score <= -3
                    style = SIGNAL_STRONG_RED
                else
                    style = "" # Default to CSS (Stripe color)
                end
            end
            
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
                    try
                        df = CSV.read(joinpath(root, file), DataFrame; strict=false, silencewarnings=true)
                        if nrow(df) > 0 && ncol(df) >= 2
                            push!(table_list, Dict(
                                "title" => clean_name, "id" => id, 
                                "content" => build_table_html(df, id)
                            ))
                        end
                    catch; end
                end
            end
        end
    end
    
    ist_time = get_ist()
    final_html = render(DASHBOARD_TEMPLATE, Dict(
        "last_updated" => Dates.format(ist_time, "yyyy-mm-dd I:MM p") * " IST",
        "tables" => table_list
    ))
    open("public/index.html", "w") do io; write(io, final_html); end
    println("✅ Dashboard generated.")
end

main()
