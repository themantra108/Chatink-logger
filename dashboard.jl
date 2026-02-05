using CSV, DataFrames, Dates, Printf
using Mustache

# ==============================================================================
# 1. 📄 THE TEMPLATE (Striped & Rule-Based)
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
        
        .card { 
            background: #1e1e1e; 
            border: 1px solid #333; 
            border-radius: 8px; 
            padding: 10px; 
            margin-bottom: 20px;
            overflow: hidden; 
        }
        h3 { margin-top: 0; color: #bb86fc; border-bottom: 1px solid #333; padding-bottom: 10px; }
        .timestamp { text-align: center; color: #4db6ac; margin-bottom: 20px; font-weight: bold; }
        .header-container { text-align: center; }

        /* --- STICKY HEADERS --- */
        table.dataTable thead th { 
            background-color: #2c2c2c !important; 
            color: #ffffff;
            border-bottom: 2px solid #555;
            z-index: 100;
            position: sticky;
            top: 0;
        }

        /* --- ALTERNATING ROW COLORS (ZEBRA STRIPING) --- */
        
        /* 1. Even Rows (Darker) */
        table.dataTable tbody tr.even {
            background-color: #1e1e1e !important; 
        }
        table.dataTable tbody tr.even td:first-child {
            background-color: #1e1e1e !important; /* Sticky col matches row */
        }

        /* 2. Odd Rows (Lighter) */
        table.dataTable tbody tr.odd {
            background-color: #2a2a2a !important; 
        }
        table.dataTable tbody tr.odd td:first-child {
            background-color: #2a2a2a !important; /* Sticky col matches row */
        }

        /* --- STICKY COLUMN POSITIONING --- */
        table.dataTable tbody tr td:first-child {
            position: sticky;
            left: 0;
            z-index: 50;
            border-right: 2px solid #444; /* Subtle separator */
            color: #ffffff;
            font-weight: 500;
        }

        /* --- CELL FORMATTING --- */
        table.dataTable td, table.dataTable th {
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            max-width: 300px;
        }
        table.dataTable td { padding: 8px 10px; border-bottom: 1px solid #333; }
        table.dataTable { border-collapse: separate; border-spacing: 0; width: 100% !important; }

        /* --- SCROLLBARS --- */
        .dataTables_scrollBody {
            resize: vertical !important; 
            border-bottom: 3px solid #444; 
            transform: translateZ(0);
            -webkit-overflow-scrolling: touch; 
        }
        .dataTables_scrollBody::-webkit-scrollbar { width: 12px; height: 12px; }
        .dataTables_scrollBody::-webkit-scrollbar-track { background: #1a1a1a; }
        .dataTables_scrollBody::-webkit-scrollbar-thumb { background: #444; border-radius: 6px; border: 2px solid #1a1a1a; }
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
                        "order": [],
                        "paging": false,
                        "info": false,
                        "deferRender": false,
                        "processing": false,
                        "scrollX": true,
                        "scrollY": "50vh", 
                        "scrollCollapse": true,
                        "fixedColumns": { left: 1 },
                        "stripeClasses": ['odd', 'even'], // Enforce classes
                        "language": { "search": "", "searchPlaceholder": "Search..." }
                    });
                } catch(e) { console.log(e); }
            }
        });
    </script>
    {{/tables}}
</body>
</html>
"""

# ==============================================================================
# 2. 🧠 COLOR RULES (Specific Only)
# ==============================================================================
const COL_RED    = "background-color: rgba(231, 76, 60, 0.5); color: white;"
const COL_GREEN  = "background-color: rgba(46, 204, 113, 0.5); color: white;"
const COL_YELLOW = "background-color: rgba(241, 196, 15, 0.5); color: white;"

function get_cell_style(col_name::String, val)
    num = tryparse(Float64, string(val))
    if isnothing(num); return ""; end

    col = replace(lowercase(string(col_name)), " " => "")

    # --- ONLY YOUR RULES BELOW ---
    
    # Rule: 4.5r
    if occursin("4.5r", col)
        if num > 400; return COL_YELLOW;
        elseif num >= 200; return COL_GREEN;
        elseif num < 50; return COL_RED; end

    # Rule: Change Columns
    elseif any(x -> occursin(x, col), ["4.5chg", "20chg", "50chg"])
        if num < -20; return COL_RED;
        elseif num > 20; return COL_GREEN; end

    # Rule: 20r
    elseif occursin("20r", col)
        if num < 50; return COL_RED;
        elseif num > 75; return COL_GREEN; end

    # Rule: 50r
    elseif occursin("50r", col)
        if num < 60; return COL_RED;
        elseif num > 85; return COL_GREEN; end
    end
    
    # NO GENERIC FALLBACK -> Other cells stay transparent (showing stripe color)
    return ""
end

get_ist() = now(Dates.UTC) + Hour(5) + Minute(30)

function is_valid_table(df::DataFrame)
    if nrow(df) == 0; return false, "Empty Dataset (0 Rows)"; end
    if ncol(df) < 2; return false, "Malformed Data (< 2 Columns)"; end
    return true, "OK"
end

# ==============================================================================
# 3. 🏗️ BUILDER
# ==============================================================================
function build_table_html(df::DataFrame, id::String)
    if "Timestamp" in names(df); select!(df, Not("Timestamp")); end

    io = IOBuffer()
    # Note: 'stripe' class enables the CSS selectors we wrote above
    println(io, """<table id="$id" class="display compact stripe nowrap" style="width:100%">""")
    println(io, "<thead><tr>")
    
    seen_headers = Set{String}()
    for col in names(df)
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
    println(io, "</tr></thead>")
    
    println(io, "<tbody>")
    for row in eachrow(df)
        println(io, "<tr>")
        for col in names(df)
            val = row[col]
            style = get_cell_style(string(col), val)
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
    println("✅ Dashboard generated (Striped Rows).")
end

main()