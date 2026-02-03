using ChromeDevToolsLite
using Dates
using JSON
using DataFrames
using CSV

get_ist() = now(Dates.UTC) + Hour(5) + Minute(30)

# 🛡️ Safe Unwrap
function safe_unwrap(result)
    if isa(result, Dict)
        if haskey(result, "value")
            return result["value"]
        elseif haskey(result, "result")
            inner = result["result"]
            if isa(inner, Dict) && haskey(inner, "value")
                return inner["value"]
            end
        elseif haskey(result, "description")
            return "JS_ERROR: " * result["description"]
        end
    end
    return result
end

function wait_for_selector(page, selector; timeout=60, poll_interval=1)
    start_time = time()
    while time() - start_time < timeout
        check_js = "document.querySelector('$selector') !== null"
        result = ChromeDevToolsLite.evaluate(page, check_js)
        val = safe_unwrap(result)
        if val == true
            return true
        end
        sleep(poll_interval)
    end
    throw(ErrorException("Timeout waiting for selector: $selector"))
end

function main()
    @info "🚀 Julia Scraper: Initializing..."
    page = nothing
    
    try
        page = ChromeDevToolsLite.connect_browser() 
        @info "✅ Chrome Connected."

        target_url = "https://chartink.com/dashboard/208896"
        ChromeDevToolsLite.goto(page, target_url)
        @info "🧭 Navigated to dashboard."

        @info "👀 Watching DOM for tables..."
        wait_for_selector(page, "table"; timeout=60) 
        sleep(5) 

        @info "⚡ DOM Ready. Preparing Payload..."
        
        # 1️⃣ THE JAVASCRIPT PAYLOAD (Same as before)
        raw_js_logic = """
        (() => {
            try {
                window._chartinkData = "";
                const tables = document.querySelectorAll("table");
                if (tables.length === 0) {
                    window._chartinkData = "NO DATA FOUND";
                    return "NODATA";
                }
                
                let allRows = [];

                tables.forEach(table => {
                    let widgetName = "Unknown Widget";
                    let current = table;
                    let depth = 0;
                    try {
                        while (current && depth < 6) {
                            let sibling = current.previousElementSibling;
                            let foundTitle = false;
                            for (let i = 0; i < 5; i++) {
                                if (!sibling) break;
                                let text = sibling.innerText ? sibling.innerText.trim() : "";
                                if (text.length > 0 && !text.includes("Loading") && !text.includes("Error")) {
                                    widgetName = text.split('\\n')[0].trim();
                                    foundTitle = true;
                                    break;
                                }
                                sibling = sibling.previousElementSibling;
                            }
                            if (foundTitle) break;
                            current = current.parentElement;
                            depth++;
                        }
                    } catch (err) {}

                    const rows = table.querySelectorAll("tr");
                    const processedRows = Array.from(rows).map(row => {
                        const cells = row.querySelectorAll("th, td");
                        if (cells.length === 0) return null;

                        const isHeader = row.querySelector("th") !== null;
                        const rowText = row.innerText || "";

                        if (rowText.includes("No data for table") || rowText.includes("Clause")) return null;

                        const safeWidget = widgetName.replace(/"/g, '""');

                        const cellData = Array.from(cells).map(c => {
                            let text = c.innerText ? c.innerText.trim() : "";
                            if (isHeader) {
                                text = text.split('\\n')[0].trim();
                                text = text.replace(/Sort table by/gi, "").trim();
                            }
                            text = text.replace(/"/g, '""');
                            return '"' + text + '"';
                        }).join(",");

                        return '"' + safeWidget + '",' + cellData;
                    });

                    allRows = allRows.concat(processedRows.filter(r => r));
                });

                window._chartinkData = allRows.join("\\n");
                return "DONE";

            } catch (e) {
                window._chartinkData = "JS_CRASH: " + e.toString();
                return "ERROR";
            }
        })()
        """
        
        safe_payload = JSON.json(raw_js_logic)
        transport_js = "eval($safe_payload)"
        
        result = ChromeDevToolsLite.evaluate(page, transport_js)
        status = safe_unwrap(result)
        @info "🛠️ JS Setup Status: $status"

        if status == "ERROR" || (isa(status, String) && startswith(status, "JS_ERROR"))
             return
        end

        # 2️⃣ FETCH DATA
        @info "📦 Fetching Data..."
        len_res = ChromeDevToolsLite.evaluate(page, "window._chartinkData.length")
        total_len = safe_unwrap(len_res)
        
        if !isa(total_len, Int)
             total_len = try parse(Int, string(total_len)) catch; 0 end
        end

        if total_len == 0
            @warn "⚠️ No data found."
            return
        end

        full_data = ""
        chunk_size = 50000
        current_idx = 0
        
        while current_idx < total_len
            end_idx = min(current_idx + chunk_size, total_len)
            chunk_js = "window._chartinkData.substring($current_idx, $end_idx)"
            chunk_res = ChromeDevToolsLite.evaluate(page, chunk_js)
            chunk_val = safe_unwrap(chunk_res)
            full_data = full_data * string(chunk_val)
            current_idx += chunk_size
        end

        # 3️⃣ DATAFRAME PROCESSING ENGINE 🧠
        @info "💾 DataFrames Engine: Deduplicating and Merging..."
        
        output_dir = "chartink_data"
        mkpath(output_dir)
        
        current_time = get_ist()
        lines = split(full_data, "\n")
        
        # Buffer to hold raw text for each widget
        # Key: WidgetName, Value: Vector of CSV Strings (Header + Data)
        widget_buffers = Dict{String, Vector{String}}()
        
        for line in lines
            if length(line) < 5; continue; end
            
            # Identify Widget
            m = match(r"^\"([^\"]+)\"", line)
            if m === nothing; continue; end
            widget_name = m.captures[1]
            
            # Sanitize Name
            safe_name = replace(widget_name, r"\s+" => "_")
            safe_name = replace(safe_name, r"[^a-zA-Z0-9_\-]" => "")
            
            if !haskey(widget_buffers, safe_name)
                widget_buffers[safe_name] = String[]
            end
            
            push!(widget_buffers[safe_name], line)
        end
        
        # Process each widget
        for (safe_name, rows) in widget_buffers
            file_path = joinpath(output_dir, safe_name * ".csv")
            
            # Separate Header and Data
            # First row in our logic is mostly headers, but we need to be sure.
            # We construct a pure CSV string to parse with CSV.read
            
            # We prepend the timestamp column to the header and data
            # Header check:
            header_row_idx = findfirst(x -> occursin("Symbol", x), rows)
            if header_row_idx === nothing
                @warn "Skipping $safe_name: No header found."
                continue
            end
            
            # Construct IO Buffer for New Data
            # We manually reconstruct the CSV to add Timestamp
            io_buf = IOBuffer()
            
            # Write Header
            println(io_buf, "\"Timestamp\"," * rows[header_row_idx])
            
            # Write Data (Skip header row in loop)
            for (i, row) in enumerate(rows)
                if i == header_row_idx; continue; end
                println(io_buf, "\"$(current_time)\"," * row)
            end
            
            # Turn into DataFrame
            seekstart(io_buf)
            try
                df_new = CSV.read(io_buf, DataFrame)
                
                if isempty(df_new)
                    continue
                end

                # Merge with Old Data
                if isfile(file_path)
                    try
                        df_old = CSV.read(file_path, DataFrame)
                        
                        # Fix column types if they mismatch (convert all to string mostly safe)
                        # or rely on CSV.jl smart detection.
                        # We allow missing columns to handle schema changes
                        df_combined = vcat(df_old, df_new, cols=:union)
                        
                        # 🦄 DEDUPLICATION MAGIC
                        # Unique by Timestamp + Symbol + Strategy (implicitly handled by file)
                        unique!(df_combined, [:Timestamp, :Symbol])
                        
                        # Sort
                        sort!(df_combined, :Timestamp)
                        
                        # Write back
                        CSV.write(file_path, df_combined)
                    catch e
                        @warn "Error reading old file $file_path. Overwriting." exception=e
                        CSV.write(file_path, df_new)
                    end
                else
                    CSV.write(file_path, df_new)
                end
                
                @info "✅ Processed: $safe_name ($(nrow(df_new)) new rows)"
                
            catch e
                @warn "Failed to parse CSV for $safe_name" exception=e
            end
        end

    catch e
        @error "💥 Scraper Failed" exception=(e, catch_backtrace())
        exit(1)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
