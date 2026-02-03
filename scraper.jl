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

# 📜 NEW: Smart Scroll Function
function smart_scroll(page)
    @info "📜 Scrolling to trigger lazy-loading widgets..."
    
    # Get page height
    h_res = ChromeDevToolsLite.evaluate(page, "document.body.scrollHeight")
    height = safe_unwrap(h_res)
    if !isa(height, Number); height = 5000; end
    
    # Scroll in steps
    current_scroll = 0
    step = 800
    while current_scroll < height
        current_scroll += step
        ChromeDevToolsLite.evaluate(page, "window.scrollTo(0, $current_scroll)")
        sleep(0.5) # Wait for trigger
    end
    
    # Scroll back to top just in case
    ChromeDevToolsLite.evaluate(page, "window.scrollTo(0, 0)")
    sleep(2) # Final stabilization wait
    @info "✅ Scroll complete."
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
        
        # 🟢 UPGRADE: Run Smart Scroll
        smart_scroll(page)

        @info "⚡ DOM Ready. Preparing Payload..."
        
        # 1️⃣ THE JAVASCRIPT PAYLOAD
        raw_js_logic = """
        (() => {
            try {
                window._chartinkData = "";
                // Select ALL tables (now that we scrolled)
                const tables = document.querySelectorAll("table");
                
                // Debug Info
                console.log("Found tables: " + tables.length);
                
                if (tables.length === 0) {
                    window._chartinkData = "NO DATA FOUND";
                    return "NODATA";
                }
                
                let allRows = [];

                tables.forEach((table, index) => {
                    let widgetName = "Unknown Widget " + index;
                    let current = table;
                    let depth = 0;
                    try {
                        // Sibling Hunter Logic
                        while (current && depth < 6) {
                            let sibling = current.previousElementSibling;
                            let foundTitle = false;
                            for (let i = 0; i < 5; i++) {
                                if (!sibling) break;
                                let text = sibling.innerText ? sibling.innerText.trim() : "";
                                // Heuristic: Widget titles are short and don't contain "Loading"
                                if (text.length > 0 && text.length < 100 && !text.includes("Loading") && !text.includes("Error")) {
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
                return "DONE (Found " + tables.length + " tables)";

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

        # 3️⃣ DATAFRAME PROCESSING ENGINE
        @info "💾 DataFrames Engine: Deduplicating and Merging..."
        
        output_dir = "chartink_data"
        mkpath(output_dir)
        
        current_time = get_ist()
        lines = split(full_data, "\n")
        
        widget_buffers = Dict{String, Vector{String}}()
        
        for line in lines
            if length(line) < 5; continue; end
            
            m = match(r"^\"([^\"]+)\"", line)
            if m === nothing; continue; end
            widget_name = m.captures[1]
            
            safe_name = replace(widget_name, r"\s+" => "_")
            safe_name = replace(safe_name, r"[^a-zA-Z0-9_\-]" => "")
            
            if !haskey(widget_buffers, safe_name)
                widget_buffers[safe_name] = String[]
            end
            push!(widget_buffers[safe_name], line)
        end
        
        @info "📊 Found $(length(widget_buffers)) unique widgets."

        for (safe_name, rows) in widget_buffers
            file_path = joinpath(output_dir, safe_name * ".csv")
            
            header_row_idx = findfirst(x -> occursin("Symbol", x), rows)
            if header_row_idx === nothing
                # fallback: take first row if no "Symbol" found, or skip?
                # Let's assume row 1 is header if explicit match fails, but safe check is better
                if length(rows) > 0
                     header_row_idx = 1
                else
                     continue
                end
            end
            
            io_buf = IOBuffer()
            println(io_buf, "\"Timestamp\"," * rows[header_row_idx])
            for (i, row) in enumerate(rows)
                if i == header_row_idx; continue; end
                println(io_buf, "\"$(current_time)\"," * row)
            end
            
            seekstart(io_buf)
            try
                df_new = CSV.read(io_buf, DataFrame)
                if isempty(df_new); continue; end

                if isfile(file_path)
                    try
                        df_old = CSV.read(file_path, DataFrame)
                        df_combined = vcat(df_old, df_new, cols=:union)
                        unique!(df_combined, [:Timestamp, :Symbol])
                        sort!(df_combined, :Timestamp)
                        CSV.write(file_path, df_combined)
                    catch e
                        @warn "Overwrite: Old file $file_path corrupted."
                        CSV.write(file_path, df_new)
                    end
                else
                    CSV.write(file_path, df_new)
                end
            catch e
                @warn "CSV Parse Error: $safe_name"
            end
        end
        
        @info "✅ Scrape Cycle Complete."

    catch e
        @error "💥 Scraper Failed" exception=(e, catch_backtrace())
        exit(1)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
