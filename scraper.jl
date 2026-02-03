using ChromeDevToolsLite
using Dates
using JSON

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
        
        # 1️⃣ THE JAVASCRIPT PAYLOAD
        # Returns: "WidgetName","Symbol","Cmp"...
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
                    
                    // --- Sibling Hunter ---
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

                    // --- Row Extraction ---
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
        
        # Secure Transport
        safe_payload = JSON.json(raw_js_logic)
        transport_js = "eval($safe_payload)"
        
        result = ChromeDevToolsLite.evaluate(page, transport_js)
        status = safe_unwrap(result)
        @info "🛠️ JS Setup Status: $status"

        if status == "ERROR" || (isa(status, String) && startswith(status, "JS_ERROR"))
             err_res = ChromeDevToolsLite.evaluate(page, "window._chartinkData")
             err_msg = safe_unwrap(err_res)
             @warn "⚠️ JS setup crashed: $err_msg"
             return
        end

        # 2️⃣ FETCH FULL DATA
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

        # 3️⃣ SPLIT & SAVE LOGIC (New!) 📂
        @info "💾 Processing & Splitting Files..."
        
        # Ensure directory exists
        output_dir = "chartink_data"
        mkpath(output_dir)
        
        current_time = get_ist()
        lines = split(full_data, "\n")
        
        stats = Dict{String, Int}()
        
        for line in lines
            if length(line) < 5; continue; end
            
            # Extract Widget Name (First Column)
            # Regex looks for the first quoted string: "Widget Name"
            m = match(r"^\"([^\"]+)\"", line)
            if m === nothing; continue; end
            
            widget_name = m.captures[1]
            
            # Sanitize Filename (Mod 5_day_Check -> Mod_5_day_Check.csv)
            safe_name = replace(widget_name, r"\s+" => "_")
            safe_name = replace(safe_name, r"[^a-zA-Z0-9_\-]" => "")
            file_path = joinpath(output_dir, safe_name * ".csv")
            
            # Check if this row is a Header (2nd col is Symbol)
            is_header = occursin(r"^\"[^\"]+\",\"Symbol\"", line)
            
            # Write Logic
            if !isfile(file_path)
                # New File: Create it
                open(file_path, "w") do io
                    if is_header
                        println(io, "\"Timestamp\"," * line)
                    else
                        # Orphan data row? Add header manually if needed, or just dump
                        println(io, "\"$(current_time)\"," * line)
                    end
                end
            else
                # Existing File: Append
                if is_header
                    # Skip headers for existing files
                    continue
                end
                open(file_path, "a") do io
                    println(io, "\"$(current_time)\"," * line)
                end
            end
            
            stats[safe_name] = get(stats, safe_name, 0) + 1
        end
        
        @info "✅ Done! Stats: $stats"

    catch e
        @error "💥 Scraper Failed" exception=(e, catch_backtrace())
        exit(1)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
