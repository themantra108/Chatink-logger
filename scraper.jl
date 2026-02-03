using ChromeDevToolsLite
using Dates

get_ist() = now(Dates.UTC) + Hour(5) + Minute(30)

function safe_unwrap(result)
    if isa(result, Dict)
        if haskey(result, "value")
            return result["value"]
        elseif haskey(result, "result")
            inner = result["result"]
            if isa(inner, Dict) && haskey(inner, "value")
                return inner["value"]
            end
        elseif haskey(result, "description") # Error description
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

        @info "⚡ DOM Ready. Running Scraper Logic..."
        
        # 1️⃣ EXECUTE SCRAPER (With Try/Catch Safety)
        setup_js = """
        (() => {
            try {
                // Initialize to empty string to prevent 'undefined' errors
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
                    
                    // Sibling Hunter (Wrapped safely)
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
                    } catch (err) {
                        console.log("Widget Name Error: " + err);
                    }
                    
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
                        return '"' + safeWidget + ""," + cellData;
                    });
                    allRows = allRows.concat(processedRows.filter(r => r));
                });
                
                window._chartinkData = allRows.join("\\n");
                return "DONE";
                
            } catch (e) {
                // 🚨 CATCH CRASHES AND SAVE ERROR
                window._chartinkData = "JS_CRASH: " + e.toString();
                return "ERROR";
            }
        })()
        """
        result = ChromeDevToolsLite.evaluate(page, setup_js)
        status = safe_unwrap(result)
        @info "🛠️ JS Setup Status: $status"

        # 2️⃣ FETCH DATA LENGTH
        @info "📦 Checking data length..."
        
        len_js = """
        (() => {
            if (typeof window._chartinkData === 'undefined') return "UNDEFINED";
            return window._chartinkData.length;
        })()
        """
        len_res = ChromeDevToolsLite.evaluate(page, len_js)
        total_len = safe_unwrap(len_res)
        
        # Handle non-integer returns (errors)
        if !isa(total_len, Int)
            # Check if it is our crash report
            data_res = ChromeDevToolsLite.evaluate(page, "window._chartinkData")
            error_msg = safe_unwrap(data_res)
            @warn "⚠️ JS Error Detected: $error_msg"
            
            # Save Debug HTML only if it's a real crash
            if isa(error_msg, String) && startswith(error_msg, "JS_CRASH")
                # Fall through to catch block to save HTML
                throw(ErrorException("JavaScript Crashed: $error_msg"))
            end
            return
        end

        @info "📊 Total Data Length: $total_len chars"
        
        if total_len == 0
            @warn "⚠️ Data length is 0."
            return
        end

        # 3️⃣ CHUNK FETCHING
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

        # 💾 Write to File
        temp_file = "new_chunk.csv"
        rows = split(full_data, "\n")
        current_time = get_ist()
        
        open(temp_file, "w") do io
            count = 0
            for row in rows
                if length(row) > 10 
                    println(io, "\"$(current_time)\",$row")
                    count += 1
                end
            end
            @info "✅ Success! Captured $count rows."
        end

    catch e
        @error "💥 Scraper Failed" exception=(e, catch_backtrace())
        
        # 📸 Debug Snapshot
        if page !== nothing
            try
                html_res = ChromeDevToolsLite.evaluate(page, "document.documentElement.outerHTML")
                html_content = safe_unwrap(html_res)
                open("debug_error.html", "w") do f
                    write(f, string(html_content))
                end
                @info "✅ Debug HTML saved."
            catch err
                @warn "Could not save debug HTML."
            end
        end
        exit(1)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
