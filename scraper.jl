using ChromeDevToolsLite
using Dates
using JSON  # <--- The Secret Weapon 🗝️

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

        @info "⚡ DOM Ready. Preparing Safe Payload..."
        
        # 1️⃣ THE JAVASCRIPT PAYLOAD
        # We can write normal, readable JS here.
        # JSON.json() will handle all the escaping for us!
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
                                    // Split by newline safely
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
        
        # 🛡️ THE FIX: JSON ENCODING
        # We turn the code into a safe JSON string (e.g., "(() => { ... })()")
        # Then we tell Chrome to eval() that string.
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

        # 2️⃣ FETCH DATA LENGTH
        @info "📦 Checking data length..."
        len_res = ChromeDevToolsLite.evaluate(page, "window._chartinkData.length")
        total_len = safe_unwrap(len_res)
        
        @info "📊 Raw Length: $total_len"
        
        if isa(total_len, String) && total_len == "UNDEFINED"
             @warn "⚠️ Data variable is undefined."
             return
        end
        
        if !isa(total_len, Int)
            try
                total_len = parse(Int, string(total_len))
            catch
                @warn "⚠️ Invalid length format."
                return
            end
        end

        if total_len == 0
            @warn "⚠️ Data length is 0."
            return
        end

        # 3️⃣ CHUNK FETCHING (Safe & Fast)
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
        exit(1)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
