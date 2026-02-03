using ChromeDevToolsLite
using Dates

# 🕒 Helper: Precise Time (IST)
get_ist() = now(Dates.UTC) + Hour(5) + Minute(30)

# ⏳ Helper: Smart Waiting
# REVERTED: Removed the 'returnByValue' arg that caused the crash.
function wait_for_selector(page, selector; timeout=60, poll_interval=1)
    start_time = time()
    while time() - start_time < timeout
        check_js = "document.querySelector('$selector') !== null"
        result = ChromeDevToolsLite.evaluate(page, check_js)
        
        # Simple boolean unwrap
        val = false
        if isa(result, Dict) && haskey(result, "value")
             val = result["value"]
        elseif isa(result, Dict) && haskey(result, "result")
             val = get(result["result"], "value", false)
        else
             val = result
        end
        
        if val == true
            return true
        end
        sleep(poll_interval)
    end
    throw(ErrorException("Timeout waiting for selector: $selector"))
end

# 🚀 Main Execution Function
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
        
        # 1️⃣ EXECUTE SCRAPER (But don't return data yet!)
        # We save the result to 'window._chartinkData'
        setup_js = """
        (() => {
            const tables = document.querySelectorAll("table");
            if (tables.length === 0) {
                window._chartinkData = "NO DATA FOUND";
                return;
            }
            
            let allRows = [];

            tables.forEach(table => {
                let widgetName = "Unknown Widget";
                let current = table;
                let depth = 0;
                while (current && depth < 6) {
                    let sibling = current.previousElementSibling;
                    let foundTitle = false;
                    for (let i = 0; i < 5; i++) {
                        if (!sibling) break;
                        let text = sibling.innerText.trim();
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
                
                const rows = table.querySelectorAll("tr");
                const processedRows = Array.from(rows).map(row => {
                    const cells = row.querySelectorAll("th, td");
                    if (cells.length === 0) return null; 
                    const isHeader = row.querySelector("th") !== null;
                    const rowText = row.innerText;
                    if (rowText.includes("No data for table") || rowText.includes("Clause")) return null;
                    const safeWidget = widgetName.replace(/"/g, '""');
                    const cellData = Array.from(cells).map(c => {
                        let text = c.innerText.trim();
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
            
            // SAVE TO GLOBAL VARIABLE
            window._chartinkData = allRows.join("\\n");
            return "DONE";
        })()
        """
        ChromeDevToolsLite.evaluate(page, setup_js)
        
        # 2️⃣ FETCH DATA IN CHUNKS
        # We pull 50,000 chars at a time to avoid the "Object Reference" error
        @info "📦 Fetching data chunks..."
        
        # Get total length
        len_res = ChromeDevToolsLite.evaluate(page, "window._chartinkData.length")
        total_len = isa(len_res, Dict) ? len_res["value"] : len_res
        
        if total_len == 0 || total_len == "NO DATA FOUND"
             @warn "⚠️ No data found."
             return
        end

        full_data = ""
        chunk_size = 50000
        current_idx = 0
        
        while current_idx < total_len
            # JS: slice the string
            end_idx = min(current_idx + chunk_size, total_len)
            chunk_js = "window._chartinkData.substring($current_idx, $end_idx)"
            
            chunk_res = ChromeDevToolsLite.evaluate(page, chunk_js)
            
            # Unwrap
            chunk_val = ""
            if isa(chunk_res, Dict)
                 chunk_val = chunk_res["value"]
            elseif isa(chunk_res, Dict) && haskey(chunk_res, "result")
                 chunk_val = get(chunk_res["result"], "value", "")
            else
                 chunk_val = chunk_res
            end
            
            full_data = full_data * chunk_val
            current_idx += chunk_size
        end

        if isempty(full_data) || full_data == "NO DATA FOUND"
             @warn "⚠️ Data was empty after chunking."
             return
        end

        # 💾 Write to Chunk File
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
