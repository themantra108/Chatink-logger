using ChromeDevToolsLite
using Dates

# 🕒 Helper: Precise Time (IST)
get_ist() = now(Dates.UTC) + Hour(5) + Minute(30)

# ⏳ Helper: Smart Waiting
function wait_for_selector(page, selector; timeout=60, poll_interval=1)
    start_time = time()
    while time() - start_time < timeout
        check_js = "document.querySelector('$selector') !== null"
        
        # 🔧 Force returnByValue here too, just to be safe
        result = ChromeDevToolsLite.evaluate(page, check_js; returnByValue=true)
        
        # Handle various return types
        val = false
        if isa(result, Dict)
            if haskey(result, "value")
                val = result["value"]
            elseif haskey(result, "result") && isa(result["result"], Dict)
                 val = get(result["result"], "value", false)
            end
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

        @info "⚡ DOM Ready. Extracting..."
        
        extract_js = """
        (() => {
            const tables = document.querySelectorAll("table");
            if (tables.length === 0) return "NO DATA FOUND";
            
            let allRows = [];

            tables.forEach(table => {
                let widgetName = "Unknown Widget";
                
                // 🏹 1. FIND WIDGET NAME
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
                
                // 🏹 2. EXTRACT ROWS
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
            
            return allRows.join("\\n");
        })()
        """
        
        # 🔧 FIX: explicitly request 'returnByValue=true'
        # This forces Chrome to serialize the object instead of sending a reference ID
        result = ChromeDevToolsLite.evaluate(page, extract_js; returnByValue=true)
        
        data_string = ""
        
        # 🛡️ SAFE UNWRAP (Updated)
        if isa(result, Dict)
            if haskey(result, "value")
                data_string = result["value"]
            elseif haskey(result, "result") && isa(result["result"], Dict) && haskey(result["result"], "value")
                data_string = result["result"]["value"]
            else
                # Fallback: Check if we still got an objectId (which means it failed to serialize)
                if haskey(result, "objectId") || (haskey(result, "result") && haskey(result["result"], "objectId"))
                     @warn "⚠️ Received Object Reference instead of Value. Data might be too large."
                else
                     @warn "⚠️ Unexpected JSON structure: $(keys(result))"
                end
                data_string = ""
            end
        else
            data_string = string(result)
        end

        if data_string == "NO DATA FOUND" || isempty(data_string)
            @warn "⚠️ No data found in tables (or empty return)."
            return
        end

        temp_file = "new_chunk.csv"
        rows = split(data_string, "\n")
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