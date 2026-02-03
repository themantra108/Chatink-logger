using ChromeDevToolsLite
using Dates

# 🕒 Helper: Precise Time (IST)
get_ist() = now(Dates.UTC) + Hour(5) + Minute(30)

# ⏳ Helper: Smart Waiting (The "Anti-Sleep" Hammer)
# Polls every 1s to see if table exists. Much safer than sleep().
function wait_for_selector(page, selector; timeout=30, poll_interval=1)
    start_time = time()
    while time() - start_time < timeout
        # JS check: returns true if element exists
        check_js = "document.querySelector('$selector') !== null"
        result = ChromeDevToolsLite.evaluate(page, check_js)
        
        # Handle the Dict return wrapper that CDTL sometimes uses
        val = isa(result, Dict) ? result["value"] : result
        
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
        # Connect to the Chrome instance launched by GitHub Actions
        page = ChromeDevToolsLite.connect_browser() 
        @info "✅ Chrome Connected."

        target_url = "https://chartink.com/dashboard/208896"
        ChromeDevToolsLite.goto(page, target_url)
        @info "🧭 Navigated to dashboard."

        # ⏳ SMART WAIT: Waits for tables to appear
        @info "👀 Watching DOM for tables..."
        wait_for_selector(page, "table.table") 
        
        # Small buffer for data population after the table structure appears
        sleep(5) 

        @info "⚡ DOM Ready. Extracting..."
        
        # 💉 The "Sibling Hunter" JS Logic
        extract_js = """
        (() => {
            const tables = document.querySelectorAll("table");
            if (tables.length === 0) return "NO DATA FOUND";
            
            let allRows = [];

            tables.forEach(table => {
                let widgetName = "Unknown Widget";
                
                // 🏹 1. FIND WIDGET NAME (Sibling Hunter Logic)
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

                    // Check if Header
                    const isHeader = row.querySelector("th") !== null;
                    const rowText = row.innerText;

                    // 🛡️ Filter Logic
                    if (rowText.includes("No data for table") || rowText.includes("Clause")) return null;

                    const safeWidget = widgetName.replace(/"/g, '""');

                    const cellData = Array.from(cells).map(c => {
                        let text = c.innerText.trim();
                        
                        // 🧼 HEADER CLEANING
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
        
        result = ChromeDevToolsLite.evaluate(page, extract_js)
        data_string = isa(result, Dict) ? result["value"] : result

        if data_string == "NO DATA FOUND" || isempty(data_string)
            @warn "⚠️ No data found in tables."
            return
        end

        # 💾 Write to Chunk File
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
        exit(1) # Fail the action so you get an email notification
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
