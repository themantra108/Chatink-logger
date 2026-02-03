using ChromeDevToolsLite
using Dates

# 🕒 Precise Time function
get_ist() = now(Dates.UTC) + Hour(5) + Minute(30)

println("🚀 Julia Scraper: Initializing Dynamic Header Protocol... 📋")

try
    page = ChromeDevToolsLite.connect_browser() 
    println("✅ Chrome Connected.")

    target_url = "https://chartink.com/dashboard/208896"
    println("🧭 Navigating to dashboard...")
    ChromeDevToolsLite.goto(page, target_url)

    # ⏳ Wait for data to load
    println("⏳ Waiting 20s for DOM stabilization...")
    sleep(20)

    println("⛏️ Extracting Data with Headers...")
    
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
            
            // 🏹 2. EXTRACT ROWS (Headers + Data)
            const rows = table.querySelectorAll("tr");
            
            const processedRows = Array.from(rows).map(row => {
                const cells = row.querySelectorAll("th, td");
                if (cells.length === 0) return null; 

                // Check if this is a Header Row
                const isHeader = row.querySelector("th") !== null;
                const rowText = row.innerText;

                // 🛡️ Filter Logic
                // We keep headers now, but filter "No data" and "Clause" garbage
                if (rowText.includes("No data for table") || rowText.includes("Clause")) return null;

                const safeWidget = widgetName.replace(/"/g, '""');

                const cellData = Array.from(cells).map(c => {
                    let text = c.innerText.trim();
                    
                    // 🧼 HEADER CLEANING
                    // Headers often contain "Symbol\\nSort by Symbol..."
                    // We split by newline and take the first part to get just the name.
                    if (isHeader) {
                        text = text.split('\\n')[0].trim();
                        // Backup cleanup if newline didn't work
                        text = text.replace(/Sort table by/gi, "").trim();
                    }
                    
                    text = text.replace(/"/g, '""'); 
                    return '"' + text + '"';
                }).join(",");
                
                return '"' + safeWidget + '",' + cellData;
            });

            allRows = allRows.concat(processedRows.filter(r => r));
        });
        
        return allRows.join("\\n");
    })()
    """
    
    result = ChromeDevToolsLite.evaluate(page, extract_js)
    data_string = isa(result, Dict) && haskey(result, "value") ? result["value"] : result

    temp_file = "new_chunk.csv"
    
    if data_string == "NO DATA FOUND" || isempty(data_string)
        println("⚠️ 0 Rows Found.")
        exit(1)
    else
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
            println("✅ Success! Captured $count rows (Headers included).")
        end
    end

catch e
    println("💥 Error: $e")
    rethrow(e)
end
