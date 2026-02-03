using ChromeDevToolsLite
using Dates

# 🕒 Precise Time function
get_ist() = now(Dates.UTC) + Hour(5) + Minute(30)

println("🚀 Julia Scraper: Initializing Production Protocol... 🏭")

try
    page = ChromeDevToolsLite.connect_browser() 
    println("✅ Chrome Connected.")

    target_url = "https://chartink.com/dashboard/208896"
    println("🧭 Navigating to dashboard...")
    ChromeDevToolsLite.goto(page, target_url)

    # ⏳ Wait for Vue.js to render everything
    println("⏳ Waiting 20s for DOM stabilization...")
    sleep(20)

    println("⛏️ Extracting Clean Data...")
    
    # 🛡️ SAFE JS: Using concatenation (+) to avoid '$' parse errors in Julia
    extract_js = """
    (() => {
        const tables = document.querySelectorAll("table");
        if (tables.length === 0) return "NO DATA FOUND";
        
        let allRows = [];

        tables.forEach(table => {
            let widgetName = "Unknown Widget";
            
            // 🏹 LOGIC: Climb up, then look backwards for the real title
            let current = table;
            let depth = 0;
            
            // 1. Climb up to find the container
            while (current && depth < 6) {
                let sibling = current.previousElementSibling;
                
                // 2. Walk backwards through siblings (max 5 steps)
                let foundTitle = false;
                for (let i = 0; i < 5; i++) {
                    if (!sibling) break;
                    
                    let text = sibling.innerText.trim();
                    
                    // 🛡️ SMART FILTER: Skip empty, "Loading", or "Error" messages
                    if (text.length > 0 && 
                        !text.includes("Loading") && 
                        !text.includes("Error while loading")) {
                        
                        // ✅ FOUND IT!
                        // Format: "Mod 5_day_Check\n3rd Feb..." -> Take first line
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
            
            // --- Standard Row Extraction ---
            const rows = table.querySelectorAll("tr");
            
            const cleanRows = Array.from(rows).map(row => {
                const cells = row.querySelectorAll("th, td");
                if (cells.length === 0) return null; 

                // 🛡️ Filter Headers and Garbage
                const rowText = row.innerText;
                if (rowText.includes("Sort table by") || 
                    rowText.includes("Clause") || 
                    rowText.includes("Symbol") ||  
                    rowText.includes("No data")) return null;

                const safeWidget = widgetName.replace(/"/g, '""');

                const cellData = Array.from(cells).map(c => {
                    let text = c.innerText.trim();
                    text = text.replace(/"/g, '""'); 
                    return '"' + text + '"'; // Use + instead of dollar sign
                }).join(",");
                
                // Final CSV Row
                return '"' + safeWidget + '",' + cellData;
            });

            allRows = allRows.concat(cleanRows.filter(r => r));
        });
        
        return allRows.join("\\n");
    })()
    """
    
    result = ChromeDevToolsLite.evaluate(page, extract_js)
    data_string = isa(result, Dict) && haskey(result, "value") ? result["value"] : result

    temp_file = "new_chunk.csv"
    
    if data_string == "NO DATA FOUND" || isempty(data_string)
        println("⚠️ 0 Rows Found (Dashboard might be empty).")
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
            println("✅ Success! Captured $count clean rows.")
        end
    end

catch e
    println("💥 Error: $e")
    rethrow(e)
end
