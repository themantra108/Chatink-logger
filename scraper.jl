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
    
    # 🛡️ CLEAN JS: No comments, simple syntax, string concatenation (+)
    extract_js = """
    (() => {
        const tables = document.querySelectorAll("table");
        if (tables.length === 0) return "NO DATA FOUND";
        
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
                    
                    if (text.length > 0 && !text.includes("Loading") && !text.includes("Error while loading")) {
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
            
            const cleanRows = Array.from(rows).map(row => {
                const cells = row.querySelectorAll("th, td");
                if (cells.length === 0) return null; 

                const rowText = row.innerText;
                if (rowText.includes("Sort table by") || 
                    rowText.includes("Clause") || 
                    rowText.includes("Symbol") ||  
                    rowText.includes("No data")) return null;

                const safeWidget = widgetName.replace(/"/g, '""');

                const cellData = Array.from(cells).map(c => {
                    let text = c.innerText.trim();
                    text = text.replace(/"/g, '""'); 
                    return '"' + text + '"';
                }).join(",");
                
                return '"' + safeWidget + '",' + cellData;
            });

            allRows = allRows.concat(cleanRows.filter(r => r));
        });
        
        return allRows.join("\\n");
    })()
    """
    
    result = ChromeDevToolsLite.evaluate(page, extract_js)
    
    # 🚨 ERROR HANDLING: Check if Chrome returned an error object
    if isa(result, Dict) && (haskey(result, "className") || haskey(result, "exceptionDetails"))
        println("💥 JS Execution Error: $result")
        exit(1) # Fail the job gracefully
    end

    # Unwrap the value if it's a valid result dict
    data_string = isa(result, Dict) && haskey(result, "value") ? result["value"] : result

    temp_file = "new_chunk.csv"
    
    if !isa(data_string, String)
        println("💥 Error: Expected String data, got $(typeof(data_string))")
        exit(1)
    elseif data_string == "NO DATA FOUND" || isempty(data_string)
        println("⚠️ 0 Rows Found (Dashboard might be empty).")
        exit(1)
    else
        rows = split(data_string, "\n")
        current_time = get_ist()
        
        open(temp_file, "w") do io
            count = 0
            for row in rows
                # Basic validation
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
