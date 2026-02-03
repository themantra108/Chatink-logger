using ChromeDevToolsLite
using Dates

function get_ist()
    return now(Dates.UTC) + Hour(5) + Minute(30)
end

println("🚀 Starting Scraper Job at $(get_ist()) IST")

try
    page = ChromeDevToolsLite.connect_browser() 
    println("✅ Connected to Chrome!")

    target_url = "https://chartink.com/dashboard/208896"
    println("🧭 Navigating to: $target_url")
    ChromeDevToolsLite.goto(page, target_url)

    println("⏳ Waiting 20 seconds for data...")
    sleep(20)

    println("⛏️ Extracting Clean Data...")
    
    extract_js = """
    (() => {
        const tables = document.querySelectorAll("table");
        if (tables.length === 0) return "NO DATA FOUND";
        
        let allCsvRows = [];

        tables.forEach(table => {
            // 1. ROBUST WIDGET NAME FINDER
            // We look for the header in parent containers
            let widgetName = "Unknown Widget";
            const container = table.closest('.card, .panel, .box, .widget-content, div[class*="widget"]');
            
            if (container) {
                const header = container.querySelector('.card-header, .panel-heading, .card-title, h3, h4, h5, .widget-title');
                if (header) {
                    widgetName = header.innerText.replace(/[\\r\\n]+/g, " ").trim();
                }
            }

            const rows = table.querySelectorAll("tr");
            
            const tableRows = Array.from(rows).map(row => {
                const cells = row.querySelectorAll("th, td");
                if (cells.length === 0) return null; 

                // 2. NOISE FILTER (In-Browser)
                // If the row is just a "Sort by" header, skip it immediately
                if (row.innerText.includes("Sort table by")) return null;
                if (row.innerText.includes("Clause")) return null; // Skip header row

                const safeWidgetName = widgetName.replace(/"/g, '""');

                const cellData = Array.from(cells).map(c => {
                     let text = c.innerText.trim();
                     text = text.replace(/"/g, '""'); 
                     return `"\${text}"`;
                }).join(",");
                
                // Format: "Widget Name", "Symbol", "Price"...
                return `"\${safeWidgetName}",\${cellData}`;
            });

            allCsvRows = allCsvRows.concat(tableRows.filter(r => r));
        });
        
        return allCsvRows.join("\\n");
    })()
    """
    
    result = ChromeDevToolsLite.evaluate(page, extract_js)
    data_string = isa(result, Dict) && haskey(result, "value") ? result["value"] : result

    temp_file = "new_chunk.csv"
    
    if data_string == "NO DATA FOUND" || isempty(data_string)
        println("⚠️ 0 ROWS FOUND.")
        exit(1)
    else
        rows = split(data_string, "\n")
        current_time = get_ist()
        
        open(temp_file, "w") do io
            count = 0
            for row in rows
                # Double check filter to ensure no garbage gets in
                if length(row) > 10 
                    println(io, "\"$(current_time)\",$row")
                    count += 1
                end
            end
            println("✅ Saved $count CLEAN rows to $temp_file")
        end
    end

catch e
    println("💥 Error occurred: $e")
    rethrow(e)
end
