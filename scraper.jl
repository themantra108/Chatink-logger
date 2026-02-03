using ChromeDevToolsLite
using Dates

# 🕒 Helper for Indian Standard Time
function get_ist()
    return now(Dates.UTC) + Hour(5) + Minute(30)
end

println("🚀 Starting Scraper Job at $(get_ist()) IST")

try
    # Connect (GitHub Actions automatically starts Chrome for us now)
    page = ChromeDevToolsLite.connect_browser() 
    println("✅ Connected to Chrome!")

    target_url = "https://chartink.com/dashboard/208896"
    println("🧭 Navigating to: $target_url")
    ChromeDevToolsLite.goto(page, target_url)

    # ⏳ Wait for data
    println("⏳ Waiting 20 seconds for data...")
    sleep(20)

    println("⛏️ Extracting table data with Widget Names...")
    
    # 🛠️ JS UPDATE: Iterate tables to find their Widget Name
    extract_js = """
    (() => {
        const tables = document.querySelectorAll("table");
        if (tables.length === 0) return "NO DATA FOUND";
        
        let allCsvRows = [];

        tables.forEach(table => {
            // 1. Find the Widget Name
            // We look for the closest container (.card or .panel) and then its title
            let widgetName = "Unknown Widget";
            const container = table.closest('.card, .panel, .box, .widget-content');
            
            if (container) {
                // Try to find a header tag inside the container
                const header = container.querySelector('.card-header, .panel-heading, .card-title, h3, h4, h5');
                if (header) {
                    // Clean up the name (remove newlines/extra spaces)
                    widgetName = header.innerText.replace(/[\\r\\n]+/g, " ").trim();
                }
            }

            // 2. Extract Rows for this specific table
            const rows = table.querySelectorAll("tr");
            
            const tableRows = Array.from(rows).map(row => {
                const cells = row.querySelectorAll("th, td");
                if (cells.length === 0) return null; // Skip empty rows

                // Escape quotes in widget name
                const safeWidgetName = widgetName.replace(/"/g, '""');

                // Get cell data
                const cellData = Array.from(cells).map(c => {
                     let text = c.innerText.trim();
                     text = text.replace(/"/g, '""'); 
                     return `"\${text}"`;
                }).join(",");
                
                // Format: "Widget Name", "Cell 1", "Cell 2"...
                return `"\${safeWidgetName}",\${cellData}`;
            });

            // Filter out nulls and add to main list
            allCsvRows = allCsvRows.concat(tableRows.filter(r => r));
        });
        
        return allCsvRows.join("\\n");
    })()
    """
    
    result = ChromeDevToolsLite.evaluate(page, extract_js)
    
    # Unwrap result
    data_string = isa(result, Dict) && haskey(result, "value") ? result["value"] : result

    csv_file = "chartink_history.csv"
    
    if data_string == "NO DATA FOUND" || isempty(data_string)
        println("⚠️ 0 ROWS FOUND.")
        exit(1)
    else
        rows = split(data_string, "\n")
        
        current_time = get_ist()
        open(csv_file, "a") do io
            count = 0
            for row in rows
                # Basic validation to ensure it's a real data row
                if length(row) > 10 && !contains(row, "No data for table")
                    # Final CSV Format: "Time", "Widget Name", "Symbol", "Price"...
                    println(io, "\"$(current_time)\",$row")
                    count += 1
                end
            end
            println("✅ Appended $count rows with Widget Names to $csv_file")
        end
    end

catch e
    println("💥 Error occurred: $e")
    rethrow(e)
end
