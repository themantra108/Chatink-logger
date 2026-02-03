using ChromeDevToolsLite
using Dates

# 🕒 Helper for Indian Standard Time
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

    println("⛏️ Extracting table data...")
    
    extract_js = """
    (() => {
        const tables = document.querySelectorAll("table");
        if (tables.length === 0) return "NO DATA FOUND";
        
        let allCsvRows = [];

        tables.forEach(table => {
            let widgetName = "Unknown Widget";
            const container = table.closest('.card, .panel, .box, .widget-content');
            
            if (container) {
                const header = container.querySelector('.card-header, .panel-heading, .card-title, h3, h4, h5');
                if (header) {
                    widgetName = header.innerText.replace(/[\\r\\n]+/g, " ").trim();
                }
            }

            const rows = table.querySelectorAll("tr");
            const tableRows = Array.from(rows).map(row => {
                const cells = row.querySelectorAll("th, td");
                if (cells.length === 0) return null; 

                const safeWidgetName = widgetName.replace(/"/g, '""');

                const cellData = Array.from(cells).map(c => {
                     let text = c.innerText.trim();
                     text = text.replace(/"/g, '""'); 
                     return `"\${text}"`;
                }).join(",");
                
                return `"\${safeWidgetName}",\${cellData}`;
            });

            allCsvRows = allCsvRows.concat(tableRows.filter(r => r));
        });
        
        return allCsvRows.join("\\n");
    })()
    """
    
    result = ChromeDevToolsLite.evaluate(page, extract_js)
    data_string = isa(result, Dict) && haskey(result, "value") ? result["value"] : result

    # 🚨 CHANGE: Write to a TEMP file ("w" mode), not the history file
    temp_file = "new_chunk.csv"
    
    if data_string == "NO DATA FOUND" || isempty(data_string)
        println("⚠️ 0 ROWS FOUND.")
        exit(1)
    else
        rows = split(data_string, "\n")
        current_time = get_ist()
        
        # Write clean data to the temp file
        open(temp_file, "w") do io
            count = 0
            for row in rows
                if length(row) > 10 && !contains(row, "No data for table")
                    println(io, "\"$(current_time)\",$row")
                    count += 1
                end
            end
            println("✅ Saved $count new rows to $temp_file")
        end
    end

catch e
    println("💥 Error occurred: $e")
    rethrow(e)
end