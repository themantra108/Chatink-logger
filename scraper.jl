using ChromeDevToolsLite
using Dates

# 🕒 Function for IST? Simple and type-stable!
get_ist() = now(Dates.UTC) + Hour(5) + Minute(30)

println("🚀 Julia Scraper initializing... Time to eat some data! 🍽️")

try
    # Connection? Fast. ⚡
    page = ChromeDevToolsLite.connect_browser() 
    println("✅ Connected! Chrome is listening.")

    target_url = "https://chartink.com/dashboard/208896"
    println("🧭 Warp drive to: $target_url")
    ChromeDevToolsLite.goto(page, target_url)

    # Allow the JS heavy lifting to finish...
    println("⏳ Letting the DOM simmer for 20s...")
    sleep(20)

    println("⛏️ Mining diamonds (data)...")
    
    # 🧠 THE BRAIN: We do the filtering right in the JS execution context.
    # Why bring garbage into Julia just to GC it? Leave it in V8!
    extract_js = """
    (() => {
        const tables = document.querySelectorAll("table");
        if (tables.length === 0) return "NO DATA FOUND";
        
        let allRows = [];

        tables.forEach(table => {
            // 🔍 Find the Widget Name (The Metadata!)
            // We traverse up the DOM tree—Julia would love this tree traversal!
            let widgetName = "Unknown Widget";
            const container = table.closest('.card, .panel, .box, .widget-content, div[class*="widget"]');
            
            if (container) {
                const header = container.querySelector('.card-header, .panel-heading, .card-title, h3, h4, h5, .widget-title');
                if (header) {
                    widgetName = header.innerText.replace(/[\\r\\n]+/g, " ").trim();
                }
            }

            const rows = table.querySelectorAll("tr");
            
            const cleanRows = Array.from(rows).map(row => {
                const cells = row.querySelectorAll("th, td");
                if (cells.length === 0) return null; 

                // 🛡️ The Shield: Block the "Sort By" UI text
                const rowText = row.innerText;
                if (rowText.includes("Sort table by") || rowText.includes("Clause")) return null;

                const safeWidget = widgetName.replace(/"/g, '""');

                const cellData = Array.from(cells).map(c => {
                     let text = c.innerText.trim();
                     text = text.replace(/"/g, '""'); 
                     return `"\${text}"`;
                }).join(",");
                
                // 💎 The Gem: "Widget", "Data", "Data"...
                return `"\${safeWidget}",\${cellData}`;
            });

            allRows = allRows.concat(cleanRows.filter(r => r));
        });
        
        return allRows.join("\\n");
    })()
    """
    
    result = ChromeDevToolsLite.evaluate(page, extract_js)
    data_string = isa(result, Dict) && haskey(result, "value") ? result["value"] : result

    # 📂 Temp file strategy to avoid Git locking conflicts (Smart!)
    temp_file = "new_chunk.csv"
    
    if data_string == "NO DATA FOUND" || isempty(data_string)
        println("⚠️ 0 Rows. The dashboard might be sleeping.")
        exit(1)
    else
        rows = split(data_string, "\n")
        current_time = get_ist()
        
        open(temp_file, "w") do io
            count = 0
            for row in rows
                # One last sanity check using Julia's string powers
                if length(row) > 10 
                    println(io, "\"$(current_time)\",$row")
                    count += 1
                end
            end
            println("✅ BOOM! Saved $count clean rows to $temp_file")
        end
    end

catch e
    println("💥 Exception caught! Stacktrace incoming...")
    rethrow(e)
end
