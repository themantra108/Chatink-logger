using ChromeDevToolsLite
using Dates

# 🕒 Helper for Indian Standard Time (UTC + 5:30)
function get_ist()
    return now(Dates.UTC) + Hour(5) + Minute(30)
end

println("🚀 Starting Scraper Job at $(get_ist()) IST")

try
    # Connect to the browser instance started by GitHub Actions
    page = ChromeDevToolsLite.connect_browser() 
    println("✅ Connected to Chrome!")

    target_url = "https://chartink.com/dashboard/208896"
    println("🧭 Navigating to: $target_url")
    ChromeDevToolsLite.goto(page, target_url)

    # ⏳ Wait 20 seconds to ensure Chartink loads fully (bypasses some loaders)
    println("⏳ Waiting for data to render...")
    sleep(20)

    println("⛏️ Extracting table data...")
    
    # JavaScript to scrape data and format it as CSV lines
    extract_js = """
    (() => {
        // Select all table rows
        const rows = document.querySelectorAll("table tr");
        if (rows.length === 0) return [];
        
        return Array.from(rows).map(row => {
            const cells = row.querySelectorAll("th, td");
            return Array.from(cells).map(c => {
                // Get text and escape existing quotes for CSV validity
                let text = c.textContent.trim(); 
                text = text.replace(/"/g, '""');
                return `"\${text}"`;
            }).join(",");
        });
    })()
    """
    
    result = ChromeDevToolsLite.evaluate(page, extract_js)
    
    # Handle the wrapper object returned by the library
    rows = if isa(result, Dict) && haskey(result, "value")
        result["value"]
    else
        result
    end

    csv_file = "chartink_history.csv"
    
    # 🚨 LOGIC: Check if data was found
    if isempty(rows) || rows == "NO DATA FOUND"
        println("⚠️ 0 ROWS FOUND! DUMPING HTML FOR DEBUGGING...")
        
        # Save the page HTML to debug if Chartink blocked us
        html_dump = ChromeDevToolsLite.evaluate(page, "document.documentElement.outerHTML")
        html_content = isa(html_dump, Dict) ? html_dump["value"] : html_dump
        
        open("debug_error.html", "w") do io
            print(io, html_content)
        end
        println("📸 Saved debug_error.html for inspection.")
    else
        # ✅ SUCCESS: Append data to history file
        current_time = get_ist()
        open(csv_file, "a") do io
            count = 0
            for row in rows
                # Basic filter to skip empty/malformed rows
                if length(row) > 5 
                    println(io, "\"$(current_time)\",$row")
                    count += 1
                end
            end
            println("✅ Appended $count rows to $csv_file")
        end
    end

catch e
    println("💥 Error occurred: $e")
    rethrow(e)
end
