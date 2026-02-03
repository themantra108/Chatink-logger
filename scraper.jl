using ChromeDevToolsLite
using Dates

# 🕒 Helper for Indian Standard Time
function get_ist()
    return now(Dates.UTC) + Hour(5) + Minute(30)
end

println("🚀 Starting Scraper Job at $(get_ist()) IST")

try
    # Connect to the browser (GitHub Actions starts it for us)
    page = ChromeDevToolsLite.connect_browser() 
    println("✅ Connected to Chrome!")

    target_url = "https://chartink.com/dashboard/208896"
    println("🧭 Navigating to: $target_url")
    ChromeDevToolsLite.goto(page, target_url)

    # ⏳ Wait for Chartink to load
    println("⏳ Waiting 20 seconds for data...")
    sleep(20)

    println("⛏️ Extracting table data...")
    
    # 🛠️ THE FIX: Join rows with newlines inside JS
    extract_js = """
    (() => {
        const rows = document.querySelectorAll("table tr");
        if (rows.length === 0) return "NO DATA FOUND";
        
        const data = Array.from(rows).map(row => {
            const cells = row.querySelectorAll("th, td");
            // Join cells with comma for CSV format
            // Escape quotes to prevent CSV breakage
            return Array.from(cells).map(c => {
                 let text = c.innerText.trim();
                 text = text.replace(/"/g, '""'); 
                 return `"\${text}"`;
            }).join(",");
        });
        
        return data.join("\\n");
    })()
    """
    
    result = ChromeDevToolsLite.evaluate(page, extract_js)
    
    # Handle the wrapper
    data_string = isa(result, Dict) && haskey(result, "value") ? result["value"] : result

    csv_file = "chartink_history.csv"
    
    if data_string == "NO DATA FOUND" || isempty(data_string)
        println("⚠️ 0 ROWS FOUND.")
        exit(1) # Fail the job so we know something is wrong
    else
        # Split back into lines to count them
        rows = split(data_string, "\n")
        
        # Append to file
        current_time = get_ist()
        open(csv_file, "a") do io
            count = 0
            for row in rows
                # Filter out garbage headers/empty rows
                if length(row) > 10 && !contains(row, "No data for table")
                    println(io, "\"$(current_time)\",$row")
                    count += 1
                end
            end
            println("✅ Appended $count clean rows to $csv_file")
        end
    end

catch e
    println("💥 Error occurred: $e")
    rethrow(e)
end
