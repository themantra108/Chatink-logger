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

    # Give it time to load all widgets
    println("⏳ Waiting for data to render...")
    sleep(15)

    println("⛏️ Extracting table data...")
    
    # --- 🛠️ JS: THE CSV BUILDER ---
    # This JS function finds all data rows, escapes quotes, 
    # and returns a clean list of CSV lines.
    extract_js = """
    (() => {
        // Select ONLY data rows (tr inside tbody) to avoid grabbing headers every time
        // If the dashboard uses 'thead' for headers, this skips them. 
        // If it doesn't, we might get some headers, but that is okay for now.
        const rows = document.querySelectorAll("table tr");
        
        if (rows.length === 0) return [];
        
        return Array.from(rows).map(row => {
            const cells = row.querySelectorAll("th, td");
            return Array.from(cells).map(c => {
                let text = c.innerText.trim();
                // Escape quotes (standard CSV rule: " becomes "")
                text = text.replace(/"/g, '""');
                // Wrap in quotes to handle commas inside the text
                return `"\${text}"`;
            }).join(",");
        });
    })()
    """
    
    # Get the list of strings (rows)
    result = ChromeDevToolsLite.evaluate(page, extract_js)
    
    # Handle the result wrapper (sometimes it's a Dict, sometimes a Vector)
    rows = if isa(result, Dict) && haskey(result, "value")
        result["value"]
    else
        result
    end

    # Define our persistent history file
    csv_file = "chartink_history.csv"
    current_time = get_ist()
    
    # Check if this is the very first run (to write headers if needed)
    # For now, we just append data.
    
    # 📝 APPEND MODE ("a")
    open(csv_file, "a") do io
        # If result is empty/error
        if isempty(rows) || rows == "NO DATA FOUND"
            println("⚠️ No data found this run.")
        else
            count = 0
            for row in rows
                # We skip empty rows
                if length(row) > 2 
                    # Format: "2026-02-02T09:30:00, "Col1", "Col2"..."
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

finally
    # ChromeDevToolsLite.close(page)
end