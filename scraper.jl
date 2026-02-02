using ChromeDevToolsLite
using Dates

println("🚀 Starting Scraper Job at $(now())")

# Connect to the Chrome instance running in the GitHub Action background
# Note: We use the default host "127.0.0.1" and port 9222
chrome = ChromeDevToolsLite.connect() 
page = ChromeDevToolsLite.new_tab(chrome)

try
    target_url = "https://chartink.com/dashboard/208896"
    println("🧭 Navigating to: $target_url")
    ChromeDevToolsLite.goto(page, target_url)

    # Wait for the dashboard JS to render the tables
    println("⏳ Waiting for data to render...")
    sleep(8) 

    # Extract data
    println("⛏️ Extracting table data...")
    extract_js = """
    (() => {
        const rows = document.querySelectorAll("table tr");
        return Array.from(rows).map(row => {
            const cells = row.querySelectorAll("th, td");
            return Array.from(cells).map(c => c.innerText.trim()).join(" | ");
        });
    })()
    """
    
    result = ChromeDevToolsLite.evaluate(page, extract_js)
    
    # Save to file for the Artifact Upload step
    output_file = "output_data.txt"
    open(output_file, "w") do io
        println(io, "Scrape Timestamp: $(now())")
        println(io, "-" ^ 50)
        for row in result
            println(io, row)
        end
    end

    println("✅ Success! Data written to $output_file")
    println("Preview: $(first(result))")

catch e
    println("💥 Error occurred: $e")
    exit(1) # Fail the action if the script crashes

finally
    ChromeDevToolsLite.close(page)
end
