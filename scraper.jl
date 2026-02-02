using ChromeDevToolsLite
using Dates

println("🚀 Starting Scraper Job at $(now())")

# --- FIX IS HERE ---
# We use connect_browser() instead of connect(). 
# It connects to localhost:9222 by default and returns a client/page object.
try
    page = ChromeDevToolsLite.connect_browser() 
    println("✅ Connected to Chrome!")

    target_url = "https://chartink.com/dashboard/208896"
    println("🧭 Navigating to: $target_url")
    ChromeDevToolsLite.goto(page, target_url)

    # Wait for the dashboard JS to render the tables
    println("⏳ Waiting for data to render...")
    sleep(10) # Increased slightly to be safe in CI

    # Extract data
    println("⛏️ Extracting table data...")
    extract_js = """
    (() => {
        const rows = document.querySelectorAll("table tr");
        if (rows.length === 0) return "NO DATA FOUND";
        return Array.from(rows).map(row => {
            const cells = row.querySelectorAll("th, td");
            return Array.from(cells).map(c => c.innerText.trim()).join(" | ");
        });
    })()
    """
    
    result = ChromeDevToolsLite.evaluate(page, extract_js)
    
    # Save to file
    output_file = "output_data.txt"
    open(output_file, "w") do io
        println(io, "Scrape Timestamp: $(now())")
        println(io, "-" ^ 50)
        # Handle cases where result might be a single string or array
        if result isa Vector
            for row in result
                println(io, row)
            end
        else
            println(io, result)
        end
    end

    println("✅ Success! Data written to $output_file")

catch e
    println("💥 Error occurred: $e")
    rethrow(e) # This ensures the GitHub Action marks the job as FAILED if it crashes

finally
    # Optional: Close the tab/browser if needed, though CI kills the container anyway.
    # ChromeDevToolsLite.close(page) 
end