# FILE: scraper.jl
import Pkg
using Dates, Sockets

println(">> 1. Setting up Environment...")

try
    using ChromeDevToolsLite, DataFrames, JSON, CSV
catch
    Pkg.add(["DataFrames", "JSON", "CSV"])
    Pkg.add(url="https://github.com/svilupp/ChromeDevToolsLite.jl")
    using ChromeDevToolsLite, DataFrames, JSON, CSV
end

# --- CONFIGURATION ---
SCAN_MAPPING = Dict(
    "Atlas" => "atlas_data.csv",
    "Breakout" => "breakouts.csv",
    "Volume" => "volume_shocks.csv"
)

MAX_WAIT_SECONDS = 120  
STABILITY_REQUIRED = 5 

# --- 2. LAUNCH CHROME ---
println(">> 2. Launching Chrome...")
cmd = `google-chrome --headless=new --no-sandbox --disable-gpu --remote-debugging-port=9222 --user-data-dir=/tmp/chrome-data`
process = run(pipeline(cmd, stdout=devnull, stderr=devnull), wait=false)

ready = false
for i in 1:20
    try connect("127.0.0.1", 9222); global ready = true; break; catch; sleep(1); end
end
if !ready error("Chrome failed to start") end

# --- 3. SCRAPING LOGIC ---
println(">> 3. Scraping Dashboard...")
browser = connect_browser()

# FIX: Initialize these variables globally BEFORE the try block
global last_row_count = 0
global stable_ticks = 0

try
    url = "https://chartink.com/dashboard/208896"
    goto(browser, url)

    println("   Waiting for tables to fully load...")
    
    # Start Stability Loop
    for i in 1:MAX_WAIT_SECONDS
        # Count rows (excluding 'Loading...' placeholders)
        current_count = evaluate(browser, """
            (() => {
                let rows = document.querySelectorAll('tbody tr');
                let count = 0;
                rows.forEach(r => {
                    if (!r.innerText.includes("Loading")) count++;
                });
                return count;
            })()
        """)
        
        # Stability Check
        # We must use 'global' to update the variables defined outside the loop
        if current_count > 0 && current_count == last_row_count
            global stable_ticks += 1
        else
            global stable_ticks = 0 
        end
        
        # Update tracker 
        global last_row_count = current_count
        
        # Status update
        if i % 5 == 0
            println("      Time: $(i)s | Rows found: $current_count | Stable for: $(stable_ticks)s")
        end

        # Exit Condition
        if stable_ticks >= STABILITY_REQUIRED
            println("   ✔ Data stabilized. Proceeding to extract.")
            break
        end
        
        sleep(1)
    end
    
    sleep(2) # Buffer

    # --- EXTRACTION ---
    js_script = """
        (() => {
            const results = [];
            document.querySelectorAll("table").forEach((table, index) => {
                const rows = Array.from(table.querySelectorAll("tbody tr")).map(tr => 
                    Array.from(tr.querySelectorAll("td")).map(td => td.innerText.trim())
                );
                
                let title = "Scan_" + (index + 1);
                let parent = table.closest('.card, .panel');
                if (parent) {
                    let header = parent.querySelector('.card-header, .panel-heading');
                    if (header) title = header.innerText.trim();
                }

                results.push({ "title": title, "rows": rows });
            });
            return JSON.stringify({ "scans": results });
        })()
    """
    
    data_str = evaluate(browser, js_script)
    
    if data_str !== nothing
        data = JSON.parse(data_str)
        scans = data["scans"]
        timestamp = Dates.format(now(), "yyyy-mm-dd HH:MM:SS")
        
        println(">> Found $(length(scans)) Scans...")
        
        for scan in scans
            title = scan["title"]
            rows = scan["rows"]
            
            if length(rows) == 0 
                println("   ⚠ Skipping '$title' (No Data)")
                continue
            end

            target_file = "other_scans.csv"
            for (k, v) in SCAN_MAPPING
                if occursin(k, title) target_file = v; break; end
            end
            
            println("   📝 Saving '$title' -> '$target_file'")

            n_cols = maximum(length.(rows))
            clean_rows = []
            for r in rows
                while length(r) < n_cols push!(r, "") end
                push!(clean_rows, r)
            end
            
            # Dynamic Headers
            headers = Symbol.(["Col_$i" for i in 1:n_cols])
            df = DataFrame([r[i] for r in clean_rows, i in 1:n_cols], headers)
            
            insertcols!(df, 1, :Scraped_At => timestamp)
            insertcols!(df, 2, :Scan_Name => title)
            
            file_exists = isfile(target_file)
            CSV.write(target_file, df; append=file_exists, writeheader=!file_exists)
        end
        println("\n✅ Data saved locally.")
    else
        println("❌ No data found on page.")
    end

finally
    try close(browser) catch; end
end
