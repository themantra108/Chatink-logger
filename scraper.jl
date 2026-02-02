# FILE: scraper.jl
import Pkg
using Dates, Sockets

println(">> 1. Setting up Environment...")

# Install pure Julia dependencies
try
    using ChromeDevToolsLite, DataFrames, JSON, CSV
catch
    Pkg.add(["DataFrames", "JSON", "CSV"])
    Pkg.add(url="https://github.com/svilupp/ChromeDevToolsLite.jl")
    using ChromeDevToolsLite, DataFrames, JSON, CSV
end

# --- CONFIGURATION ---
# Map scan names to specific CSV filenames
SCAN_MAPPING = Dict(
    "Atlas" => "atlas_data.csv",
    "Breakout" => "breakouts.csv",
    "Volume" => "volume_shocks.csv"
)

# --- 2. LAUNCH CHROME ---
println(">> 2. Launching Chrome...")
cmd = `google-chrome --headless=new --no-sandbox --disable-gpu --remote-debugging-port=9222 --user-data-dir=/tmp/chrome-data`
process = run(pipeline(cmd, stdout=devnull, stderr=devnull), wait=false)

# Wait for Chrome
ready = false
for i in 1:20
    try connect("127.0.0.1", 9222); global ready = true; break; catch; sleep(1); end
end
if !ready error("Chrome failed to start") end

# --- 3. SCRAPING LOGIC ---
println(">> 3. Scraping Dashboard...")
browser = connect_browser()

try
    url = "https://chartink.com/dashboard/208896"
    goto(browser, url)

    # Smart Wait (Wait up to 30s)
    for i in 1:30
        if evaluate(browser, "document.querySelectorAll('tbody tr').length > 0") == true break end
        sleep(1)
    end
    sleep(2)

    # Extract Data
    js_script = """
        (() => {
            const results = [];
            document.querySelectorAll("table").forEach((table, index) => {
                const headers = Array.from(table.querySelectorAll("th")).map(th => th.innerText.trim());
                const rows = Array.from(table.querySelectorAll("tbody tr")).map(tr => 
                    Array.from(tr.querySelectorAll("td")).map(td => td.innerText.trim())
                );
                
                let title = "Scan_" + (index + 1);
                let parent = table.closest('.card, .panel');
                if (parent) {
                    let header = parent.querySelector('.card-header, .panel-heading');
                    if (header) title = header.innerText.trim();
                }

                if (rows.length > 0) {
                    results.push({ "title": title, "headers": headers, "rows": rows });
                }
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
            headers = scan["headers"]
            
            # 1. Determine Filename
            target_file = "other_scans.csv"
            for (k, v) in SCAN_MAPPING
                if occursin(k, title) target_file = v; break; end
            end
            
            println("   📝 Saving '$title' -> '$target_file'")

            # 2. Build DataFrame
            n_cols = maximum(length.(rows))
            # Fix Headers
            if length(headers) < n_cols append!(headers, ["Col_$i" for i in length(headers)+1:n_cols]) end
            headers = headers[1:n_cols]
            
            # Clean Rows
            clean_rows = []
            for r in rows
                while length(r) < n_cols push!(r, "") end
                push!(clean_rows, r)
            end
            
            # Create DataFrame
            df = DataFrame([r[i] for r in clean_rows, i in 1:n_cols], Symbol.(headers))
            
            # Add Metadata Columns to the LEFT
            insertcols!(df, 1, :Scraped_At => timestamp)
            insertcols!(df, 2, :Scan_Name => title)
            
            # 3. Save to CSV (Append Mode)
            # If file exists, append without headers. If new, write with headers.
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
