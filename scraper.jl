# FILE: scraper.jl
import Pkg
println(">> 1. Installing Packages...")
try
    using ChromeDevToolsLite, DataFrames, JSON, CSV, Dates
catch
    Pkg.add(url="https://github.com/svilupp/ChromeDevToolsLite.jl")
    Pkg.add(["DataFrames", "JSON", "CSV", "Dates"])
    using ChromeDevToolsLite, DataFrames, JSON, CSV, Dates
end

using Sockets

# --- CONFIGURATION ---
SCAN_MAPPING = Dict(
    "Atlas" => "atlas_data.csv",
    "Breakout" => "breakouts.csv",
    "Volume" => "volume_shocks.csv"
)

# --- LAUNCH CHROME (HEADLESS) ---
println(">> 2. Launching Chrome...")
# GitHub Runners have Chrome installed, we just need to find it
chrome_cmd = `google-chrome --headless=new --no-sandbox --disable-gpu --remote-debugging-port=9222 --user-data-dir=/tmp/chrome-data`
process = run(pipeline(chrome_cmd, stdout=devnull, stderr=devnull), wait=false)

# Wait for connection
ready = false
for i in 1:20
    try connect("127.0.0.1", 9222); global ready = true; break; catch; sleep(1); end
end
if !ready error("Chrome failed to start") end

# --- SCRAPING LOGIC ---
println(">> 3. Scraping Dashboard...")
client = connect_browser()

try
    url = "https://chartink.com/dashboard/208896"
    goto(client, url)

    # Wait for Data
    for i in 1:30
        if evaluate(client, "document.querySelectorAll('tbody tr').length > 0") == true break end
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
    
    data_str = evaluate(client, js_script)
    
    if data_str !== nothing
        data = JSON.parse(data_str)
        scans = data["scans"]
        timestamp = Dates.format(now(), "yyyy-mm-dd HH:MM")
        
        println(">> Processing $(length(scans)) Scans...")
        
        for scan in scans
            title = scan["title"]
            rows = scan["rows"]
            headers = scan["headers"]
            
            # Map Filename
            target_file = "other_scans.csv"
            for (k, v) in SCAN_MAPPING
                if occursin(k, title) target_file = v; break; end
            end
            
            println("   📝 Saving '$title' -> '$target_file'")
            
            # Build DataFrame
            n_cols = maximum(length.(rows))
            if length(headers) < n_cols append!(headers, ["Col_$i" for i in length(headers)+1:n_cols]) end
            
            clean_rows = []
            for r in rows
                while length(r) < n_cols push!(r, "") end
                push!(clean_rows, r)
            end
            
            df = DataFrame([r[i] for r in clean_rows, i in 1:n_cols], headers[1:n_cols])
            insertcols!(df, 1, :Scraped_At => timestamp)
            insertcols!(df, 2, :Scan_Name => title)
            
            # Save/Append
            if isfile(target_file)
                existing = CSV.read(target_file, DataFrame)
                append!(existing, df)
                CSV.write(target_file, existing)
            else
                CSV.write(target_file, df)
            end
        end
    end

finally
    try close(client) catch; end
end
