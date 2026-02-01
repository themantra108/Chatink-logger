# FILE: scraper.jl
import Pkg
using Dates, Sockets

# --- 1. SETUP ENVIRONMENT & AUTH ---
println(">> 1. Installing Packages...")
try
    using GoogleSheets, ChromeDevToolsLite, DataFrames, JSON
catch
    Pkg.add("GoogleSheets")
    Pkg.add(url="https://github.com/svilupp/ChromeDevToolsLite.jl")
    Pkg.add(["DataFrames", "JSON"])
    using GoogleSheets, ChromeDevToolsLite, DataFrames, JSON
end

# --- CONFIGURATION ---
SPREADSHEET_ID = "https://docs.google.com/spreadsheets/d/17hKfqbd5-BP9QV8ZyJSlGFvhggDMPxX47B7O1wWJLUs/edit?usp=drive_link" # <--- REPLACE THIS!

SCAN_MAPPING = Dict(
    "Atlas" => "Atlas_Scan",
    "Breakout" => "Breakouts",
    "Volume" => "Volume_Shocks"
)

# Recover Credentials from GitHub Secrets
if haskey(ENV, "G_SHEETS_CREDENTIALS")
    open("service_account.json", "w") do f
        write(f, ENV["G_SHEETS_CREDENTIALS"])
    end
    ENV["GOOGLESHEETSCREDENTIALS"] = "service_account.json"
else
    println("⚠ Warning: No credentials found in ENV.")
end

# --- 2. CONNECT TO GOOGLE SHEETS ---
println(">> 2. Connecting to Google Sheets...")
client = sheets_client(AUTH_SCOPE_READWRITE)
ss = Spreadsheet(SPREADSHEET_ID)

# --- 3. LAUNCH CHROME ---
println(">> 3. Launching Chrome...")
# GitHub Runners have Chrome at /usr/bin/google-chrome
cmd = `google-chrome --headless=new --no-sandbox --disable-gpu --remote-debugging-port=9222 --user-data-dir=/tmp/chrome-data`
process = run(pipeline(cmd, stdout=devnull, stderr=devnull), wait=false)

# Wait for Chrome
ready = false
for i in 1:20
    try connect("127.0.0.1", 9222); global ready = true; break; catch; sleep(1); end
end
if !ready error("Chrome failed to start") end

# --- 4. SCRAPING LOGIC ---
println(">> 4. Scraping Dashboard...")
browser = connect_browser()

try
    url = "https://chartink.com/dashboard/208896"
    goto(browser, url)

    # Wait for Data
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
                    results.push({ "title": title, "rows": rows });
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
            
            # Map Title to Tab Name
            target_tab = "Other_Scans"
            for (k, v) in SCAN_MAPPING
                if occursin(k, title) target_tab = v; break; end
            end
            if target_tab == "Other_Scans"
                target_tab = replace(title, " " => "_")[1:min(end, 30)]
            end

            println("   📝 Syncing '$title' -> '$target_tab'")

            # Prepare Data Matrix for GoogleSheets.jl
            n_cols = maximum(length.(rows))
            matrix_rows = []
            
            for r in rows
                while length(r) < n_cols push!(r, "") end
                push!(matrix_rows, [timestamp, title, r...])
            end
            
            if !isempty(matrix_rows)
                # Convert to 2D Matrix
                data_matrix = permutedims(hcat(matrix_rows...))
                
                # Check if Sheet exists, if not create it
                existing_sheets = sheet_names(client, ss)
                if !(target_tab in existing_sheets)
                    add_sheet!(client, ss, target_tab)
                    # Add Header row for new sheets
                    header = ["Scraped_At", "Scan_Name", ["Col_$i" for i in 1:n_cols]...]
                    update!(client, CellRange(ss, "$(target_tab)!A1"), permutedims(header))
                end

                # Find next empty row (simple append logic)
                # Note: In production, batch_update is faster, but this is safer for headers
                try
                    current_data = get(client, CellRange(ss, "$(target_tab)!A:A"))
                    next_row = length(current_data.values) + 1
                    
                    update!(client, CellRange(ss, "$(target_tab)!A$(next_row)"), data_matrix)
                catch
                    # If sheet is empty/new
                    update!(client, CellRange(ss, "$(target_tab)!A2"), data_matrix)
                end
            end
        end
    end

finally
    try close(browser) catch; end
end
