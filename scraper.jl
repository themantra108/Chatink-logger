# FILE: scraper.jl
import Pkg
using Dates, Sockets

# --- 1. SETUP ENVIRONMENT ---
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
# REPLACE WITH YOUR ACTUAL SHEET ID
SPREADSHEET_ID = "YOUR_SPREADSHEET_ID_GOES_HERE" 

SCAN_MAPPING = Dict(
    "Atlas" => "Atlas_Scan",
    "Breakout" => "Breakouts",
    "Volume" => "Volume_Shocks"
)

# --- 2. ROBUST AUTHENTICATION ---
# We force the file into the default location AND set the ENV var with absolute path
if haskey(ENV, "GCP_SERVICE_ACCOUNT")
    # 1. Determine the 'default' path where GoogleSheets.jl looks
    # usually: ~/.julia/config/google_sheets/credentials.json
    creds_dir = joinpath(first(DEPOT_PATH), "config", "google_sheets")
    mkpath(creds_dir) # Create directory if missing
    
    creds_path = joinpath(creds_dir, "credentials.json")
    
    # 2. Write the secret to this file
    open(creds_path, "w") do f
        write(f, ENV["GCP_SERVICE_ACCOUNT"])
    end
    
    # 3. Set ENV var to Absolute Path (Double safety)
    ENV["GOOGLESHEETSCREDENTIALS"] = abspath(creds_path)
    
    println(">> Auth: Credentials saved to: $creds_path")
else
    println("⚠ ERROR: GCP_SERVICE_ACCOUNT secret is missing from GitHub!")
    exit(1)
end

# --- 3. CONNECT TO SHEETS ---
println(">> 2. Connecting to Google Sheets...")
# Initialize with specific scope
client = sheets_client(AUTH_SCOPE_READWRITE)
ss = Spreadsheet(SPREADSHEET_ID)
println("   ✔ Connected to Sheet.")

# --- 4. LAUNCH CHROME ---
println(">> 3. Launching Chrome...")
cmd = `google-chrome --headless=new --no-sandbox --disable-gpu --remote-debugging-port=9222 --user-data-dir=/tmp/chrome-data`
process = run(pipeline(cmd, stdout=devnull, stderr=devnull), wait=false)

# Wait for Chrome
ready = false
for i in 1:20
    try connect("127.0.0.1", 9222); global ready = true; break; catch; sleep(1); end
end
if !ready error("Chrome failed to start") end

# --- 5. SCRAPING LOGIC ---
println(">> 4. Scraping Dashboard...")
browser = connect_browser()

try
    url = "https://chartink.com/dashboard/208896"
    goto(browser, url)

    # Smart Wait
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
            
            # Map Title
            target_tab = "Other_Scans"
            for (k, v) in SCAN_MAPPING
                if occursin(k, title) target_tab = v; break; end
            end
            if target_tab == "Other_Scans"
                target_tab = replace(title, " " => "_")[1:min(end, 30)]
            end

            println("   📝 Syncing '$title' -> '$target_tab'")

            # Prepare Data
            n_cols = maximum(length.(rows))
            matrix_rows = []
            
            for r in rows
                while length(r) < n_cols push!(r, "") end
                push!(matrix_rows, [timestamp, title, r...])
            end
            
            if !isempty(matrix_rows)
                data_matrix = permutedims(hcat(matrix_rows...))
                
                # Check/Create Sheet
                existing_sheets = sheet_names(client, ss)
                if !(target_tab in existing_sheets)
                    add_sheet!(client, ss, target_tab)
                    header = ["Scraped_At", "Scan_Name", ["Col_$i" for i in 1:n_cols]...]
                    update!(client, CellRange(ss, "$(target_tab)!A1"), permutedims(header))
                end

                # Append Logic
                try
                    current_data = get(client, CellRange(ss, "$(target_tab)!A:A"))
                    next_row = length(current_data.values) + 1
                    update!(client, CellRange(ss, "$(target_tab)!A$(next_row)"), data_matrix)
                catch
                    update!(client, CellRange(ss, "$(target_tab)!A2"), data_matrix)
                end
            end
        end
    end

finally
    try close(browser) catch; end
end
