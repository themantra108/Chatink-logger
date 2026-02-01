# FILE: scraper.jl
import Pkg
using Dates, Sockets

println(">> 1. Setting up Environment...")

# Install packages if missing
try
    using Conda, PyCall, ChromeDevToolsLite, DataFrames, JSON
catch
    Pkg.add(["Conda", "PyCall", "DataFrames", "JSON"])
    Pkg.add(url="https://github.com/svilupp/ChromeDevToolsLite.jl")
    
    # We build PyCall to ensure it sees the PYTHON="" from the YAML file
    Pkg.build("PyCall") 
    
    using Conda, PyCall, ChromeDevToolsLite, DataFrames, JSON
end

# --- INSTALL PYTHON DEPENDENCIES (THE RIGHT WAY) ---
println(">> Installing Python libraries into Conda...")
# Enable pip inside Julia's private Python
Conda.pip_interop(true)
# Install gspread into the SAME environment PyCall is using
Conda.pip("install", ["gspread", "google-auth"])

# --- CONFIGURATION ---
SPREADSHEET_ID = "17hKfqbd5-BP9QV8ZyJSlGFvhggDMPxX47B7O1wWJLUs"

SCAN_MAPPING = Dict(
    "Atlas" => "Atlas_Scan",
    "Breakout" => "Breakouts",
    "Volume" => "Volume_Shocks"
)

# --- 2. AUTHENTICATION ---
println(">> 2. Authenticating...")

if !haskey(ENV, "GCP_SERVICE_ACCOUNT")
    println("⚠ ERROR: GCP_SERVICE_ACCOUNT secret is missing from GitHub Settings!")
    exit(1)
end

# Import Python libraries
# This will now work because PyCall and Conda are finally synced
gspread = pyimport("gspread")
service_account = pyimport("google.oauth2.service_account")

# Parse Secret
creds_dict = JSON.parse(ENV["GCP_SERVICE_ACCOUNT"])
scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=scopes)
gc = gspread.authorize(creds)

# Connect to Sheet
try
    global ss = gc.open_by_key(SPREADSHEET_ID)
    println("   ✔ Connected to Sheet: ", ss.title)
catch e
    println("❌ ERROR: Could not open sheet!")
    println("   Make sure you shared the sheet with: ", creds_dict["client_email"])
    rethrow(e)
end

# --- 3. LAUNCH CHROME ---
println(">> 3. Launching Chrome...")
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
            
            target_tab = "Other_Scans"
            for (k, v) in SCAN_MAPPING
                if occursin(k, title) target_tab = v; break; end
            end
            if target_tab == "Other_Scans"
                target_tab = replace(title, " " => "_")[1:min(end, 30)]
            end

            println("   📝 Syncing '$title' -> '$target_tab'")

            n_cols = maximum(length.(rows))
            matrix_rows = []
            
            for r in rows
                while length(r) < n_cols push!(r, "") end
                push!(matrix_rows, [timestamp, title, r...])
            end
            
            if !isempty(matrix_rows)
                ws = nothing
                try
                    ws = ss.worksheet(target_tab)
                catch
                    ws = ss.add_worksheet(title=target_tab, rows=1000, cols=30)
                    header = ["Scraped_At", "Scan_Name", ["Col_$i" for i in 1:n_cols]...]
                    ws.append_row(header)
                end

                ws.append_rows(matrix_rows)
                println("      ✔ Appended $(length(matrix_rows)) rows.")
            end
        end
    end

finally
    try close(browser) catch; end
end
