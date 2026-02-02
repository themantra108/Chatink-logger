# FILE: scraper.jl
import Pkg
using Dates, Sockets

# --- 1. SETUP ---
println(">> 1. Setting up Environment...")
try
    using ChromeDevToolsLite, DataFrames, JSON, CSV
catch
    Pkg.add(["DataFrames", "JSON", "CSV"])
    Pkg.add(url="https://github.com/svilupp/ChromeDevToolsLite.jl")
    using ChromeDevToolsLite, DataFrames, JSON, CSV
end

# 📂 OUTPUT FOLDER
OUTPUT_DIR = "scans"
if !isdir(OUTPUT_DIR)
    mkdir(OUTPUT_DIR)
    println("   Created folder: $OUTPUT_DIR")
end

# --- HELPER: CLEAN FILENAMES ---
function get_clean_filename(raw_text)
    # Remove time pattern like (10:30 AM)
    clean_name = replace(raw_text, r"\(.*?\)" => "")
    # Clean special chars
    clean_name = replace(clean_name, r"[^a-zA-Z0-9]" => "_") 
    clean_name = replace(clean_name, r"__+" => "_")          
    clean_name = strip(clean_name, ['_'])                    
    
    if isempty(clean_name) clean_name = "Unknown_Scan" end
    return "scan_" * first(clean_name, 50) * ".csv"
end

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
println(">> 3. Starting Scraper...")
browser = connect_browser()

try
    url = "https://chartink.com/dashboard/208896"
    goto(browser, url)

    println("   Waiting for tables to load...")
    
    # --- ROBUST WAIT ---
    # Wait until data appears OR "No records" text appears
    data_ready = false
    for i in 1:60
        status = evaluate(browser, """
            (() => {
                let rows = document.querySelectorAll('tbody tr');
                if (rows.length === 0) return false;
                
                let text = document.body.innerText;
                
                // Case 1: Explicitly empty
                if (text.includes("No records") || text.includes("No stocks match")) return true;

                // Case 2: Data loaded (check first row for actual content)
                let firstRow = rows[0].innerText;
                if (firstRow.length > 5 && !firstRow.includes("Loading")) return true;
                
                return false;
            })()
        """)
        
        if status == true
            println("   ✔ Tables detected in $(i)s.")
            data_ready = true
            break
        end
        sleep(1)
    end
    
    if !data_ready
        println("   ⚠ Timeout: Page took too long to load.")
    else
        # Extra buffer for full rendering
        sleep(2)

        # --- EXTRACTION ---
        js_script = """
            (() => {
                const results = [];
                document.querySelectorAll(".card, .panel").forEach((card, index) => {
                    let title = "Scan_" + index;
                    let headerEl = card.querySelector('.card-header, .panel-heading');
                    if (headerEl) title = headerEl.innerText.trim();

                    let table = card.querySelector("table");
                    if (!table) return;
                    
                    let headers = Array.from(table.querySelectorAll("thead th")).map(th => th.innerText.trim());
                    let rows = [];
                    
                    table.querySelectorAll("tbody tr").forEach(tr => {
                        let tds = Array.from(tr.querySelectorAll("td"));
                        if (tds.length > 1) {
                            rows.push(tds.map(td => td.innerText.trim()));
                        }
                    });

                    if (headers.length > 0) {
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
            
            # IST Timestamp
            utc_now = now(Dates.UTC)
            ist_now = utc_now + Dates.Hour(5) + Dates.Minute(30)
            sys_timestamp = Dates.format(ist_now, "yyyy-mm-dd HH:MM:SS")
            
            println("   💾 Processing $(length(scans)) scans...")
            
            for scan in scans
                title = scan["title"]
                rows = scan["rows"]
                raw_headers = scan["headers"]
                
                # Filename Logic
                target_filename = get_clean_filename(title)
                target_path = joinpath(OUTPUT_DIR, target_filename)
                
                if isempty(rows) continue end

                println("      -> Saving '$title'")

                # Prepare Headers
                n_cols = maximum(length.(rows); init=length(raw_headers))
                while length(raw_headers) < n_cols push!(raw_headers, "Col_$(length(raw_headers)+1)") end
                safe_headers = Symbol.(raw_headers[1:n_cols])
                
                # Prepare Data
                clean_rows = []
                for r in rows
                    while length(r) < n_cols push!(r, "") end
                    push!(clean_rows, r)
                end
                
                df = DataFrame([r[i] for r in clean_rows, i in 1:n_cols], safe_headers)
                
                # Add Metadata
                insertcols!(df, 1, :Scraped_At => sys_timestamp)
                
                # Save
                file_exists = isfile(target_path)
                CSV.write(target_path, df; append=file_exists, writeheader=!file_exists)
            end
        end
    end
    println("\n✅ Job Complete.")

finally
    try close(browser) catch; end
end
