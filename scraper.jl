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
LOOPS_PER_RUN = 3
SLEEP_BETWEEN_LOOPS = 40 
MAX_WAIT_SECONDS = 60  

# 📂 OUTPUT FOLDER
OUTPUT_DIR = "scans"
if !isdir(OUTPUT_DIR)
    mkdir(OUTPUT_DIR)
end

# --- HELPER: CLEAN FILENAMES ---
function get_clean_filename(raw_text)
    # Remove time pattern like (10:30 AM)
    time_regex = r"\(?\d{1,2}:\d{2}\s?(?:AM|PM|am|pm)?\)?"
    clean_name = replace(raw_text, time_regex => "")
    
    # Clean special chars
    clean_name = replace(clean_name, r"[^a-zA-Z0-9]" => "_") 
    clean_name = replace(clean_name, r"__+" => "_")          
    clean_name = strip(clean_name, ['_'])                    
    
    if isempty(clean_name) clean_name = "Unknown_Scan" end
    return "scan_" * first(clean_name, 40) * ".csv"
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

# --- 3. SCRAPING LOOP ---
println(">> 3. Starting Intelligent Scraper...")
browser = connect_browser()

try
    url = "https://chartink.com/dashboard/208896"
    goto(browser, url)

    for iter in 1:LOOPS_PER_RUN
        println("\n--- ⚡ Iteration $iter / $LOOPS_PER_RUN ---")
        
        if iter > 1
            println("   Refreshing page...")
            reload(browser)
        end

        println("   Waiting for tables...")
        
        # --- 🚀 ROBUST SMART WAIT LOGIC ---
        for i in 1:MAX_WAIT_SECONDS
            status = evaluate(browser, """
                (() => {
                    let containers = document.querySelectorAll('.card, .panel');
                    let total = containers.length;
                    let ready = 0;
                    containers.forEach(c => {
                        let text = c.innerText;
                        let has_rows = c.querySelectorAll('tbody tr').length > 0;
                        let is_empty = text.includes("No records") || text.includes("No stocks match");
                        let is_processing = text.includes("Processing");
                        if (!is_processing && (has_rows || is_empty)) ready++;
                    });
                    return { total: total, ready: ready };
                })()
            """)
            
            # FIX: Check if status is valid before accessing keys
            if status === nothing || !haskey(status, "ready")
                if i % 5 == 0
                    println("      ⚠ Page loading... (JS not ready)")
                end
                sleep(1)
                continue
            end
            
            # Now it is safe to read keys
            if status["ready"] >= status["total"] && status["total"] > 0
                println("   ✔ All $(status["total"]) tables loaded in $(i)s.")
                break
            end
            
            if i % 5 == 0
                println("      Waiting... ($(status["ready"]) / $(status["total"]) tables ready)")
            end
            sleep(1)
        end
        
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
                        if (tds.length > 1) rows.push(tds.map(td => td.innerText.trim()));
                    });

                    if (rows.length > 0) {
                        results.push({ "raw_title": title, "headers": headers, "rows": rows });
                    }
                });
                return JSON.stringify({ "scans": results });
            })()
        """
        
        data_str = evaluate(browser, js_script)
        
        if data_str !== nothing
            data = JSON.parse(data_str)
            scans = data["scans"]
            
            # IST System Timestamp
            utc_now = now(Dates.UTC)
            ist_now = utc_now + Dates.Hour(5) + Dates.Minute(30)
            sys_timestamp = Dates.format(ist_now, "yyyy-mm-dd HH:MM:SS")
            
            println("   💾 Processing $(length(scans)) active scans...")
            
            for scan in scans
                raw_title = scan["raw_title"]
                rows = scan["rows"]
                raw_headers = scan["headers"]
                
                # Filename Logic
                target_filename = get_clean_filename(raw_title)
                target_path = joinpath(OUTPUT_DIR, target_filename)
                
                println("      -> '$raw_title' => $target_filename")

                # Prepare Headers
                n_cols = maximum(length.(rows))
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
        
        if iter < LOOPS_PER_RUN
            println("   ⏳ Sleeping $(SLEEP_BETWEEN_LOOPS)s...")
            sleep(SLEEP_BETWEEN_LOOPS)
        end
    end
    println("\n✅ Job Complete.")

finally
    try close(browser) catch; end
end
