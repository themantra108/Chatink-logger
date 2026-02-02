import Pkg; using Dates, Sockets
try using ChromeDevToolsLite, DataFrames, JSON, CSV catch; Pkg.add(["DataFrames", "JSON", "CSV"]); Pkg.add(url="https://github.com/svilupp/ChromeDevToolsLite.jl"); using ChromeDevToolsLite, DataFrames, JSON, CSV end

# --- SETUP ---
OUTPUT_DIR = "scans"
if !isdir(OUTPUT_DIR) mkdir(OUTPUT_DIR) end

# Launch Chrome
println(">> Launching Chrome...")
cmd = `google-chrome --headless=new --no-sandbox --disable-gpu --remote-debugging-port=9222 --user-data-dir=/tmp/chrome-data`
run(pipeline(cmd, stdout=devnull, stderr=devnull), wait=false)

# Wait for Chrome
is_ready = false
for i in 1:20 
    try connect("127.0.0.1", 9222); global is_ready=true; break; catch; sleep(1) end 
end
if !is_ready error("Chrome failed to start") end

# --- SCRAPE ---
browser = connect_browser()
try
    println(">> Navigating to dashboard...")
    goto(browser, "https://chartink.com/dashboard/208896")
    
    # 🛠 FIX 1: Better Wait Logic
    println(">> Waiting for tables...")
    data_detected = false
    for i in 1:60
        # Check if we have rows with actual data (more than 1 column)
        # OR if we explicitly see "No records" (which means it finished loading empty)
        status = evaluate(browser, """
            (() => {
                const rows = document.querySelectorAll('tbody tr');
                if (rows.length === 0) return false;
                
                // Look for at least one row that isn't a "Loading..." message
                for (let row of rows) {
                    if (!row.innerText.includes("Loading")) return true;
                }
                return false;
            })()
        """)
        
        if status == true
            global data_detected = true
            println("   ✔ Data detected at $(i)s.")
            break
        end
        sleep(1)
    end

    # 🛠 FIX 2: Force Wait for Slow Tables
    # Even if one table loads, others might still be spinning. Give them 10s.
    println("   ⏳ Waiting 10s for all AJAX calls to finish...")
    sleep(10)

    # Extract JS (Now robust against empty/loading states)
    js = """
    (() => {
        const res = [];
        document.querySelectorAll(".card, .panel").forEach((c, i) => {
            // Get Title
            let t = c.querySelector('.card-header, .panel-heading')?.innerText.trim() || "Scan_" + i;
            
            // Get Headers
            let h = Array.from(c.querySelectorAll("thead th")).map(x => x.innerText.trim());
            
            // Get Rows (Filter out 'No records' or single-column messages)
            let r = [];
            c.querySelectorAll("tbody tr").forEach(tr => {
                let d = Array.from(tr.querySelectorAll("td"));
                if(d.length > 1) r.push(d.map(x => x.innerText.trim()));
            });

            // We push the scan even if rows are empty, so we know we checked it
            if(h.length > 0) res.push({title: t, headers: h, rows: r});
        });
        return JSON.stringify(res);
    })()
    """
    
    # Process Data
    data = JSON.parse(evaluate(browser, js))
    ts = Dates.format(now(Dates.UTC) + Dates.Hour(5) + Dates.Minute(30), "yyyy-mm-dd HH:MM:SS")

    println(">> Found $(length(data)) total scans.")

    for s in data
        # Clean Filename
        clean_name = replace(s["title"], r"\(.*?\)" => "", r"[^a-zA-Z0-9]" => "_", r"__+" => "_")
        filename = "scan_" * strip(clean_name, ['_']) * ".csv"
        path = joinpath(OUTPUT_DIR, filename)
        
        if isempty(s["rows"])
            println("   ⚠ Skipping: $(s["title"]) (Empty/No Data)")
            continue 
        end
        
        # Fix Headers & Rows
        rows = s["rows"]; raw_h = s["headers"]
        n = maximum(length.(rows))
        while length(raw_h) < n push!(raw_h, "Col_$(length(raw_h)+1)") end
        
        # Save
        df = DataFrame([r[i] for r in rows, i in 1:n], Symbol.(raw_h[1:n]))
        insertcols!(df, 1, :Scraped_At => ts)
        CSV.write(path, df; append=isfile(path), writeheader=!isfile(path))
        println("   ✔ Saved: $(s["title"]) -> $filename")
    end
finally
    try close(browser) catch; end
end
