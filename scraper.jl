import Pkg; using Dates, Sockets
try using ChromeDevToolsLite, DataFrames, JSON, CSV catch; Pkg.add(["DataFrames", "JSON", "CSV"]); Pkg.add(url="https://github.com/svilupp/ChromeDevToolsLite.jl"); using ChromeDevToolsLite, DataFrames, JSON, CSV end

# --- SETUP ---
OUTPUT_DIR = "scans"
if !isdir(OUTPUT_DIR) mkdir(OUTPUT_DIR) end

println(">> Launching Chrome...")
cmd = `google-chrome --headless=new --no-sandbox --disable-gpu --remote-debugging-port=9222 --user-data-dir=/tmp/chrome-data`
run(pipeline(cmd, stdout=devnull, stderr=devnull), wait=false)

# Wait for Chrome to be ready
is_ready = false
for i in 1:20 
    try connect("127.0.0.1", 9222); global is_ready=true; break; catch; sleep(1) end 
end
if !is_ready error("Chrome failed to start") end

# --- SCRAPE ---
browser = connect_browser()
try
    println(">> Navigating...")
    goto(browser, "https://chartink.com/dashboard/208896")
    
    # 🛠 FIX: Dumb Wait (Reliable)
    # Don't check for "Ready" state. Just wait 15s for data to definitely appear.
    println(">> Waiting 15 seconds for data...")
    sleep(15)

    # Extract Data
    js = """
    (() => {
        const res = [];
        document.querySelectorAll(".card, .panel").forEach((c, i) => {
            let t = c.querySelector('.card-header, .panel-heading')?.innerText.trim() || "Scan_" + i;
            let h = Array.from(c.querySelectorAll("thead th")).map(x => x.innerText.trim());
            let r = [];
            
            c.querySelectorAll("tbody tr").forEach(tr => {
                let d = Array.from(tr.querySelectorAll("td"));
                // Only keep rows with actual data columns
                if(d.length > 1) r.push(d.map(x => x.innerText.trim()));
            });

            // Save if headers exist (even if rows are empty, so we verify it ran)
            if(h.length > 0) res.push({title: t, headers: h, rows: r});
        });
        return JSON.stringify(res);
    })()
    """
    
    data = JSON.parse(evaluate(browser, js))
    ts = Dates.format(now(Dates.UTC) + Dates.Hour(5) + Dates.Minute(30), "yyyy-mm-dd HH:MM:SS")

    println(">> Found $(length(data)) scans.")

    for s in data
        # Clean Filename
        clean_name = replace(s["title"], r"\(.*?\)" => "", r"[^a-zA-Z0-9]" => "_", r"__+" => "_")
        filename = "scan_" * strip(clean_name, ['_']) * ".csv"
        path = joinpath(OUTPUT_DIR, filename)
        
        # Skip empty scans (don't save empty files)
        if isempty(s["rows"]) 
            println("   -- Empty: $(s["title"])")
            continue 
        end
        
        # Fix Headers
        rows = s["rows"]; raw_h = s["headers"]
        n = maximum(length.(rows))
        while length(raw_h) < n push!(raw_h, "Col_$(length(raw_h)+1)") end
        
        # Save
        df = DataFrame([r[i] for r in rows, i in 1:n], Symbol.(raw_h[1:n]))
        insertcols!(df, 1, :Scraped_At => ts)
        CSV.write(path, df; append=isfile(path), writeheader=!isfile(path))
        println("   ✔ Saved: $(s["title"])")
    end
finally
    try close(browser) catch; end
end
