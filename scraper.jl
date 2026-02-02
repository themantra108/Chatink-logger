import Pkg; using Dates, Sockets
try using ChromeDevToolsLite, DataFrames, JSON, CSV catch; Pkg.add(["DataFrames", "JSON", "CSV"]); Pkg.add(url="https://github.com/svilupp/ChromeDevToolsLite.jl"); using ChromeDevToolsLite, DataFrames, JSON, CSV end

# --- SETUP ---
OUTPUT_DIR = "scans"
if !isdir(OUTPUT_DIR) mkdir(OUTPUT_DIR) end

# Launch Chrome
cmd = `google-chrome --headless=new --no-sandbox --disable-gpu --remote-debugging-port=9222 --user-data-dir=/tmp/chrome-data`
run(pipeline(cmd, stdout=devnull, stderr=devnull), wait=false)

# Wait for Chrome
ready=false; for i in 1:20 try connect("127.0.0.1", 9222); global ready=true; break; catch; sleep(1) end end
if !ready error("Chrome failed") end

# --- SCRAPE ---
browser = connect_browser()
try
    goto(browser, "https://chartink.com/dashboard/208896")
    
    # 🛠 FIX: Wait specifically for DATA ROWS, not just empty tables
    println("Waiting for data...")
    for i in 1:60
        # Check if rows exist OR if "No records" text exists
        ready = evaluate(browser, "document.querySelectorAll('tbody tr').length > 0 || document.body.innerText.includes('No records')")
        if ready == true break end
        sleep(1)
    end
    sleep(5) # Safety buffer

    # Extract JS
    js = """
    (() => {
        const res = [];
        document.querySelectorAll(".card, .panel").forEach((c, i) => {
            let t = c.querySelector('.card-header, .panel-heading')?.innerText.trim() || "Scan_" + i;
            let h = Array.from(c.querySelectorAll("thead th")).map(x => x.innerText.trim());
            let r = [];
            c.querySelectorAll("tbody tr").forEach(tr => {
                let d = Array.from(tr.querySelectorAll("td"));
                if(d.length > 1) r.push(d.map(x => x.innerText.trim()));
            });
            if(h.length > 0 && r.length > 0) res.push({title: t, headers: h, rows: r});
        });
        return JSON.stringify(res);
    })()
    """
    
    # Process Data
    data = JSON.parse(evaluate(browser, js))
    ts = Dates.format(now(Dates.UTC) + Dates.Hour(5) + Dates.Minute(30), "yyyy-mm-dd HH:MM:SS")

    println("Found $(length(data)) scans with data.")

    for s in data
        # Clean Filename
        clean_name = replace(s["title"], r"\(.*?\)" => "", r"[^a-zA-Z0-9]" => "_", r"__+" => "_")
        path = joinpath(OUTPUT_DIR, "scan_" * strip(clean_name, ['_']) * ".csv")
        
        # Fix Headers & Rows
        rows = s["rows"]; raw_h = s["headers"]
        n = maximum(length.(rows))
        while length(raw_h) < n push!(raw_h, "Col_$(length(raw_h)+1)") end
        
        # Save
        df = DataFrame([r[i] for r in rows, i in 1:n], Symbol.(raw_h[1:n]))
        insertcols!(df, 1, :Scraped_At => ts)
        CSV.write(path, df; append=isfile(path), writeheader=!isfile(path))
        println("Saved: $(s["title"]) -> $path")
    end
finally
    try close(browser) catch; end
end