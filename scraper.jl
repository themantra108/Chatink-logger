import Pkg; using Dates, Sockets
try using ChromeDevToolsLite, DataFrames, JSON, CSV catch; Pkg.add(["DataFrames", "JSON", "CSV"]); Pkg.add(url="https://github.com/svilupp/ChromeDevToolsLite.jl"); using ChromeDevToolsLite, DataFrames, JSON, CSV end

# --- SETUP ---
OUTPUT_DIR = "scans"
if !isdir(OUTPUT_DIR) mkdir(OUTPUT_DIR) end

# Launch Chrome
cmd = `google-chrome --headless=new --no-sandbox --disable-gpu --remote-debugging-port=9222 --user-data-dir=/tmp/chrome-data`
run(pipeline(cmd, stdout=devnull, stderr=devnull), wait=false)

# Wait for Chrome to start
ready=false; for i in 1:20 try connect("127.0.0.1", 9222); global ready=true; break; catch; sleep(1) end end
if !ready error("Chrome failed") end

# --- SCRAPE ---
browser = connect_browser()
try
    goto(browser, "https://chartink.com/dashboard/208896")
    
    # Simple Wait: Wait up to 30s for tables to appear, then wait 5s for data to fill
    for i in 1:30
        if evaluate(browser, "document.querySelectorAll('.card, .panel').length > 0") == true break end
        sleep(1)
    end
    sleep(5) 

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
            if(h.length > 0) res.push({title: t, headers: h, rows: r});
        });
        return JSON.stringify(res);
    })()
    """
    
    # Process Data
    data = JSON.parse(evaluate(browser, js))
    ts = Dates.format(now(Dates.UTC) + Dates.Hour(5) + Dates.Minute(30), "yyyy-mm-dd HH:MM:SS")

    for s in data
        if isempty(s["rows"]) continue end
        
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
        println("Saved: $(s["title"])")
    end
finally
    try close(browser) catch; end
end
