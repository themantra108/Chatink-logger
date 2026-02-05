using ChromeDevToolsLite, Dates, JSON, DataFrames, CSV

# ==============================================================================
# 1. 🧱 CONFIGURATION
# ==============================================================================
const TARGET_URLS = [
    "https://chartink.com/dashboard/419640",
    "https://chartink.com/dashboard/208896"
]
const OUTPUT_ROOT = "chartink_data"
const NAV_SLEEP_SEC = 15
const SCROLL_STEP = 2000
const SCROLL_SLEEP = 0.5

const DATE_REGEX = r"(\d+)(?:st|nd|rd|th)?\s+([a-zA-Z]+)"
const MONTH_MAP = Dict{SubString{String}, Int}(
    "Jan" => 1, "Feb" => 2, "Mar" => 3, "Apr" => 4, "May" => 5, "Jun" => 6,
    "Jul" => 7, "Aug" => 8, "Sep" => 9, "Oct" => 10, "Nov" => 11, "Dec" => 12
)

get_ist() = now(Dates.UTC) + Hour(5) + Minute(30)

struct WidgetTable
    name::String
    clean_name::String
    data::DataFrame
    subfolder::String
    has_date_col::Bool 
end

# ==============================================================================
# 2. 🛡️ FILTERS (The "Junk" Remover)
# ==============================================================================
function is_junk_widget(df::DataFrame)
    if nrow(df) == 0; return true; end
    
    # Check the very first cell of the table
    # Chartink filter descriptions always start with #, *, or Clause
    first_val = string(df[1,1])
    
    if occursin("Clause", first_val) || occursin("*", first_val) || occursin("#", first_val)
        return true # It is junk
    end
    
    return false # It is real data
end

# ==============================================================================
# 3. 📅 PARSING LOGIC
# ==============================================================================
function parse_chartink_date(date_str::AbstractString)
    m = match(DATE_REGEX, date_str)
    if isnothing(m); return (0, 0); end
    day = parse(Int, m.captures[1])
    mon = get(MONTH_MAP, titlecase(m.captures[2])[1:3], 0)
    return (day, mon)
end

function enrich_dataframe!(df::DataFrame)
    if "Timestamp" in names(df)
        df[!, :Scan_Date] = Date.(df[!, :Timestamp])
    end

    if "Date" in names(df)
        nrows = nrow(df)
        full_dates = Vector{Union{Date, Missing}}(missing, nrows)
        scrape_date = Date(get_ist())
        current_year = year(scrape_date)
        last_month = month(scrape_date) 
        date_col = df.Date
        
        @inbounds for i in 1:nrows
            raw_val = string(date_col[i])
            (day, mon) = parse_chartink_date(raw_val)
            if day == 0 || mon == 0; continue; end
            
            if last_month < 3 && mon > 10; current_year -= 1;
            elseif last_month > 10 && mon < 3; current_year += 1; end
            
            try
                cand = Date(current_year, mon, day)
                if cand > (scrape_date + Day(2)); cand = Date(current_year - 1, mon, day); current_year -= 1; end
                full_dates[i] = cand
            catch; end
            last_month = mon
        end
        df[!, :Full_Date] = full_dates
    end
end

# ==============================================================================
# 4. 💾 SAVING LOGIC
# ==============================================================================
function save_to_disk(w::WidgetTable, final_df::DataFrame)
    folder_path = joinpath(OUTPUT_ROOT, w.subfolder)
    mkpath(folder_path)
    path = joinpath(folder_path, w.clean_name * ".csv")
    sort!(final_df, :Timestamp, rev=true)
    CSV.write(path, final_df)
    @info "  💾 Saved: [$(w.subfolder)] -> $(w.clean_name)"
end

function has_row_changed(new_row, old_row, check_cols)
    for col in check_cols
        val_new = hasproperty(new_row, col) ? new_row[col] : missing
        val_old = hasproperty(old_row, col) ? old_row[col] : missing
        if !isequal(val_new, val_old); return true; end
    end
    return false
end

function save_widget(w::WidgetTable)
    path = joinpath(OUTPUT_ROOT, w.subfolder, w.clean_name * ".csv")
    if !isfile(path); save_to_disk(w, w.data); return; end

    old_df = CSV.read(path, DataFrame)
    
    if w.has_date_col
        new_df = w.data
        meta_cols = ["Timestamp", "Full_Date", "Scan_Date"]
        content_cols = filter(n -> !(n in meta_cols), names(new_df))
        
        old_map = Dict{String, DataFrameRow}()
        for row in eachrow(old_df)
            d = string(row.Date)
            if !haskey(old_map, d); old_map[d] = row; end
        end
        
        rows_to_keep = Int[]
        for i in 1:nrow(new_df)
            new_row = new_df[i, :]
            d_key = string(new_row.Date)
            if !haskey(old_map, d_key) || has_row_changed(new_row, old_map[d_key], content_cols)
                push!(rows_to_keep, i)
            end
        end
        
        if isempty(rows_to_keep); return; end
        combined = vcat(new_df[rows_to_keep, :], old_df, cols=:union)
        unique!(combined, :Date)
        save_to_disk(w, combined)
    else
        if "Scan_Date" in names(w.data)
            active_dates = unique(dropmissing(w.data, :Scan_Date).Scan_Date)
            filter!(row -> ismissing(row.Scan_Date) || !(row.Scan_Date in active_dates), old_df)
        end
        save_to_disk(w, vcat(w.data, old_df, cols=:union))
    end
end

# ==============================================================================
# 5. 🌐 BROWSER INTERACTION
# ==============================================================================
function scroll_page(page)
    h = ChromeDevToolsLite.evaluate(page, "document.body.scrollHeight")
    h_val = isa(h, Dict) ? h["value"] : h
    h_int = isa(h_val, Number) ? h_val : 5000
    for s in 0:SCROLL_STEP:h_int
        ChromeDevToolsLite.evaluate(page, "window.scrollTo(0, $s)")
        sleep(SCROLL_SLEEP)
    end
    sleep(2)
end

const JS_PAYLOAD = """
(() => {
    try {
        let out = [];
        const cln = (t) => t ? t.trim().replace(/"/g, '""').replace(/\\n/g, " ") : "";
        const cleanHeader = (txt) => {
            if (!txt) return "";
            return txt.replace(/Sort table by[\\s\\S]*/i, "").replace(/\\n/g, " ").replace(/\\s+/g, " ").trim();
        };
        const scan = (nodes, forcedName) => {
            nodes.forEach((n, i) => {
                let name = forcedName;
                if (!name) {
                    let curr = n, d = 0;
                    while (curr && d++ < 12) {
                        let sib = curr.previousElementSibling;
                        while (sib) {
                            let txt = sib.innerText || "";
                            if (txt.length>2 && txt.length<150 && !/Loading|Error|Run/.test(txt)) {
                                name = txt.split('\\n')[0].trim(); break;
                            }
                            sib = sib.previousElementSibling;
                        }
                        if (name) break;
                        curr = curr.parentElement;
                    }
                }
                if (!name) name = "Unknown Widget " + i;
                const rows = n.querySelectorAll("tr");
                if (!rows.length) return;
                rows.forEach(r => {
                    if (r.innerText.includes("No data")) return;
                    const cells = Array.from(r.querySelectorAll("th, td"));
                    if (!cells.length) return;
                    const isHead = r.querySelector("th") !== null;
                    const line = cells.map(c => {
                        let val = isHead ? cleanHeader(c.innerText) : cln(c.innerText);
                        return '"' + val + '"';
                    }).join(",");
                    out.push('"' + cln(name) + '",' + line);
                });
            });
        };
        document.querySelectorAll("table, div.dataTables_wrapper").forEach(n => {
            if (n.tagName === "TABLE") scan([n]);
        });
        document.querySelectorAll("div.card").forEach(c => {
            const h = c.querySelector(".card-header, h1, h2, h3, h4, h5, h6");
            const t = c.querySelector("table");
            if (h && t) scan([t], "MANUAL_CATCH_" + cln(h.innerText));
        });
        window._data = [...new Set(out)].join("\\n");
        return "DONE";
    } catch(e) { return "ERR:" + e; }
})()
"""

function extract_and_parse(page, folder_name) :: Vector{WidgetTable}
    @info "⚡ Extracting..."
    ChromeDevToolsLite.evaluate(page, "eval($(JSON.json(JS_PAYLOAD)))")
    len_res = ChromeDevToolsLite.evaluate(page, "window._data ? window._data.length : 0")
    len_val = isa(len_res, Dict) ? len_res["value"] : len_res
    len = try parse(Int, string(len_val)) catch; 0 end
    
    if len == 0; return WidgetTable[]; end
    
    buf = IOBuffer()
    for i in 0:50000:len
        chunk = ChromeDevToolsLite.evaluate(page, "window._data.substring($i, $(min(i+50000, len)))")
        print(buf, isa(chunk, Dict) ? chunk["value"] : chunk)
    end
    
    raw_csv = String(take!(buf))
    widgets = WidgetTable[]
    groups = Dict{String, Vector{String}}()
    
    for line in eachline(IOBuffer(raw_csv))
        if length(line) < 5 || !startswith(line, "\""); continue; end
        m = match(r"^\"([^\"]+)\"", line)
        key = isnothing(m) ? "Unknown" : replace(m.captures[1], "MANUAL_CATCH_" => "")
        push!(get!(groups, key, String[]), line)
    end

    for (name, rows) in groups
        clean_name = replace(name, r"[^a-zA-Z0-9]" => "_")[1:min(end,50)]
        header_idx = findfirst(l -> occursin(r"\",\"(Symbol|Name|Scan Name|Date)\"", l), rows)
        io = IOBuffer()
        start_row, expected_cols = 1, 0
        
        if !isnothing(header_idx)
            raw_header = replace(rows[header_idx], r"^\"[^\"]+\"," => "")
            println(io, "\"Timestamp\"," * raw_header)
            start_row = header_idx + 1
            expected_cols = length(split(rows[header_idx], "\",\""))
        else
            cols_count = length(split(replace(rows[1], r"^\"[^\"]+\"," => ""), "\",\""))
            println(io, "\"Timestamp\"," * join(["\"Col_$i\"" for i in 1:cols_count], ","))
            expected_cols = cols_count + 1
        end
        
        current_ts = get_ist()
        valid_count = 0
        for i in start_row:length(rows)
            if abs(length(split(rows[i], "\",\"")) - expected_cols) > 2; continue; end
            clean_row = replace(rows[i], r"^\"[^\"]+\"," => "")
            if !occursin(r"\"(Symbol|Name|Date)\"", clean_row)
                 println(io, "\"$current_ts\"," * clean_row)
                 valid_count += 1
            end
        end
        
        if valid_count > 0
            seekstart(io)
            try
                df = CSV.read(io, DataFrame; strict=false, silencewarnings=true)
                
                # 🛑 THE FIX: Filter Junk BEFORE doing anything else
                if is_junk_widget(df)
                    @warn "🗑️ Discarding Junk Widget: $clean_name"
                    continue 
                end
                
                enrich_dataframe!(df)
                has_date = "Date" in names(df)
                push!(widgets, WidgetTable(name, clean_name, df, folder_name, has_date))
            catch; end
        end
    end
    return widgets
end

function get_dashboard_name(page)
    raw_title = ChromeDevToolsLite.evaluate(page, "document.title") 
    val = isa(raw_title, Dict) ? raw_title["value"] : raw_title
    if isnothing(val) || val == ""; return "Unknown_Dashboard"; end
    
    clean_title = replace(val, " - Chartink.com" => "") |> 
                  x -> replace(x, " - Chartink" => "") |> 
                  x -> replace(x, r"[^a-zA-Z0-9 \-_]" => "") |> 
                  x -> replace(strip(x), " " => "_")
    return isempty(clean_title) ? "Dashboard_Unknown" : clean_title
end

function process_url(page, url)
    @info "🧭 Navigating: $url"
    ChromeDevToolsLite.evaluate(page, "window._data = null;")
    success = false
    for attempt in 1:3
        try
            ChromeDevToolsLite.goto(page, url)
            success = true; break
        catch e; sleep(5); end
    end
    if !success; throw(ErrorException("Failed to load")); end
    
    sleep(NAV_SLEEP_SEC)
    folder_name = get_dashboard_name(page)
    scroll_page(page)
    return extract_and_parse(page, folder_name)
end

function main()
    mkpath(OUTPUT_ROOT)
    try
        @info "🔌 Connecting..."
        page = ChromeDevToolsLite.connect_browser()
        @sync begin
            for url in TARGET_URLS
                widgets = process_url(page, url)
                if !isempty(widgets)
                    for w in widgets
                        @async try save_widget(w) catch e; @error "Save failed: $e"; end
                    end
                end
            end
        end
        @info "🎉 Scrape Complete."
    catch e
        @error "Crash" exception=(e, catch_backtrace())
        exit(1)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__; main(); end
