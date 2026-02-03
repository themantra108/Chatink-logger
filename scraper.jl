using ChromeDevToolsLite, Dates, JSON, DataFrames, CSV

# --- 🧱 Types & Consts ---
const TARGET_URL = "https://chartink.com/dashboard/208896"
const OUTPUT_DIR = "chartink_data"

# A specific type to hold our scraped data before saving
struct WidgetTable
    name::String
    clean_name::String
    data::DataFrame
end

get_ist() = now(Dates.UTC) + Hour(5) + Minute(30)

# --- 🛠️ Core Logic ---

function connect_and_scroll()
    @info "🔌 Connecting..."
    page = ChromeDevToolsLite.connect_browser()
    ChromeDevToolsLite.goto(page, TARGET_URL)
    
    @info "📜 Scrolling..."
    # Julian way to handle nullable types: strictly check or default
    h = ChromeDevToolsLite.evaluate(page, "document.body.scrollHeight") |> safe_unwrap
    h = isa(h, Number) ? h : 5000
    
    # Range loop is very Julian
    for s in 0:800:h
        ChromeDevToolsLite.evaluate(page, "window.scrollTo(0, $s)")
        sleep(0.2)
    end
    sleep(2)
    return page
end

function extract_raw_csv(page)
    @info "⚡ Executing JS..."
    # We keep the JS payload in a separate variable for cleanliness (omitted here for brevity, assumes JS_PAYLOAD exists)
    ChromeDevToolsLite.evaluate(page, "eval($(JSON.json(JS_PAYLOAD)))")
    
    # Fetch length
    len = ChromeDevToolsLite.evaluate(page, "window._data.length") |> safe_unwrap
    len = try parse(Int, string(len)) catch; 0 end
    
    if len == 0; error("No Data Found"); end
    
    # IOBuffer is faster than string concatenation for massive data
    buf = IOBuffer()
    for i in 0:50000:len
        chunk = ChromeDevToolsLite.evaluate(page, "window._data.substring($i, $(min(i+50000, len)))") |> safe_unwrap
        print(buf, chunk)
    end
    return String(take!(buf))
end

function parse_widgets(raw_csv::String) :: Vector{WidgetTable}
    widgets = WidgetTable[]
    current_ts = get_ist()
    
    # Split by widget using regex directly on the full string? 
    # Actually, grouping by line is safer for CSV integrity.
    lines = split(raw_csv, "\n")
    groups = Dict{String, Vector{String}}()
    
    for line in lines
        length(line) < 5 && continue
        m = match(r"^\"([^\"]+)\"", line)
        key = isnothing(m) ? "Unknown" : m.captures[1]
        push!(get!(groups, key, String[]), line)
    end

    for (name, rows) in groups
        # Functional text cleaning
        clean_name = replace(name, r"[^a-zA-Z0-9]" => "_")
        
        # Heuristic to find header
        header_idx = findfirst(l -> occursin(r"\",\"(Symbol|Name|Scan Name)\"", l), rows)
        header_idx = isnothing(header_idx) ? 1 : header_idx
        
        # Build DataFrame in memory
        io = IOBuffer()
        println(io, "\"Timestamp\"," * rows[header_idx])
        for i in (header_idx+1):length(rows)
             # Filter garbage lines
             if !occursin(r"\",\"(Symbol|Name)\"", rows[i])
                 println(io, "\"$current_ts\"," * rows[i])
             end
        end
        seekstart(io)
        
        try
            df = CSV.read(io, DataFrame)
            push!(widgets, WidgetTable(name, clean_name, df))
        catch e
            @warn "Failed to parse $name"
        end
    end
    return widgets
end

function save_widget(w::WidgetTable)
    path = joinpath(OUTPUT_DIR, w.clean_name * ".csv")
    
    if isfile(path)
        old_df = CSV.read(path, DataFrame)
        # The power of DataFrames.jl: vcat + unique + sort in one chain
        final_df = vcat(old_df, w.data, cols=:union)
        unique!(final_df, [:Timestamp, :Symbol])
        sort!(final_df, :Timestamp)
        CSV.write(path, final_df)
    else
        CSV.write(path, w.data)
    end
    @info "  Saved: $(w.clean_name) ($(nrow(w.data)) rows)"
end

# --- 🏃 Main Pipeline ---
function main()
    mkpath(OUTPUT_DIR)
    
    # This is the "Julia Enthusiast" Pipeline syntax:
    try
        connect_and_scroll() |> 
        extract_raw_csv      |> 
        parse_widgets        .|> # Broadcast (map) over the vector of widgets
        save_widget
        
        @info "✅ Pipeline Complete."
    catch e
        @error "Pipeline Crashed" exception=(e, catch_backtrace())
        exit(1)
    end
end

# Helper to unwrap dicts (same as before)
function safe_unwrap(res)
    isa(res, Dict) ? (haskey(res,"value") ? res["value"] : (haskey(res,"result") ? safe_unwrap(res["result"]) : res)) : res
end

# (Insert JS_PAYLOAD const here from previous response)
const JS_PAYLOAD = "..." # Keep the simplified JS string

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
