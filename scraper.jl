using ChromeDevToolsLite
using Dates
using Base64

println("🚀 Starting DIAGNOSTIC Job at $(now())")

try
    # 1. Connect
    page = ChromeDevToolsLite.connect_browser() 
    println("✅ Connected to Chrome!")

    target_url = "https://chartink.com/dashboard/208896"
    println("🧭 Navigating to: $target_url")
    ChromeDevToolsLite.goto(page, target_url)

    # 2. Wait a long time (30s) to rule out slow internet
    println("⏳ Waiting 30s for full render...")
    sleep(30)

    # 3. 📸 TAKE A SCREENSHOT (The Smoking Gun)
    # This will save a PNG so you can see if it's a blank screen or a login page
    println("📸 Taking screenshot...")
    screenshot_json = ChromeDevToolsLite.execute_cdp(page, "Page.captureScreenshot", Dict("format" => "png"))
    
    if haskey(screenshot_json, "data")
        # Decode base64 and save to file
        open("debug_screenshot.png", "w") do io
            write(io, base64decode(screenshot_json["data"]))
        end
        println("✅ Screenshot saved to debug_screenshot.png")
    else
        println("❌ Failed to capture screenshot.")
    end

    # 4. 📄 DUMP FULL HTML
    println("💾 Saving full HTML...")
    html_dump = ChromeDevToolsLite.evaluate(page, "document.documentElement.outerHTML")
    html_content = isa(html_dump, Dict) ? html_dump["value"] : html_dump
    
    open("debug_page.html", "w") do io
        print(io, html_content)
    end
    println("✅ HTML saved to debug_page.html")

    # 5. 🔍 PRINT VISIBLE TEXT (To Console)
    # This tells us if the bot can "read" any text at all
    println("-" ^ 20, " VISIBLE PAGE TEXT ", "-" ^ 20)
    text_check = ChromeDevToolsLite.evaluate(page, "document.body.innerText")
    body_text = isa(text_check, Dict) ? text_check["value"] : text_check
    
    # Print first 500 chars only to avoid spamming logs
    println(first(body_text, 500)) 
    println("-" ^ 60)

catch e
    println("💥 Critical Error: $e")
    rethrow(e)
end
