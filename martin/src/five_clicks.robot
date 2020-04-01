*** Settings ***
Documentation     Navigate Wikipedia articles starting at a random page and stoping at a target page
Library           SeleniumLibrary
Library           Process
Library           String

*** Variables ***
${target}         https://en.wikipedia.org/wiki/Jesus

*** Test Cases ***
You Need Jesus
    Open Browser    https://en.wikipedia.org/wiki/Special:Random    Firefox
    ${start}=    Get Location
    ${result}=    Run Process    python    ./WikiCrawler.py    -c    25    ${start}    ${target}    -k    jesus    -k    christ    -k    religion
    @{urls}=    Split String    ${result.stdout}
    FOR    ${url}    IN    @{urls[1:]}
        ${next}=    Fetch From Right    ${url}    wikipedia.org
        ${locator}=    Set Variable    css:a[href="${next}"]
        Capture Element Screenshot    ${locator}    EMBED
        Click Element    ${locator}
    END
    Capture Page Screenshot    EMBED
    Close All Browsers
