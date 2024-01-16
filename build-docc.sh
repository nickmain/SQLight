##!/bin/sh

xcrun xcodebuild docbuild \
    -scheme SQLight \
    -destination 'generic/platform=iOS Simulator' \
    -derivedDataPath "$PWD/.derivedData"

xcrun docc process-archive transform-for-static-hosting \
    "$PWD/.derivedData/Build/Products/Debug-iphonesimulator/SQLight.doccarchive" \
    --output-path ".docs" \
    --hosting-base-path "sqlight"

echo '<script>window.location.href += "documentation/sqlight"</script>' > .docs/index.html
