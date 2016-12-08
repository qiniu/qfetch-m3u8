DIR=$(cd ../; pwd)
export GOPATH=$GOPATH:$DIR
GOOS="darwin" GOARCH="amd64" go build -o "../bin/qfetch-m3u8_darwin_amd64" main.go 
GOOS="darwin" GOARCH="386" go build -o "../bin/qfetch-m3u8_darwin_386" main.go
GOOS="windows" GOARCH="amd64" go build -o "../bin/qfetch-m3u8_windows_amd64.exe" main.go
GOOS="windows" GOARCH="386" go build -o "../bin/qfetch-m3u8_windows_386.exe" main.go
GOOS="linux" GOARCH="amd64" go build -o "../bin/qfetch-m3u8_linux_amd64" main.go
GOOS="linux" GOARCH="386" go build -o "../bin/qfetch-m3u8_linux_386" main.go
GOOS="linux" GOARCH="arm" go build -o "../bin/qfetch-m3u8_linux_arm" main.go
