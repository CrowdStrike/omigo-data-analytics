Some setup and run instructions (not tested)

# download image
wget https://golang.org/dl/go1.17.linux-amd64.tar.gz

# unzip
tar xzvf ~/downloads/go1.17.linux-amd64.tar.gz      

# copy go binary
sudo mv go /usr/local/   

# change permissions
sudo chwon -R root:root /usr/local/go

# set local path
export PATH=$PATH:/usr/local/go/bin
export GO111MODULE=off
export GOPATH=<setme>
export GOPRIVATE=<setme>
export GOPROXY=<setme>
export GOROOT=/usr/local/go
export ZLE_LINE_ABORTED='echo $GO'

# download modules
go get github.com/aws/aws-sdk-go
go get github.com/hashicorp/golang-lru

# run main

