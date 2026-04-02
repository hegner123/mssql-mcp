binary := "mssql-mcp"
install_path := "/usr/local/bin"

build:
    go build -o {{binary}}
    codesign -s - {{binary}}

install: build
    sudo cp {{binary}} {{install_path}}/

clean:
    rm -f {{binary}}

test:
    go test -v
