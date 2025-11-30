# Installing csvsql
There are a few ways to install csvsql:

## From source
To install csvsql from source, make sure you have the Rust toolchain installed. See details [here](https://www.rust-lang.org/tools/install)

### From the repository
To install csvsql from the repository, you need to clone the repository, build the tool, and copy the executable to your path. For example, on Linux (assuming ~/bin is in your path):
```bash
git clone https://github.com/yift/csvsql
cd csvsql
cargo build -r
cp target/release/csvsql ~/bin
```

### Using Cargo
To install csvsql using Cargo, you can simply run:
```bash
cargo install csvsql
```

## From Docker
You can use the csvsql Docker container. Please note that this will allow you to access only the files in the container volume. For example:
```bash
docker run -it --rm -v $(pwd):/data yiftach/csvsql -m /data
```
(To install Docker, see [here](https://docs.docker.com/engine/install/)).

## From binary
Some operating system binaries are available in the [latest release](https://github.com/yift/csvsql/releases/latest)

### For Apple
Please note that Apple machines might not allow you to run the binary after downloading it. If you get an error like: `Apple could not verify "csvsql" is free of malware that may harm your Mac or compromise your privacy`, you can follow these steps:
1. Allow `csvsql` to be executed from the `Privacy & Security` settings. See [Apple Support](https://support.apple.com/en-gb/102445) for details.
2. Allow `csvsql` to be executed from the command line using:
```bash
chmod +x ./csvsql
xattr -d com.apple.quarantine ./csvsql
```
3. Allow all apps downloaded from GitHub to be executed from the `Privacy & Security` settings. See [Apple Support](https://support.apple.com/en-gb/102445) for details.
4. Use another type of installation (see above).
