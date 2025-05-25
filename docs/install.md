# Installing csvsql
There are a few ways to install csvsql:
## From source
To install csvsql from source, make sure you have the Rust toolchain installed. See details in [here](https://www.rust-lang.org/tools/install)

### From the repository
To install csvsql from the repository, one need to clone the repository, build the tool and copy the executable to the path. For example, on linux (assuming ~/bin is in the path):

```bash
git clone https://github.com/yift/csvsql
cd csvsql
cargo build -r
cp target/release/csvsql ~/bin
```


### Using Cargo
To install csvsql using Cargo, one can simply run:

cargo install csvsql


## From Docker
One can use csvsql docker container. Please note that this will allow you to access only the files in the container volume. For example:

```bash
docker run -it --rm -v $(pwd):/data yiftach/csvsql -m /data
```
(To install docker see [here](https://docs.docker.com/engine/install/)).


## From binary
Some operating system binaries are available in the [latest release](https://github.com/yift/csvsql/releases/latest)
