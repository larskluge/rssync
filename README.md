# rssync

Sync a source Redis Stream to another destination


## Build

```
shards install
shards build --release
bin/rssync # prints help
```


## Usage

```
rssync redis://admin:password1@redis.io:6379/0/stream redis://localhost:6379/stream-backup
```
