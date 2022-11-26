module go.oneofone.dev/mbbolt

go 1.19

require (
	github.com/vmihailenco/msgpack/v5 v5.3.5
	go.etcd.io/bbolt v1.3.6
	go.oneofone.dev/genh v0.0.0-20221126100704-2bef71a004e6
	go.oneofone.dev/gserv v1.0.1-0.20221104192552-a68ea5f7842f
)

require (
	github.com/gorilla/securecookie v1.1.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	go.oneofone.dev/oerrs v1.0.7-0.20221003171156-2726106fc553
	go.oneofone.dev/otk v1.0.7
	golang.org/x/crypto v0.3.0 // indirect
	golang.org/x/image v0.1.0 // indirect
	golang.org/x/net v0.2.0 // indirect
	golang.org/x/sys v0.2.0 // indirect
	golang.org/x/text v0.4.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
)

replace github.com/vmihailenco/msgpack/v5 v5.3.5 => github.com/alpineiq/msgpack/v5 v5.3.5-no-partial-alloc
