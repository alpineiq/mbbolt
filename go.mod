module github.com/alpineiq/mbbolt

go 1.19

require (
	github.com/alpineiq/genh v0.0.0-20230426193226-b53f8cef9202
	github.com/alpineiq/gserv v0.0.0-20230426185153-3aa2400540e6
	github.com/alpineiq/oerrs v0.0.0-20230412221016-05c25682e645
	github.com/alpineiq/otk v0.0.0-20230426184658-b28afce44f3f
	github.com/vmihailenco/msgpack/v5 v5.3.5
	go.etcd.io/bbolt v1.3.7-0.20221229101948-b654ce922133
)

require (
	github.com/gorilla/securecookie v1.1.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	go.oneofone.dev/oerrs v1.0.7-0.20221003171156-2726106fc553 // indirect
	go.oneofone.dev/otk v1.0.7 // indirect
	golang.org/x/crypto v0.6.0 // indirect
	golang.org/x/image v0.6.0 // indirect
	golang.org/x/net v0.7.0 // indirect
	golang.org/x/sys v0.5.0 // indirect
	golang.org/x/text v0.8.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
)

replace github.com/vmihailenco/msgpack/v5 v5.3.5 => github.com/alpineiq/msgpack/v5 v5.3.5-no-partial-alloc
