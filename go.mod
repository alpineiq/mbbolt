module go.oneofone.dev/mbbolt

go 1.19

require (
	go.etcd.io/bbolt v1.3.6
	go.oneofone.dev/genh v0.0.0-20220818195556-5786c73a5d1f
	go.oneofone.dev/gserv v1.0.0
)

require golang.org/x/net v0.0.0-20220907135653-1e95f45603a7 // indirect

require (
	github.com/google/uuid v1.3.0
	github.com/gorilla/securecookie v1.1.1 // indirect
	github.com/vmihailenco/msgpack/v5 v5.3.5 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	go.oneofone.dev/oerrs v1.0.6
	go.oneofone.dev/otk v1.0.7 // indirect
	golang.org/x/crypto v0.0.0-20220829220503-c86fa9a7ed90 // indirect
	golang.org/x/image v0.0.0-20220902085622-e7cb96979f69 // indirect
	golang.org/x/sys v0.0.0-20220908150016-7ac13a9a928d // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
)

replace (
	github.com/vmihailenco/msgpack/v5 v5.3.5 => github.com/alpineiq/msgpack/v5 v5.3.5-no-partial-alloc
	go.oneofone.dev/gserv => ../gserv
)
