module go.oneofone.dev/mbbolt

go 1.18

require (
	go.etcd.io/bbolt v1.3.6
	go.oneofone.dev/genh v0.0.0-20220818195556-5786c73a5d1f
	go.oneofone.dev/gserv v1.0.1-0.20220711160558-3a247f8bf248
)

require golang.org/x/net v0.0.0-20220812174116-3211cb980234 // indirect

require (
	github.com/google/uuid v1.3.0
	github.com/gorilla/securecookie v1.1.1 // indirect
	github.com/vmihailenco/msgpack/v5 v5.3.5 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	go.oneofone.dev/oerrs v1.0.6
	go.oneofone.dev/otk v1.0.7 // indirect
	golang.org/x/crypto v0.0.0-20220817201139-bc19a97f63c8 // indirect
	golang.org/x/image v0.0.0-20220722155232-062f8c9fd539 // indirect
	golang.org/x/sys v0.0.0-20220728004956-3c1f35247d10 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/xerrors v0.0.0-20220609144429-65e65417b02f // indirect
)

replace (
	github.com/vmihailenco/msgpack/v5 v5.3.5 => github.com/alpineiq/msgpack/v5 v5.3.5-no-partial-alloc
	go.oneofone.dev/gserv => ../gserv
)
