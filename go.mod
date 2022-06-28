module go.oneofone.dev/mbbolt

go 1.18

require (
	go.etcd.io/bbolt v1.3.6
	go.oneofone.dev/genh v0.0.0-20220608195125-451317e3176f
	go.oneofone.dev/gserv v0.0.0-20220608200243-8dc651995a1d
	golang.org/x/net v0.0.0-20220607020251-c690dde0001d
)

require (
	github.com/google/uuid v1.3.0
	github.com/gorilla/securecookie v1.1.1 // indirect
	github.com/vmihailenco/msgpack/v5 v5.3.5 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	go.oneofone.dev/oerrs v1.0.6 // indirect
	go.oneofone.dev/otk v1.0.7-0.20220531144415-25d6454ec911 // indirect
	golang.org/x/crypto v0.0.0-20220525230936-793ad666bf5e // indirect
	golang.org/x/image v0.0.0-20220601225756-64ec528b34cd // indirect
	golang.org/x/sys v0.0.0-20220610221304-9f5ed59c137d // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/xerrors v0.0.0-20220609144429-65e65417b02f // indirect
)

replace (
	github.com/vmihailenco/msgpack/v5 v5.3.5 => github.com/alpineiq/msgpack/v5 v5.3.5-no-partial-alloc
	go.oneofone.dev/gserv => ../gserv
)
