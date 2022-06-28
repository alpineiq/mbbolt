module go.oneofone.dev/mbbolt

go 1.18

require (
	go.etcd.io/bbolt v1.3.6
	go.oneofone.dev/genh v0.0.0-20220626190728-46f771ae1ea4
	go.oneofone.dev/gserv v0.0.0-20220622235746-e6c29b42aeb4
)

require golang.org/x/net v0.0.0-20220624214902-1bab6f366d9e // indirect

require (
	github.com/google/uuid v1.3.0
	github.com/gorilla/securecookie v1.1.1 // indirect
	github.com/vmihailenco/msgpack/v5 v5.3.5 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	go.oneofone.dev/oerrs v1.0.6
	go.oneofone.dev/otk v1.0.7-0.20220615225005-5a3ffebd985f // indirect
	golang.org/x/crypto v0.0.0-20220622213112-05595931fe9d // indirect
	golang.org/x/image v0.0.0-20220617043117-41969df76e82 // indirect
	golang.org/x/sys v0.0.0-20220610221304-9f5ed59c137d // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/xerrors v0.0.0-20220609144429-65e65417b02f // indirect
)

replace (
	github.com/vmihailenco/msgpack/v5 v5.3.5 => github.com/alpineiq/msgpack/v5 v5.3.5-no-partial-alloc
	go.oneofone.dev/gserv => ../gserv
)
