module go.oneofone.dev/mbbolt

go 1.19

require (
	go.etcd.io/bbolt v1.3.6
	go.oneofone.dev/genh v0.0.0-20221009230543-ee7571e1180e
	go.oneofone.dev/gserv v1.0.1-0.20221005034716-202f34ffc2cd
)

require golang.org/x/net v0.0.0-20221004154528-8021a29435af // indirect

require (
	github.com/gorilla/securecookie v1.1.1 // indirect
	github.com/vmihailenco/msgpack/v5 v5.3.5
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	go.oneofone.dev/oerrs v1.0.7-0.20221003171156-2726106fc553
	go.oneofone.dev/otk v1.0.7
	golang.org/x/crypto v0.0.0-20221010152910-d6f0a8c073c2 // indirect
	golang.org/x/image v0.0.0-20220902085622-e7cb96979f69 // indirect
	golang.org/x/sys v0.0.0-20221006211917-84dc82d7e875 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
)

replace github.com/vmihailenco/msgpack/v5 v5.3.5 => github.com/alpineiq/msgpack/v5 v5.3.5-no-partial-alloc
