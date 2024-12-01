package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id int
	OpCount int
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id int
	OpCount int
}

type GetReply struct {
	Value string
}

type LogValue struct {
	count int
	Value string
	// timestamps   time.Time
}