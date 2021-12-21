# NJU-DisSys-2017
This is the resource repository for the course Distributed System, Fall 2017, CS@NJU.

In Assignment 2 and Assignment 3, you should primarily focus on /src/raft/...


## Notes
Edit PATH variable and project GOPATH:
```shell
export PATH=$PATH:/usr/local/go/bin
export GOPATH=/home/hxt/NJU-DisSys-2017
```

### Part1 test
```shell
cd src/raft
go test -run Election
```

### Part2 test
```shell
go test -run FailNoAgree
go test -run ConcurrentStarts
go test -run Rejoin
go test -run Backup
```

### Part3 test
```shell
go test -run Persist1
go test -run Persist2
go test -run Persist3
```


### Total test
```shell
go test -run Election
go test -run FailNoAgree
go test -run ConcurrentStarts
go test -run Rejoin
go test -run Backup
go test -run Persist1
go test -run Persist2
go test -run Persist3
#
```

## References
- sworduo/MIT6.824.git
- D0ub1ePieR/Distrubted-system-raft.git