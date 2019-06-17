minidote
=====

A small replicated CRDT store.

Build
-----

This project can be built using [rebar3](https://www.rebar3.org/).
The most important tasks are defined in a Makefile: 

    # compile the program
    make compile
    # start the program with an interactive shell
    make shell
    # Run unit and system tests
    make test
    # Run only a single test suite (e.g. pb_client_SUITE)
    make test SUITE=pb_client_SUITE
    # Run type-checks
    make dialyzer
    # Build a release
    make rel

### Setup a test cluster

Note: These tasks use the Bash Syntax for environment variables.
Therefore, on Windows you need to execute the commands manually and set the environment variables using the `set` command.

To setup a test-cluster with 3 nodes, run the following 3 commands in 3 terminals:

    make run-cluster-node1
    make run-cluster-node2
    make run-cluster-node3 


### Docker

To build a docker-image named `minidote` run the following command:

    make docker
    
To run a single Minidote docker container run:
    
    make docker-run

To run a cluster with 3 Minidote instances run:

    make docker-run-cluster

When running a cluster, you can open terminal on a single node (e.g. minidote1) for debugging by running:

    docker-compose exec minidote1 bash