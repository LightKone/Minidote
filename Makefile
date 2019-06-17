REBAR = rebar3

DEPS_PATH = $(shell rebar3 as test path)


CT_OPTS_DIST = -erl_args

ifdef SUITE
CT_OPTS_SUITE = -suite test/${SUITE}
else
CT_OPTS_SUITE =
endif

all: compile

compile:
	$(REBAR) compile

clean:
	rm -rf ebin/* test/*.beam logs log
	$(REBAR) clean

distclean: clean
	rm -rf _build priv/*.so logs log

dialyzer:
	$(REBAR) dialyzer

shell:
	$(REBAR) shell

compile_tests:
	$(REBAR) as test compile


ifeq ($(OS),Windows_NT) 
#windows
test: compile_tests
	-mkdir logs
	ct_run -dir test -pa $(DEPS_PATH) -logdir logs ${CT_OPTS_SUITE} $(CT_OPTS_DIST)
else 
#normal operating system
test: compile_tests
	mkdir -p logs
	ct_run -dir test -pa $(DEPS_PATH) -logdir logs ${CT_OPTS_SUITE} $(CT_OPTS_DIST) || (rm -rf test/*.beam && false)
	rm -rf test/*.beam
endif

rel:
	rebar3 release

docker:
	docker build . -t minidote

docker-run:
	docker run --rm -it -p 8087:8087 minidote

docker-run-cluster:
	docker-compose up


run-cluster-node1:
	MINIDOTE_NODES=minidote2@127.0.0.1,minidote3@127.0.0.1 LOG_DIR=./data/m1/ OP_LOG_DIR=./data/m1/op_log/ MINIDOTE_PORT=8087 rebar3 shell --name minidote1@127.0.0.1
run-cluster-node2:
	MINIDOTE_NODES=minidote1@127.0.0.1,minidote3@127.0.0.1 LOG_DIR=./data/m2/ OP_LOG_DIR=./data/m2/op_log/ MINIDOTE_PORT=8088 rebar3 shell --name minidote2@127.0.0.1
run-cluster-node3:
	MINIDOTE_NODES=minidote1@127.0.0.1,minidote2@127.0.0.1 LOG_DIR=./data/m3/ OP_LOG_DIR=./data/m3/op_log/ MINIDOTE_PORT=8089 rebar3 shell --name minidote3@127.0.0.1
