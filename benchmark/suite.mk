#! /usr/bin/make -f

## ------------- ##
## Nexmark suite ##
## ------------- ##

# Run this Makefile to produce a full matrix of:
#
#   - runners (Feldera, Flink, Beam)
#   - modes (batch, stream)
#   - languages for Beam (default, SQL, ZetaSQL)
#
# You can set variables on the command line to control what it does, e.g.
# limit runners or modes or set the number of events:
#
#   make -f suite.mk runners='feldera flink' modes=batch events=1M

# Fill these in, or pass on the command line, to enable dataflow tests.
project =
bucket =

runners = feldera flink beam.flink beam.spark beam.dataflow
modes = batch stream
languages = default sql zetasql

events = 100M
cores = 16

# Compose the Cartesian product of runners × modes × languages, but
# exclude:
#
#   - Spark streaming (which hangs)
#   - Feldera batching (Feldera only does streaming)
#   - languages other than the default except for Beam and Feldera
#   - Dataflow (unless project and bucket are configured)
targets = \
$(filter-out beam.spark-stream-% feldera-batch-% \
	     $(if $(project)$(bucket),,beam.dataflow-%),\
$(foreach runner,$(runners),\
$(foreach language,$(if $(filter beam.%,$(runner)),$(languages), \
                     $(if (filter feldera,$(runner)),default sql,default)),\
$(foreach mode,$(modes),\
$(runner)-$(mode)-$(language)-$(events).csv))))

all: $(targets)
.PHONY: all

%.log:
	@IFS=-; target=$(@:.log=); set -- $$target; \
	runner=$$1 mode=$$2 language=$$3 events=$$4; \
	./run-nexmark.sh --runner=$$runner --language=$$language --mode=$$mode \
		--events=$$events --project="$(project)" --bucket="$(bucket)" \
		--cores=$(cores) \
		| tee $@

%.csv: %.log
	@echo "Generate $@ from $<"
	@IFS=-; target=$(@:.csv=); set -- $$target; \
	runner=$$1 mode=$$2 language=$$3 events=$$4; \
	./run-nexmark.sh --runner=$$runner --language=$$language --mode=$$mode \
		--events=$$events --cores=$(cores) --parse < $< > $@

.SECONDARY:
