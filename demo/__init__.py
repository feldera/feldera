import sys
import argparse
from dbsp import DBSPConnection


def execute(
    dbsp_url,
    actions,
    name,
    code_file,
    make_pipeline_fn,
    prepare_fn=None,
    verify_fn=None,
):
    dbsp = DBSPConnection(dbsp_url)
    sql_code = open(code_file, "r").read()
    program = dbsp.create_or_replace_program(name=name, sql_code=sql_code)
    pipeline = make_pipeline_fn(program)

    if "compile" in actions:
        program.compile()
        print("Project compiled")
        status = program.status()
        print("Project status: " + status)

    if prepare_fn and "prepare" in actions:
        print("Preparing...")
        prepare_fn()

    if "run" in actions:
        # Make sure we run the pipeline from a clean slate.
        pipeline.shutdown()
        pipeline.run()
        print("Pipeline started")

    if verify_fn and "verify" in actions:
        verify_fn(dbsp_url, pipeline)


def run_demo(name, code_file, make_pipeline_fn, prepare_fn=None, verify_fn=None):
    parser = argparse.ArgumentParser(
        description="What do you want to do with the demo."
    )
    parser.add_argument("--dbsp_url", required=True)
    parser.add_argument("--actions", nargs="*", default=["create"])
    parser.add_argument("--prepare-args", nargs="*", default=None)
    args = parser.parse_args()

    # Add hard-coded dependencies
    dbsp_url = args.dbsp_url
    actions = set(args.actions)
    if "create" in actions:
        actions.add("compile")
    if "run" in actions:
        actions.add("create")
        actions.add("compile")
    if "verify" in actions:
        actions.add("compile")
        actions.add("create")
        actions.add("run")
    prepare = (
        prepare_fn
        if args.prepare_args == None
        else lambda: prepare_fn(args.prepare_args)
    )
    execute(dbsp_url, actions, name, code_file, make_pipeline_fn, prepare, verify_fn)
