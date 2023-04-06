import sys
import argparse
from dbsp import DBSPConnection


def execute(actions, name, code_file, make_config_fn, prepare_fn=None, verify_fn=None):
    sql_code = open(code_file, "r").read()
    dbsp = DBSPConnection()
    project = dbsp.create_or_replace_project(
        name=name, sql_code=sql_code)
    config = make_config_fn(project)

    if 'compile' in actions:
        project.compile()
        print("Project compiled")
        status = project.status()
        print("Project status: " + status)

    if prepare_fn and 'prepare' in actions:
        prepare_fn()

    pipeline = None
    try:
        if 'run' in actions:
            pipeline = config.run()
            print("Pipeline status: " + str(pipeline.status()))
            print("Pipeline metadata: " + str(pipeline.metadata()))
    finally:
        if pipeline:
            pipeline.delete()
            print("Pipeline removed")

    if verify_fn and 'verify' in actions:
        verify_fn()


def run_demo(name, code_file, make_config_fn, prepare_fn=None, verify_fn=None):
    parser = argparse.ArgumentParser(
        description='What do you want to do with the demo.')
    parser.add_argument('actions', nargs='*', default=['create'])
    args = parser.parse_args()

    # Add hard-coded dependencies
    actions = set(args.actions)
    if 'create' in actions:
        actions.add('compile')
    if 'run' in actions:
        actions.add('create')
        actions.add('compile')
    if 'verify' in actions:
        actions.add('compile')
        actions.add('create')
        actions.add('run')

    execute(actions, name, code_file,
            make_config_fn, prepare_fn, verify_fn)
