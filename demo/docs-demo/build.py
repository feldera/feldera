#! /usr/bin/python3
import glob
import imagesize
import os
import sys
import time
import argparse
import subprocess

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "project_demo00-SecOps"))


def list_pngs(dir):
    pngs = set()
    try:
        names = os.listdir(dir)
    except:
        print(
            f"failed to read files from {dir} (perhaps you should specify --screenshot-dir?)"
        )
        raise

    for name in names:
        if name.endswith(".png"):
            pngs.add(name)
    return pngs


def screenshot():
    files1 = list_pngs(SCREENSHOT_DIR)

    while True:
        files2 = list_pngs(SCREENSHOT_DIR)
        new_files = files2 - files1
        if len(new_files) == 0:
            pass
        elif len(new_files) == 1:
            new_file = list(new_files)[0]
            return f"{SCREENSHOT_DIR}/{new_file}"
        else:
            print(f"too many new screenshots ({new_files}), try again")
            files1 = files2
        time.sleep(0.1)


def screenshot_as(name):
    os.replace(screenshot(), name)


def screenshot_sequence(stem):
    for name in os.listdir():
        if name.startswith(f"{stem}-") and name.endswith(".png"):
            os.remove(name)
    names = []
    try:
        while True:
            n = len(names)
            name = f"{stem}-{n:02}.png"
            screenshot_as(name)
            names += [name]
    except KeyboardInterrupt:
        pass
    print()
    return names


def main():
    # Command-line arguments
    parser = argparse.ArgumentParser(description="Builds demos for the website")
    default_screenshot_dir = "{}/Pictures/Screenshots".format(os.environ["HOME"])
    default_steps = "calibrate,pipeline,program,connectors"
    parser.add_argument(
        "--screenshot-dir",
        default=default_screenshot_dir,
        help=f"directory where screenshots appear (defaults to {default_screenshot_dir}",
    )
    parser.add_argument(
        "--steps",
        default=default_steps,
        help=f"steps to run (default: {default_steps})",
    )

    args = parser.parse_args()
    global SCREENSHOT_DIR
    SCREENSHOT_DIR = parser.parse_args().screenshot_dir
    steps = parser.parse_args().steps.split(",")

    if "calibrate" in steps:
        calibrate()

    if "program" in steps:
        record_program()
        assemble_program()

    if "connectors" in steps:
        record_connectors()
        assemble_connectors()

    if "pipeline" in steps:
        record_pipeline()
        assemble_pipeline()


def calibrate():
    print(f"""\
We're going to start by getting the web browser in the right size and
screenshots set up to take just the right area.

1. Open the web console in the web browser, and size it so the content
   area (that is, the web page itself ignoring the web browser's frame
   and tool bars, etc.) is 1200x1200.  If you need a guide for sizing
   it, open 1200x1200.png in an image viewer, e.g. with xdg-open, and
   move the windows above and below and alongside each other as
   guides.

2. Take a screenshot of the content area only.  Assuming that you're
   using GNOME, hit PrtScr on the keyboard, click on "Selection" in the
   dialog that pops up, make sure that "Show Pointer" is selected
   (it is not selected by default).  Then, carefully select just the
   content area that you sized the window to, and then hit Enter to take
   the screenshot.

   When you do this, I'll print the size of the screenshot
   automatically, based on the screenshot appearing in this directory:
       {SCREENSHOT_DIR}
   If that doesn't happen, it's probably not the right directory, so
   you should start over with --screenshot-dir pointed to the right
   one.

3. If the size is within a few pixels of 1200x1200, hit Control+C to
   finish up.  Otherwise, adjust the size of the web browser or the
   size of the screenshot and try again until it's close enough.""")
    try:
        while True:
            width, height = imagesize.get(screenshot())
            print(f"Screenshot is {width}x{height} (ideal is 1200x1200).")
    except KeyboardInterrupt:
        pass


def record_pipeline():
    print("""\
Let's record running the pipeline:

- Click on the Feldera logo.
- Click on secops.
- Hover over the SQL program and screenshot.
""")
    screenshot_as("pipeline-home.png")

    print("""\
- Hover over the Play icon ▶️ and screenshot.
""")
    screenshot_as("pipeline-start.png")

    print("""\
- Click on the start icon.
- Wait for "starting up" to appear.
- Hover over the Play icon ▶️ and take a screenshot.
""")
    screenshot_as("pipeline-starting.png")

    print("""\
- Wait for "running" to appear.
- Hover over the pause icon ⏸️ and take a screenshot.
""")
    screenshot_as("pipeline-running.png")

    print("""\
Every 5 seconds or so, as the throughput graph updates:
  - Move the cursor aside and take a screenshot.
When the graph fills the entire width, hit Control+C.
""")
    screenshot_sequence("pipeline-throughput")

    print("""\
- Hover over the pause button and screenshot.
""")
    screenshot_as("pipeline-pause.png")

    print("""\
- Click the pause button.
- Wait for the status to change to "paused".
- Hover over the "play" button and screenshot.
""")
    screenshot_as("pipeline-pausing.png")

    print("""\
- Hover over the stop button and screenshot.
""")
    screenshot_as("pipeline-stop.png")

    print("""\
- Click on the stop button.
- Wait for the status to change to "shutting down".
- Hover where the stop button was and screenshot.
""")
    screenshot_as("pipeline-stopping.png")

    print("""\
- Wait for trash can icon to appear.
- Hover over the trash can and screenshot.
""")
    screenshot_as("pipeline-stopped.png")


def assemble_pipeline():
    print("assembling pipeline.gif...")
    throughput = sorted(glob.glob("pipeline-throughput-[0-9][0-9].png"))
    subprocess.run(
        [
            "magick",
            "-size",
            "1200x1200",
            "-delay",
            "200",
            "pipeline-home.png",
            "-delay",
            "200",
            "pipeline-start.png",
            "-delay",
            "100",
            "pipeline-running.png",
            "-delay",
            "75",
        ]
        + throughput
        + [
            "-delay",
            "200",
            "pipeline-pause.png",
            "-delay",
            "150",
            "pipeline-pausing.png",
            "-delay",
            "200",
            "pipeline-stop.png",
            "-delay",
            "200",
            "pipeline-stopping.png",
            "-delay",
            "200",
            "pipeline-stopped.png",
            "pipeline.gif",
        ]
    )


def record_program():
    print("""\
----------------------------------------------------------------------
Let's record scrolling through the SQL program.

- Click on the Feldera logo and screenshot.
""")
    screenshot_as("program-home.png")

    print("""\
- Hover over secops and screenshot.
""")
    screenshot_as("program-secops.png")

    print("""\
- Click on secops and screenshot.
""")
    screenshot_as("program-secops2.png")

    print("""\
- Scroll through the program and click on the down-arrow next to
  each "WITH", to collapse them.
- Then, scroll back up to the top.
- Then, hover over the SQL program and screenshot.
""")
    screenshot_as("program-editor.png")

    print("""\
Then, repeatedly:
- Roll the scroll wheel three or four times to scroll the program downward.
- Hover over the SQL program and take a screenshot.
...until you get to the end of the program, then hit Control+C.
""")
    screenshot_sequence("program-scroll")


def assemble_program():
    names = sorted(glob.glob("program-scroll-*.png"))
    print("assembling program.gif...")
    subprocess.run(
        [
            "magick",
            "-size",
            "1200x1200",
            "-delay",
            "200",
            "program-home.png",
            "-delay",
            "100",
            "program-secops.png",
            "-delay",
            "100",
            "program-secops2.png",
            "program-editor.png",
            "-delay",
            "200",
            "-delay",
            "40",
        ]
        + names[:-1]
        + ["-delay", "100", names[-1], "program.gif"]
    )


def record_connectors():
    print("""\
Let's record looking at the pipeline's connectors.

- Click on the Feldera logo.
- Click on secops.
- Scroll so the pipeline table and its WITH clause are visible,
  and screenshot.
""")
    screenshot_as("connectors-pipeline.png")

    print("""\
Then, repeatedly:
- Roll the scroll wheel three or four times to scroll the program downward.
- Hover over the SQL program and take a screenshot.
...until you get to k8sobject (the last table with a connector),
   then hit Control+C.
""")
    screenshot_sequence("connectors-scroll")


def assemble_connectors():
    print("assembling connectors.gif...")
    names = sorted(glob.glob("connectors-scroll-*.png"))
    subprocess.run(
        [
            "magick",
            "-size",
            "1200x1200",
            "-delay",
            "200",
            "connectors-pipeline.png",
            "-delay",
            "60",
        ]
        + names[:-1]
        + ["-delay", "200", names[-1], "connectors.gif"]
    )


if __name__ == "__main__":
    main()
