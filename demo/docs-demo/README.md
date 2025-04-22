# Demo for generating documentation

This demo is not meant for end users to run directly.  Instead, its
goal is to make it relatively easy to regenerate the GIFs in the
documentation "Concepts" section that quickly show how Feldera works.

To use the demo to regenerate the GIFs:

1. Install prerequisites:

   * The Python "imagesize" library.  In Fedora, you can use `dnf
     install python3-imagesize`, otherwise you can use `pip`.

   * The ImageMagick command-line tools.  In Fedora, you can use `dnf
     install ImageMagick`.

2. Start the pipeline manager locally, e.g.:

   ```
   cargo run -p pipeline-manager
   ```

3. Create a pipeline named `secops` and paste in the secops SQL code
   from the `try.feldera.com` sandbox.  It should use the datagen
   connector.

4. In this directory, run the demo, e.g.:

   ```
   ./build.py
   ```

   This demo assumes that you have an easy-to-use manual screenshot
   utility that adds a `.png` file to a directory.  That's true of
   recent versions of GNOME, where you just push the PrtScr key and it
   pops up and, if you've set it up the way you need, you just push
   Enter after that.  GNOME adds the screenshot to
   `~/Pictures/Screenshots` by default.  You can supply
   `--screenshot-dir` above if that's not the right directory.

   The demo by default tries to get you to, first, set up your web
   browser so that the screenshots will be the right size and, second,
   configure the screenshots properly.  Then it will run you through
   updating each of the GIFs.  If you want to only do some of those
   steps, supply `--steps`.

   For a little bit of help, use `--help`.

   Output is new `.gif` files in this directory.  To use them in the
   website, copy them into `../../docs`.
