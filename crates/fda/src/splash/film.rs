//! The picture of a frame. This file has the logo, the sample points of the
//! bird, and the composition of one frame into rows of braille characters.
//!
//! | Unit | Meaning |
//! |---|---|
//! | px | A sample of the logo grid, which is 2000 by 875 |
//! | cell | A character cell of the terminal |
//! | dot | A dot of a braille character. A cell is 2 dots wide and 4 dots high. |

use std::borrow::Cow;
use std::f64::consts::PI;

use super::color::{CLAY, ColorMode, LICHEN, Rgb, STEEL, STONE, VIOLET};
use super::{Pt, smooth};

/// The logo as a run-length encoded grid of labels, with one label per px.
/// `scripts/make_splash_logo.sh` makes this file.
const LOGO: &[u8] = include_bytes!("splash_logo.bin");
/// The moving bird has one sample point per `SAMPLE_STEP` px in each
/// direction. Fewer points make each frame faster to draw.
const SAMPLE_STEP: usize = 3;
/// The size of the logo grid, in px.
const SRC_W: f64 = 2000.0;
const SRC_H: f64 = 875.0;
// The labels of the logo grid. `BODY` includes the near wing.
const EMPTY_LABEL: u8 = 0;
const BODY: u8 = 1;
const FAR_WING: u8 = 2;
const WORD: u8 = 3;

/// The bit of each dot of a braille character, by [row][column] of the cell.
/// A braille character is U+2800 plus the bits of its dots.
const BITS: [[u8; 2]; 4] = [[0x01, 0x08], [0x02, 0x10], [0x04, 0x20], [0x40, 0x80]];

/// The color of a spark by its age. Each entry is (age limit in frames, color).
/// A spark that reaches the last age limit is removed.
pub(super) const SPARK_COLORS: [(f64, Rgb); 5] = [
    (6.0, VIOLET),
    (14.0, STEEL),
    (22.0, LICHEN),
    (30.0, CLAY),
    (38.0, STONE),
];
/// The color of a streak by its age, in the same form as `SPARK_COLORS`.
pub(super) const STREAK_COLORS: [(f64, Rgb); 3] = [(4.0, VIOLET), (9.0, STEEL), (14.0, STONE)];

// The geometry of the bird, in SVG units of the bird mark. `svg_px` converts a point to px.
/// The bounding box of the bird in the logo grid, as [left, top, right, bottom] in px.
const BIRD_PX: [f64; 4] = [1256.0, 3.0, 1953.0, 483.0];
/// The SVG point at the top left corner of the bounding box, and the number
/// of px per SVG unit.
const SVG_ORIGIN: Pt = [2.8, 0.8];
const SVG_SCALE: f64 = 1.2556;
/// The two end points of the hinge of each wing. A wing folds about its hinge.
const NEAR_HINGE: [Pt; 2] = [[268.0, 240.0], [377.0, 204.0]];
const FAR_HINGE: [Pt; 2] = [[414.3, 180.8], [539.9, 200.2]];
/// The point where the feet touch the wordmark. The body tilts about this
/// point in the landing.
pub(super) const FEET: Pt = [360.0, 300.0];
/// The point where the tail joins the body. The tail bends about this point.
const TAIL_ROOT: Pt = [235.0, 285.0];
/// The x of the tip of the tail. Dust starts from the body points left of
/// `TAIL_DUST_X`.
const TAIL_TIP_X: f64 = 3.0;
const TAIL_DUST_X: f64 = 70.0;

/// Converts a point from SVG units of the bird mark to px of the logo grid.
pub(super) fn svg_px(p: Pt) -> Pt {
    [
        BIRD_PX[0] + (p[0] - SVG_ORIGIN[0]) * SVG_SCALE,
        BIRD_PX[1] + (p[1] - SVG_ORIGIN[1]) * SVG_SCALE,
    ]
}

/// Returns `p` rotated by `angle` radians about `pivot`. A positive angle
/// rotates clockwise on the screen, because y grows downward.
fn turn(p: Pt, pivot: Pt, angle: f64) -> Pt {
    let (s, c) = angle.sin_cos();
    let (dx, dy) = (p[0] - pivot[0], p[1] - pivot[1]);
    [pivot[0] + dx * c - dy * s, pivot[1] + dx * s + dy * c]
}

/// The hinge of a wing as a coordinate frame, in px of the logo grid.
struct Hinge {
    /// The first end point of the hinge.
    origin: Pt,
    /// The unit vector along the hinge.
    along: Pt,
    /// The unit vector perpendicular to the hinge. It points up, away from the
    /// body.
    out: Pt,
}

impl Hinge {
    fn new(ends: [Pt; 2]) -> Self {
        let (a, b) = (svg_px(ends[0]), svg_px(ends[1]));
        let len = ((b[0] - a[0]).powi(2) + (b[1] - a[1]).powi(2)).sqrt();
        let along = [(b[0] - a[0]) / len, (b[1] - a[1]) / len];
        Hinge {
            origin: a,
            along,
            out: [along[1], -along[0]],
        }
    }

    /// Returns `p` in hinge coordinates, as (distance along the hinge, distance
    /// out from the hinge).
    fn coords(&self, p: Pt) -> (f64, f64) {
        let r = [p[0] - self.origin[0], p[1] - self.origin[1]];
        (
            r[0] * self.along[0] + r[1] * self.along[1],
            r[0] * self.out[0] + r[1] * self.out[1],
        )
    }

    /// Returns the point at the distance `s` along the hinge and `d` out from
    /// the hinge.
    fn point(&self, s: f64, d: f64) -> Pt {
        [
            self.origin[0] + s * self.along[0] + d * self.out[0],
            self.origin[1] + s * self.along[1] + d * self.out[1],
        ]
    }
}

/// One wing, as sample points in hinge coordinates. The wing position k scales the distance
/// of each point from the hinge. At 1 the wing is in the logo pose, at 0 it is flat on the
/// hinge, and below 0 it is folded below the hinge.
pub(super) struct Wing {
    hinge: Hinge,
    /// For each point, the distance along the hinge and the distance out from
    /// the hinge.
    s: Vec<f64>,
    d: Vec<f64>,
    /// For each point, `d` as a fraction of `dmax`. The fraction is 0 at the
    /// hinge and 1 at the tip.
    u: Vec<f64>,
    /// The distance of the tip out from the hinge, which is the largest `d`.
    dmax: f64,
    /// The lowest wing position k of a wingbeat in flight.
    kmin: f64,
    /// The phase, in radians, by which the tip is behind the hinge in a
    /// wingbeat in flight.
    lag: f64,
    /// The distance that the tip moves back along the hinge at the lowest wing
    /// position, as a fraction of `dmax`.
    sweep: f64,
}

impl Wing {
    fn new(pts: &[Pt], hinge: [Pt; 2], kmin: f64, lag: f64, sweep: f64) -> Self {
        let hinge = Hinge::new(hinge);
        let (s, d): (Vec<f64>, Vec<f64>) = pts.iter().map(|p| hinge.coords(*p)).unzip();
        let dmax = d.iter().cloned().fold(f64::MIN, f64::max);
        let u = d
            .iter()
            .map(|d| (d / dmax.max(1.0)).clamp(0.0, 1.0))
            .collect();
        Wing {
            hinge,
            s,
            d,
            u,
            dmax,
            kmin,
            lag,
            sweep,
        }
    }

    /// Writes to `out` the wing points at the phase `phase` of a wingbeat in
    /// flight. `amp` is the width of the wingbeat, from 0 to 1. Phase 0 is the top
    /// of the wingbeat, where the hinge side of the wing is in the logo pose. The
    /// tip is behind the hinge in phase, so the wing curls.
    pub(super) fn pose(&self, phase: f64, amp: f64, out: &mut Vec<Pt>) {
        let k_of_u = |u: f64| {
            let wave = 0.5 + 0.5 * (phase - self.lag * u).cos();
            let k_raw = self.kmin + (1.0 - self.kmin) * wave;
            1.0 - amp * (1.0 - k_raw)
        };
        self.pose_with(self.kmin, k_of_u, out);
    }

    /// Writes to `out` the wing points, each at the wing position that `k_of_u` returns for
    /// its `u`. `k_low` is the lowest wing position of the wingbeat, where the tip is back by
    /// the full `sweep`.
    pub(super) fn pose_with(&self, k_low: f64, k_of_u: impl Fn(f64) -> f64, out: &mut Vec<Pt>) {
        out.clear();
        for i in 0..self.s.len() {
            let k = k_of_u(self.u[i]);
            let s = self.s[i] - self.sweep * self.dmax * self.u[i] * (1.0 - k) / (1.0 - k_low);
            out.push(self.hinge.point(s, self.d[i] * k));
        }
    }
}

/// A cubic Bezier curve with sample points indexed by arc length. With `at`,
/// the caller controls the speed of the bird along the curve.
pub(super) struct Path {
    /// The sample points of the curve.
    xy: Vec<Pt>,
    /// For each sample point, the arc length from the start of the curve, as a
    /// fraction of the total length.
    s: Vec<f64>,
}

impl Path {
    /// Samples the curve with the control points `q`.
    fn new(q: [Pt; 4]) -> Self {
        let xy: Vec<Pt> = (0..600)
            .map(|i| {
                let u = i as f64 / 599.0;
                let a = 1.0 - u;
                let w = [a * a * a, 3.0 * a * a * u, 3.0 * a * u * u, u * u * u];
                [0, 1].map(|k| (0..4).map(|j| w[j] * q[j][k]).sum())
            })
            .collect();
        let mut s = vec![0.0];
        for w in xy.windows(2) {
            let seg = ((w[1][0] - w[0][0]).powi(2) + (w[1][1] - w[0][1]).powi(2)).sqrt();
            s.push(s.last().unwrap() + seg);
        }
        let total = *s.last().unwrap();
        s.iter_mut().for_each(|v| *v /= total);
        Path { xy, s }
    }

    /// Returns the point at the fraction `frac` of the arc length of the curve.
    pub(super) fn at(&self, frac: f64) -> Pt {
        let i = self
            .s
            .partition_point(|&v| v < frac)
            .clamp(1, self.s.len() - 1);
        let (s0, s1) = (self.s[i - 1], self.s[i]);
        let w = if s1 > s0 {
            (frac - s0) / (s1 - s0)
        } else {
            0.0
        };
        [0, 1].map(|k| self.xy[i - 1][k] + (self.xy[i][k] - self.xy[i - 1][k]) * w)
    }
}

/// A spark, a dust particle or a streak. A streak is a speed line of the approach. Positions
/// are in dots and ages are in frames.
pub(super) struct Particle {
    pub(super) pos: Pt,
    pub(super) vel: Pt,
    /// The position one frame before. A streak is drawn from `prev` to `pos`.
    pub(super) prev: Pt,
    pub(super) age: f64,
}

/// The part of the wordmark that a frame shows.
#[derive(Clone, Copy)]
pub(super) enum Word {
    Hidden,
    /// The progress of the Violet band across the wordmark. The band starts at
    /// 0 and is past the last letter at 1.
    Sweep(f64),
    Full,
}

/// The content of one frame, as the input of `Film::compose`.
pub(super) struct Shot<'a> {
    /// The dots of the body with the near wing, and the dots of the far wing.
    pub(super) body: &'a [bool],
    pub(super) far: &'a [bool],
    /// The color of the body. `None` is the default text color of the terminal.
    pub(super) bird_color: Option<Rgb>,
    /// True if the bird is in the landed logo. The far wing then has the
    /// default text color. In flight the far wing is Steel.
    pub(super) landed: bool,
    pub(super) word: Word,
    /// The number of registration marks that show, from 0 to 4.
    pub(super) marks: usize,
    /// True if the frame shows the "press any key to skip" hint.
    pub(super) hint: bool,
}

/// The data of the animation that does not change from frame to frame.
pub(super) struct Film {
    /// The size of a frame, in cells.
    w: usize,
    pub(super) h: usize,
    /// The size of a frame, in dots.
    pub(super) wd: usize,
    pub(super) hd: usize,
    /// The width of the landed logo, in dots.
    logo_w: usize,
    /// The x of the left edge of the landed logo, in dots.
    logo_x: usize,
    /// The number of dots per px of the logo grid.
    pub(super) scale: f64,
    /// The dots of the landed logo, for the body with the near wing, for the
    /// far wing and for the wordmark.
    pub(super) final_bird: Vec<bool>,
    pub(super) final_far: Vec<bool>,
    final_word: Vec<bool>,
    /// The sample points of the body that do not move with a wing, in px of the
    /// logo grid.
    pub(super) statics: Vec<Pt>,
    pub(super) near: Wing,
    pub(super) far: Wing,
    /// The indexes of the points near the tip of the near wing. Sparks start
    /// from these points.
    pub(super) tips: Vec<usize>,
    /// The points at the end of the tail. Dust starts from these points.
    pub(super) tail: Vec<Pt>,
    /// For each point of `statics`, the fraction of the tail motion that the point follows,
    /// from 0 at the tail root to 1 at the tail tip. The fraction is 0 for the rest of the body.
    tail_w: Vec<f64>,
    /// The center of the bounding box of the bird, in px of the logo grid.
    /// `project` scales and rotates the bird about this point.
    pub(super) anchor: Pt,
    /// The position of `anchor` in the landed logo, in dots. The comments call
    /// this position the perch.
    pub(super) perch: Pt,
    /// The positions of the four registration marks, as (column, row) in cells.
    marks: [(usize, usize); 4],
    /// The vanishing point of the approach, in dots. The bird and the streaks
    /// start from this point.
    pub(super) vp: Pt,
    /// The flight path of the side-on pass, in dots.
    pub(super) path: Path,
    color: ColorMode,
    /// The color of secondary text for the theme of the terminal.
    pub(super) muted: Rgb,
}

/// Decodes `LOGO` to (width, height, labels), with one label per px in row order. The format
/// is a u16 width, a u16 height, then runs of (label u8, length u16), all little endian.
fn decode_logo() -> (usize, usize, Vec<u8>) {
    let w = u16::from_le_bytes([LOGO[0], LOGO[1]]) as usize;
    let h = u16::from_le_bytes([LOGO[2], LOGO[3]]) as usize;
    let mut labels = Vec::with_capacity(w * h);
    for run in LOGO[4..].as_chunks::<3>().0 {
        let len = u16::from_le_bytes([run[1], run[2]]) as usize;
        labels.extend(std::iter::repeat_n(run[0], len));
    }
    (w, h, labels)
}

/// Returns the Lanczos-3 weights to resample `src` samples to `dst` samples along one axis.
/// `span` is the length of the source that maps to the `dst` samples. Each item of the result
/// is (the index of the first source sample, the weights from that sample on).
fn lanczos_weights(src: usize, dst: usize, span: f64) -> Vec<(usize, Vec<f64>)> {
    let sinc = |x: f64| {
        if x == 0.0 {
            1.0
        } else {
            (PI * x).sin() / (PI * x)
        }
    };
    let scale = span / dst as f64;
    let support = 3.0 * scale.max(1.0);
    (0..dst)
        .map(|i| {
            let center = (i as f64 + 0.5) * scale;
            let lo = ((center - support + 0.5).floor().max(0.0)) as usize;
            let hi = ((center + support + 0.5).ceil() as usize).min(src);
            let mut w: Vec<f64> = (lo..hi)
                .map(|j| {
                    let x = (j as f64 - center + 0.5) / scale.max(1.0);
                    if x.abs() < 3.0 {
                        sinc(x) * sinc(x / 3.0)
                    } else {
                        0.0
                    }
                })
                .collect();
            let sum: f64 = w.iter().sum();
            w.iter_mut().for_each(|v| *v /= sum);
            (lo, w)
        })
        .collect()
}

/// Resamples each label of the `w` by `h` grid `labels` to `dw` by `dh` with a Lanczos-3
/// filter. Returns one coverage grid per label, where 1 means that the label fills the sample.
///
/// Most of the logo grid is empty, so the horizontal pass visits only the px that have a label.
fn lanczos_labels(labels: &[u8], w: usize, h: usize, dw: usize, dh: usize) -> [Vec<f64>; 4] {
    // For each source column, the output columns that it contributes to, each
    // with its weight.
    let mut feeds: Vec<Vec<(usize, f64)>> = vec![Vec::new(); w];
    for (i, (lo, ws)) in lanczos_weights(w, dw, SRC_W).into_iter().enumerate() {
        for (k, v) in ws.into_iter().enumerate() {
            feeds[lo + k].push((i, v));
        }
    }
    let mut rows: [Vec<f64>; 4] = std::array::from_fn(|_| vec![0.0; h * dw]);
    for (idx, &label) in labels.iter().enumerate() {
        if label != EMPTY_LABEL {
            let (y, x) = (idx / w, idx % w);
            let row = &mut rows[label as usize][y * dw..(y + 1) * dw];
            for &(i, v) in &feeds[x] {
                row[i] += v;
            }
        }
    }
    let wy = lanczos_weights(h, dh, SRC_H);
    let mut out: [Vec<f64>; 4] = std::array::from_fn(|_| vec![0.0; dh * dw]);
    for label in [BODY, FAR_WING, WORD] {
        let (rows, out) = (&rows[label as usize], &mut out[label as usize]);
        for (y, (lo, ws)) in wy.iter().enumerate() {
            for (k, v) in ws.iter().enumerate() {
                let src = &rows[(lo + k) * dw..(lo + k + 1) * dw];
                for (o, r) in out[y * dw..(y + 1) * dw].iter_mut().zip(src) {
                    *o += v * r;
                }
            }
        }
    }
    out
}

/// The number of dot rows above the landed logo. The bird flies in this
/// space.
const FILM_TOP_DOTS: usize = 16;

impl Film {
    /// Returns the size of the landed logo for a frame of `cols` cells, as
    /// (width in cells, width in dots, height in dots).
    fn logo_size(cols: usize) -> (usize, usize, usize) {
        // The logo leaves 10 cells free for the registration marks. It is at
        // most 64 cells wide.
        let cells = (cols - 10).min(64);
        let logo_w = cells * 2;
        let logo_h = (logo_w as f64 * SRC_H / SRC_W).round() as usize;
        (cells, logo_w, logo_h)
    }

    /// Returns the number of rows of a frame of `cols` cells. A frame holds the space above the
    /// logo, the logo and 2 more rows. The last row shows the skip hint.
    pub(super) fn rows(cols: usize) -> usize {
        (FILM_TOP_DOTS + Film::logo_size(cols).2) / 4 + 2
    }

    pub(super) fn new(cols: usize, color: ColorMode) -> Self {
        let (aw, ah, labels) = decode_logo();
        let (w, h) = (cols, Film::rows(cols));
        let (wd, hd) = (w * 2, h * 4);
        let (logo_cells, logo_w, logo_h) = Film::logo_size(cols);
        let scale = logo_w as f64 / SRC_W;
        // The logo is centered, with its left edge on a cell boundary.
        let logo_x = (wd - logo_w) / 2 / 2 * 2;
        let logo_y = FILM_TOP_DOTS;

        // The landed logo. The Lanczos filter keeps thin strokes sharp. A dot is set if the label
        // covers more than half of it.
        let coverage = lanczos_labels(&labels, aw, ah, logo_w, logo_h);
        let place = |want: u8| {
            let small = &coverage[want as usize];
            let mut g = vec![false; hd * wd];
            for ty in 0..logo_h {
                for tx in 0..logo_w {
                    g[(logo_y + ty) * wd + logo_x + tx] = small[ty * logo_w + tx] > 0.5;
                }
            }
            g
        };
        let (final_bird, final_far, final_word) = (place(BODY), place(FAR_WING), place(WORD));

        // The sample points of the moving bird, in px of the logo grid. The points
        // of the body above the near hinge are the near wing.
        let near_hinge = Hinge::new(NEAR_HINGE);
        let (mut statics, mut near_pts, mut far_pts) = (Vec::new(), Vec::new(), Vec::new());
        for y in (0..ah).step_by(SAMPLE_STEP) {
            for x in (0..aw).step_by(SAMPLE_STEP) {
                let p = [x as f64, y as f64];
                match labels[y * aw + x] {
                    FAR_WING => far_pts.push(p),
                    BODY if near_hinge.coords(p).1 > 0.0 => near_pts.push(p),
                    BODY => statics.push(p),
                    _ => {}
                }
            }
        }
        // The near wing beats lower and sweeps back more than the far wing.
        let near = Wing::new(&near_pts, NEAR_HINGE, -0.55, 0.8, 0.18);
        let far = Wing::new(&far_pts, FAR_HINGE, -0.15, 0.7, 0.1);
        // The outer 15% of the near wing is its tip.
        let tips = (0..near.u.len()).filter(|&i| near.u[i] > 0.85).collect();
        let dust_x = svg_px([TAIL_DUST_X, 0.0])[0];
        let tail = statics.iter().filter(|p| p[0] < dust_x).cloned().collect();
        // The tail is the part of the body left of the tail root, from 40 px above
        // the root down.
        let (root, tip_x) = (svg_px(TAIL_ROOT), svg_px([TAIL_TIP_X, 0.0])[0]);
        let tail_w = statics
            .iter()
            .map(|p| {
                if p[0] < root[0] && p[1] > root[1] - 40.0 {
                    smooth(0.0, 1.0, (root[0] - p[0]) / (root[0] - tip_x))
                } else {
                    0.0
                }
            })
            .collect();
        let anchor = [
            (BIRD_PX[0] + BIRD_PX[2]) / 2.0,
            (BIRD_PX[1] + BIRD_PX[3]) / 2.0,
        ];
        let perch = [
            logo_x as f64 + anchor[0] * scale,
            logo_y as f64 + anchor[1] * scale,
        ];
        // The registration marks are 3 cells left of the logo and 2 cells right of it.
        let (mark_l, mark_r) = (logo_x / 2 - 3, logo_x / 2 + logo_cells + 2);
        let (wdf, hdf) = (wd as f64, hd as f64);
        Film {
            w,
            h,
            wd,
            hd,
            logo_w,
            logo_x,
            scale,
            final_bird,
            final_far,
            final_word,
            statics,
            near,
            far,
            tips,
            tail,
            tail_w,
            anchor,
            perch,
            marks: [(mark_l, 0), (mark_r, 0), (mark_r, h - 1), (mark_l, h - 1)],
            vp: [0.66 * wdf, 0.40 * hdf],
            // The flight path enters from the left, descends, climbs, and then
            // descends to the perch.
            path: Path::new([
                [-0.22 * wdf, 0.22 * hdf],
                [0.22 * wdf, 1.15 * hdf],
                [0.52 * wdf, -0.35 * hdf],
                perch,
            ]),
            color,
            muted: STEEL,
        }
    }

    /// Returns the position in dots of the point `p` of the bird. The bird is scaled by `sc`
    /// and rotated by `rot` radians about `anchor`, which is at `center`.
    pub(super) fn project(&self, p: Pt, rot: f64, sc: f64, center: Pt) -> Pt {
        let (c, s) = (rot.cos(), rot.sin());
        let (dx, dy) = (p[0] - self.anchor[0], p[1] - self.anchor[1]);
        let k = sc * self.scale;
        [
            (dx * c - dy * s) * k + center[0],
            (dx * s + dy * c) * k + center[1],
        ]
    }

    /// Returns the dots of the points `pts` after `project`. A dot is set if enough points map
    /// to it, which keeps the edges sharp at each scale.
    pub(super) fn raster<'a>(
        &self,
        pts: impl Iterator<Item = &'a Pt>,
        rot: f64,
        sc: f64,
        center: Pt,
    ) -> Vec<bool> {
        let mut counts = vec![0u16; self.hd * self.wd];
        for p in pts {
            let [x, y] = self.project(*p, rot, sc, center);
            if x >= 0.0 && y >= 0.0 && (x as usize) < self.wd && (y as usize) < self.hd {
                let i = y as usize * self.wd + x as usize;
                counts[i] = counts[i].saturating_add(1);
            }
        }
        // About (sc * scale * SAMPLE_STEP)^-2 points map to a dot inside the
        // bird. A dot needs 45% of that number.
        let need = (0.45 * (sc * self.scale * SAMPLE_STEP as f64).powi(-2)).max(1.0);
        counts.iter().map(|&c| c as f64 >= need).collect()
    }

    /// Returns the braille bits of each cell for the dot grid `grid`.
    fn cells(&self, grid: &[bool]) -> Vec<u8> {
        let mut out = vec![0u8; self.h * self.w];
        for cy in 0..self.h {
            for cx in 0..self.w {
                let mut bits = 0;
                for (dy, row) in BITS.iter().enumerate() {
                    for (dx, bit) in row.iter().enumerate() {
                        if grid[(cy * 4 + dy) * self.wd + cx * 2 + dx] {
                            bits |= bit;
                        }
                    }
                }
                out[cy * self.w + cx] = bits;
            }
        }
        out
    }

    /// Returns the braille bits of each cell for the (position, age) pairs `dots`, and the age
    /// of the youngest particle in each cell.
    fn age_cells(&self, dots: impl Iterator<Item = (Pt, f64)>) -> (Vec<u8>, Vec<f64>) {
        let mut age = vec![f64::INFINITY; self.hd * self.wd];
        for ([x, y], a) in dots {
            if x >= 0.0 && y >= 0.0 && (x as usize) < self.wd && (y as usize) < self.hd {
                let i = y as usize * self.wd + x as usize;
                age[i] = age[i].min(a);
            }
        }
        let bits = self.cells(&age.iter().map(|a| a.is_finite()).collect::<Vec<_>>());
        let mut cell_age = vec![f64::INFINITY; self.h * self.w];
        for (i, a) in age.iter().enumerate() {
            let c = (i / self.wd / 4) * self.w + (i % self.wd) / 2;
            cell_age[c] = cell_age[c].min(*a);
        }
        (bits, cell_age)
    }

    /// Composes one frame into rows with color escape sequences. The layers from front to
    /// back are the registration marks, the hint, the bird with the wordmark, the streaks and
    /// the sparks.
    pub(super) fn compose(
        &self,
        shot: &Shot,
        sparks: &[Particle],
        streaks: &[Particle],
    ) -> Vec<String> {
        let c = self.color;
        let reset = c.reset();
        let body = self.cells(shot.body);
        let far = self.cells(shot.far);

        // The wordmark. A Violet band moves from left to right. The letters left
        // of the band show in the text color.
        let (mut word, mut band) = (vec![0u8; self.h * self.w], vec![0u8; self.h * self.w]);
        match shot.word {
            Word::Full => word = self.cells(&self.final_word),
            Word::Sweep(b) if b > 0.0 => {
                // The factor 1.15 moves the 10-dot band past the last letter, so that the band
                // leaves the wordmark.
                let edge = self.logo_x as f64 + b * 1.15 * self.logo_w as f64;
                let visible = |lo: f64| -> Vec<bool> {
                    (0..self.hd * self.wd)
                        .map(|i| {
                            let x = (i % self.wd) as f64;
                            self.final_word[i] && x < edge && x > lo
                        })
                        .collect()
                };
                word = self.cells(&visible(f64::MIN));
                band = self.cells(&visible(edge - 10.0));
            }
            _ => {}
        }

        let (spark_bits, spark_age) = self.age_cells(sparks.iter().map(|p| (p.pos, p.age)));
        let (streak_bits, streak_age) = self.age_cells(streaks.iter().flat_map(|p| {
            // Draw each streak as the segment that it moved in the last frame, with
            // 10 dots along the segment.
            (0..10).map(move |k| {
                let w = k as f64 / 9.0;
                (
                    [0, 1].map(|j| p.prev[j] + (p.pos[j] - p.prev[j]) * w),
                    p.age,
                )
            })
        }));
        let pick = |table: &[(f64, Rgb)], age: f64| {
            table
                .iter()
                .find(|(lim, _)| age < *lim)
                .map_or(STONE, |e| e.1)
        };

        let hint = "press any key to skip";
        let hint_x = (self.w - hint.len()) / 2;
        let marks = &self.marks[..shot.marks];
        let mut lines = Vec::with_capacity(self.h);
        for y in 0..self.h {
            let mut line = String::new();
            let mut cur: Option<String> = None;
            for x in 0..self.w {
                let i = y * self.w + x;
                let bird_bits = body[i] | far[i] | word[i];
                let (ch, col): (char, Option<String>) = if marks.contains(&(x, y)) {
                    ('+', Some(c.fg(CLAY)))
                } else if shot.hint && y == self.h - 1 && (hint_x..hint_x + hint.len()).contains(&x)
                {
                    (hint.as_bytes()[x - hint_x] as char, Some(c.fg(self.muted)))
                } else if bird_bits != 0 {
                    let col = if body[i] != 0 {
                        shot.bird_color.map_or(reset.to_string(), |rgb| c.fg(rgb))
                    } else if word[i] != 0 {
                        if band[i] != 0 {
                            c.fg(VIOLET)
                        } else {
                            reset.to_string()
                        }
                    } else if !shot.landed {
                        // In flight the far wing is Steel, so that it looks
                        // farther away than the body.
                        c.fg(STEEL)
                    } else {
                        reset.to_string()
                    };
                    (braille(bird_bits), Some(col))
                } else if streak_bits[i] != 0 {
                    (
                        braille(streak_bits[i]),
                        Some(c.fg(pick(&STREAK_COLORS, streak_age[i]))),
                    )
                } else if spark_bits[i] != 0 {
                    (
                        braille(spark_bits[i]),
                        Some(c.fg(pick(&SPARK_COLORS, spark_age[i]))),
                    )
                } else {
                    (' ', None)
                };
                if let Some(col) = col
                    && cur.as_ref() != Some(&col)
                {
                    line.push_str(&col);
                    cur = Some(col);
                }
                line.push(ch);
            }
            line.push_str(reset);
            lines.push(line);
        }
        lines
    }

    /// Returns the final frame, which shows the landed logo.
    pub(super) fn final_frame(&self) -> Vec<String> {
        let shot = Shot {
            body: &self.final_bird,
            far: &self.final_far,
            bird_color: None,
            landed: true,
            word: Word::Full,
            marks: 4,
            hint: false,
        };
        self.compose(&shot, &[], &[])
    }

    /// Returns the points of the body with the tail moved. `bend` rotates the tail about its
    /// root, in radians. `sway` stretches the tail along x by a fraction of its length, which
    /// shows a side-to-side swing in the side view.
    pub(super) fn body_points(&self, bend: f64, sway: f64) -> Cow<'_, [Pt]> {
        if bend == 0.0 && sway == 0.0 {
            return Cow::Borrowed(&self.statics);
        }
        let root = svg_px(TAIL_ROOT);
        let moved = self.statics.iter().zip(&self.tail_w).map(|(p, &w)| {
            if w > 0.0 {
                let q = turn(*p, root, bend * w);
                [root[0] + (q[0] - root[0]) * (1.0 + sway * w), q[1]]
            } else {
                *p
            }
        });
        Cow::Owned(moved.collect())
    }
}

fn braille(bits: u8) -> char {
    char::from_u32(0x2800 + bits as u32).unwrap_or(' ')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::splash::test_util::strip;

    #[test]
    fn logo_asset_decodes_to_full_grid() {
        let (w, h, labels) = decode_logo();
        assert_eq!((w, h), (2000, 875));
        assert_eq!(labels.len(), w * h);
        for label in [0, BODY, FAR_WING, WORD] {
            assert!(labels.contains(&label), "label {label} missing");
        }
        assert!(labels.iter().all(|l| *l <= WORD));
    }

    #[test]
    fn final_frame_is_the_logo_in_the_text_color() {
        for cols in [60, 80, 100] {
            let film = Film::new(cols, ColorMode::Off);
            let frame = film.final_frame();
            assert_eq!(frame.len(), film.h);
            assert!(frame.iter().all(|l| strip(l).chars().count() == cols));
            let dots: usize = frame
                .iter()
                .map(|l| {
                    l.chars()
                        .filter(|c| ('\u{2801}'..='\u{28FF}').contains(c))
                        .count()
                })
                .sum();
            assert!(dots > 180, "too few logo cells at {cols} columns: {dots}");
        }
    }

    /// Resamples `src` with the same Lanczos weights as `lanczos_labels`, but
    /// visits every px.
    fn lanczos_dense(src: &[f64], w: usize, h: usize, dw: usize, dh: usize) -> Vec<f64> {
        let (wx, wy) = (lanczos_weights(w, dw, SRC_W), lanczos_weights(h, dh, SRC_H));
        let mut rows = vec![0.0; h * dw];
        for y in 0..h {
            for (x, (lo, ws)) in wx.iter().enumerate() {
                rows[y * dw + x] = ws
                    .iter()
                    .enumerate()
                    .map(|(k, v)| v * src[y * w + lo + k])
                    .sum();
            }
        }
        let mut out = vec![0.0; dh * dw];
        for (y, (lo, ws)) in wy.iter().enumerate() {
            for x in 0..dw {
                out[y * dw + x] = ws
                    .iter()
                    .enumerate()
                    .map(|(k, v)| v * rows[(lo + k) * dw + x])
                    .sum();
            }
        }
        out
    }

    #[test]
    fn fast_resize_matches_the_plain_filter() {
        let (w, h, labels) = decode_logo();
        let (dw, dh) = (128, 56);
        let fast = lanczos_labels(&labels, w, h, dw, dh);
        for label in [BODY, FAR_WING, WORD] {
            let mask: Vec<f64> = labels.iter().map(|&l| (l == label) as u8 as f64).collect();
            let plain = lanczos_dense(&mask, w, h, dw, dh);
            let worst = plain
                .iter()
                .zip(&fast[label as usize])
                .map(|(a, b)| (a - b).abs())
                .fold(0.0, f64::max);
            assert!(worst < 1e-9, "label {label} differs by {worst}");
        }
    }

    #[test]
    fn wing_is_lowest_at_half_a_wingbeat() {
        let film = Film::new(80, ColorMode::Off);
        let mut a = Vec::new();
        film.near.pose(0.0, 1.0, &mut a);
        let mut b = Vec::new();
        film.near.pose(PI, 1.0, &mut b);
        // Phase 0 is the top of the wingbeat. At phase pi the wing is at its
        // lowest position, which is below the logo pose.
        let rest_y: f64 = a.iter().map(|p| p[1]).sum::<f64>() / a.len() as f64;
        let down_y: f64 = b.iter().map(|p| p[1]).sum::<f64>() / b.len() as f64;
        assert!(
            down_y > rest_y + 50.0,
            "wing did not fold: {rest_y} -> {down_y}"
        );
    }

    #[test]
    fn lanczos_weights_sum_to_1_and_stay_inside_the_source() {
        // (source samples, output samples) for a smaller, an equal and a larger output.
        for (src, dst) in [(2000, 128), (875, 56), (100, 100), (10, 40)] {
            for (lo, ws) in lanczos_weights(src, dst, src as f64) {
                let sum: f64 = ws.iter().sum();
                assert!((sum - 1.0).abs() < 1e-9, "{src} to {dst}: sum {sum}");
                assert!(!ws.is_empty() && lo + ws.len() <= src, "{src} to {dst}");
            }
        }
    }

    #[test]
    fn path_runs_from_its_first_to_its_last_point_at_a_constant_speed() {
        let q = [[0.0, 0.0], [10.0, 40.0], [60.0, -20.0], [100.0, 0.0]];
        let path = Path::new(q);
        let far = |a: Pt, b: Pt| ((b[0] - a[0]).powi(2) + (b[1] - a[1]).powi(2)).sqrt();
        assert!(far(path.at(0.0), q[0]) < 1e-9 && far(path.at(1.0), q[3]) < 1e-9);
        let steps: Vec<f64> = (0..50)
            .map(|i| far(path.at(i as f64 / 50.0), path.at((i + 1) as f64 / 50.0)))
            .collect();
        let mean = steps.iter().sum::<f64>() / steps.len() as f64;
        assert!(
            steps.iter().all(|s| (s / mean - 1.0).abs() < 0.05),
            "uneven steps: {steps:.2?}"
        );
    }

    #[test]
    fn hinge_coordinates_convert_back_to_the_point() {
        let hinge = Hinge::new(NEAR_HINGE);
        for p in [[0.0, 0.0], [1500.0, 200.0], [1700.5, 33.25]] {
            let (s, d) = hinge.coords(p);
            let q = hinge.point(s, d);
            assert!(
                (q[0] - p[0]).abs() < 1e-9 && (q[1] - p[1]).abs() < 1e-9,
                "{p:?} -> {q:?}"
            );
        }
        // On the screen, y grows downward. A point above the hinge is out from it.
        let above = [hinge.origin[0], hinge.origin[1] - 10.0];
        assert!(hinge.coords(above).1 > 0.0);
    }

    #[test]
    fn wing_without_a_wingbeat_is_in_the_logo_pose() {
        let film = Film::new(80, ColorMode::Off);
        let wing = &film.near;
        let logo: Vec<Pt> = (0..wing.s.len())
            .map(|i| wing.hinge.point(wing.s[i], wing.d[i]))
            .collect();
        let mut pose = Vec::new();
        wing.pose_with(wing.kmin, |_| 1.0, &mut pose);
        assert_eq!(pose, logo);
        for phase in [0.0, 1.0, PI] {
            wing.pose(phase, 0.0, &mut pose);
            assert_eq!(pose, logo, "phase {phase}");
        }
    }

    #[test]
    fn frames_fit_at_each_window_width() {
        // Up to 74 columns the logo grows with the window.
        for cols in [60, 61, 73, 74, 75, 76, 99, 100] {
            let film = Film::new(cols, ColorMode::Off);
            assert_eq!((film.w, film.h), (cols, Film::rows(cols)));
            assert!(
                film.logo_x + film.logo_w <= film.wd,
                "the logo is cut at {cols} columns"
            );
            assert!(
                film.marks.iter().all(|&(x, y)| x < film.w && y < film.h),
                "a mark is outside the frame at {cols} columns"
            );
            let frame = film.final_frame();
            assert_eq!(frame.len(), film.h);
            assert!(
                frame.iter().all(|row| row.chars().count() == cols),
                "ragged frame at {cols} columns"
            );
        }
    }

    #[test]
    fn tail_bend_and_sway_move_only_the_tail() {
        let film = Film::new(80, ColorMode::Off);
        assert!(matches!(film.body_points(0.0, 0.0), Cow::Borrowed(_)));
        let tip = (0..film.statics.len())
            .max_by(|&a, &b| film.tail_w[a].total_cmp(&film.tail_w[b]))
            .unwrap();
        let still = film.statics[tip];
        let bent = film.body_points(-0.2, 0.0);
        let swayed = film.body_points(0.0, 0.2);
        for (i, p) in film.statics.iter().enumerate() {
            if film.tail_w[i] == 0.0 {
                assert_eq!((bent[i], swayed[i]), (*p, *p), "body point {i} moved");
            }
        }
        // On the screen, y grows downward. A negative bend lowers the tip.
        assert!(
            bent[tip][1] > still[1] + 1.0,
            "{still:?} -> {:?}",
            bent[tip]
        );
        // A positive sway moves the tip away from the root, to the left.
        let moved = swayed[tip];
        assert!(
            moved[0] < still[0] - 1.0 && (moved[1] - still[1]).abs() < 1e-9,
            "{still:?} -> {moved:?}"
        );
    }

    /// Composes a frame without a bird, and returns its rows as one string.
    fn compose_without_bird(film: &Film, word: Word, marks: usize, hint: bool) -> String {
        let empty = vec![false; film.hd * film.wd];
        let shot = Shot {
            body: &empty,
            far: &empty,
            bird_color: None,
            landed: false,
            word,
            marks,
            hint,
        };
        film.compose(&shot, &[], &[]).concat()
    }

    #[test]
    fn registration_marks_and_hint_show_on_request() {
        let film = Film::new(80, ColorMode::Off);
        assert_eq!(
            compose_without_bird(&film, Word::Hidden, 0, false).trim(),
            ""
        );
        let three = compose_without_bird(&film, Word::Hidden, 3, false);
        assert_eq!(three.matches('+').count(), 3);
        let hint = compose_without_bird(&film, Word::Hidden, 4, true);
        assert!(hint.contains("press any key to skip"));
    }

    #[test]
    fn wordmark_appears_from_left_to_right() {
        let film = Film::new(80, ColorMode::Off);
        let cells = |word| {
            let frame = compose_without_bird(&film, word, 0, false);
            frame.chars().filter(|c| *c != ' ').count()
        };
        let full = cells(Word::Full);
        assert_eq!((cells(Word::Hidden), cells(Word::Sweep(0.0))), (0, 0));
        let half = cells(Word::Sweep(0.5));
        assert!(
            0 < half && half < full,
            "{half} of {full} cells at half of the sweep"
        );
        assert_eq!(cells(Word::Sweep(1.0)), full);
    }
}
