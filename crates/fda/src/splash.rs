//! First-run splash: the Feldera bird flies in and lands on the wordmark.
//!
//! Three shots: the bird flies at the camera, makes a side-on pass, then lands
//! and a band of light reveals the wordmark. The art is braille dots: each
//! terminal cell holds a 2x4 grid of dots.
//!
//! It plays once, on the first command run in an interactive terminal. Any key
//! skips to the final logo. `CI`, `FDA_NO_ANIMATION`, `TERM=dumb` and
//! non-terminal output turn it off. `fda welcome` replays it.

use std::f64::consts::PI;
use std::io::{IsTerminal, Write, stdout};
use std::time::{Duration, Instant};

/// Logo label grid, made by `scripts/make_splash_logo.sh`.
const LOGO: &[u8] = include_bytes!("splash_logo.bin");
/// The in-flight bird uses every `SAMPLE_STEP`th px of the grid, to stay fast.
const SAMPLE_STEP: usize = 3;
const SRC_W: f64 = 2000.0;
const SRC_H: f64 = 875.0;
const BODY: u8 = 1;
const FAR_WING: u8 = 2;
const WORD: u8 = 3;

/// Playback speed. 1.0 is the fast cut; 0.5 is the slow, cinematic one.
const SPEED: f64 = 1.0;
const FPS: f64 = 30.0;

// Shot timeline in seconds, before `SPEED`.
const APPROACH: f64 = 0.35;
const FLYBY: f64 = 2.05;
const HERO: f64 = 2.55;
const LAND: f64 = 5.35;
const SWEEP: f64 = 5.15;
const END: f64 = 6.6;

// Brand palette. The landed logo always ends in the terminal's own text color.
type Rgb = (u8, u8, u8);
const VIOLET: Rgb = (0x62, 0x4E, 0xB3);
const STEEL: Rgb = (0x99, 0xA2, 0xB3);
const LICHEN: Rgb = (0x96, 0x96, 0x64);
const CLAY: Rgb = (0xAE, 0xA6, 0x8C);
const STONE: Rgb = (0xD7, 0xD7, 0xCC);
const LICHEN_TINT: Rgb = (0xDD, 0xDD, 0xC1);
const INK: Rgb = (0x20, 0x1F, 0x1E);
/// Spark color by age in frames: (max age, color).
const TRAIL: [(f64, Rgb); 5] = [
    (6.0, VIOLET),
    (14.0, STEEL),
    (22.0, LICHEN),
    (30.0, CLAY),
    (38.0, STONE),
];
const STREAK: [(f64, Rgb); 3] = [(4.0, VIOLET), (9.0, STEEL), (14.0, STONE)];

/// Braille bit for the dot at [row][column] of a cell.
const BITS: [[u8; 2]; 4] = [[0x01, 0x08], [0x02, 0x10], [0x04, 0x20], [0x40, 0x80]];

// Bird geometry in the mark's SVG units, mapped onto the logo's px.
const NEAR_HINGE: [[f64; 2]; 2] = [[268.0, 240.0], [377.0, 204.0]];
const FAR_HINGE: [[f64; 2]; 2] = [[414.3, 180.8], [539.9, 200.2]];
const BIRD_PX: [f64; 4] = [1256.0, 3.0, 1953.0, 483.0];

type Pt = [f64; 2];

fn svg_px(p: Pt) -> Pt {
    [
        BIRD_PX[0] + (p[0] - 2.8) * 1.2556,
        BIRD_PX[1] + (p[1] - 0.8) * 1.2556,
    ]
}

fn smooth(e0: f64, e1: f64, x: f64) -> f64 {
    let x = ((x - e0) / (e1 - e0)).clamp(0.0, 1.0);
    x * x * (3.0 - 2.0 * x)
}

fn lowpass(cur: f64, target: f64, k: f64) -> f64 {
    cur + (target - cur) * k
}

/// Small xorshift generator: the splash needs noise, not a crypto RNG.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> f64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }

    fn range(&mut self, lo: f64, hi: f64) -> f64 {
        lo + (hi - lo) * self.next()
    }

    fn index(&mut self, len: usize) -> usize {
        ((self.next() * len as f64) as usize).min(len - 1)
    }
}

#[derive(Clone, Copy, PartialEq)]
enum ColorMode {
    Off,
    Ansi256,
    TrueColor,
}

impl ColorMode {
    fn detect() -> Self {
        if std::env::var_os("NO_COLOR").is_some() {
            ColorMode::Off
        } else if matches!(
            std::env::var("COLORTERM").as_deref(),
            Ok("truecolor") | Ok("24bit")
        ) {
            ColorMode::TrueColor
        } else {
            ColorMode::Ansi256
        }
    }

    fn fg(self, c: Rgb) -> String {
        self.sgr(38, c)
    }

    fn bg(self, c: Rgb) -> String {
        self.sgr(48, c)
    }

    fn sgr(self, layer: u8, (r, g, b): Rgb) -> String {
        match self {
            ColorMode::Off => String::new(),
            ColorMode::TrueColor => format!("\x1b[{layer};2;{r};{g};{b}m"),
            ColorMode::Ansi256 => {
                let q = |v: u8| (v as f64 / 51.0).round() as u16;
                format!("\x1b[{layer};5;{}m", 16 + q(r) * 36 + q(g) * 6 + q(b))
            }
        }
    }

    fn reset(self) -> &'static str {
        if self == ColorMode::Off {
            ""
        } else {
            "\x1b[0m"
        }
    }
}

/// One wing in hinge coordinates: `s` along the shoulder, `d` away from it.
struct Wing {
    a: Pt,
    t: Pt,
    n: Pt,
    s: Vec<f64>,
    d: Vec<f64>,
    u: Vec<f64>,
    dmax: f64,
    kmin: f64,
    lag: f64,
    sweep: f64,
}

impl Wing {
    fn new(pts: &[Pt], hinge: [Pt; 2], kmin: f64, lag: f64, sweep: f64) -> Self {
        let (a, b) = (svg_px(hinge[0]), svg_px(hinge[1]));
        let len = ((b[0] - a[0]).powi(2) + (b[1] - a[1]).powi(2)).sqrt();
        let t = [(b[0] - a[0]) / len, (b[1] - a[1]) / len];
        let n = [t[1], -t[0]]; // points up, away from the body
        let (s, d): (Vec<f64>, Vec<f64>) = pts
            .iter()
            .map(|p| {
                let r = [p[0] - a[0], p[1] - a[1]];
                (r[0] * t[0] + r[1] * t[1], r[0] * n[0] + r[1] * n[1])
            })
            .unzip();
        let dmax = d.iter().cloned().fold(f64::MIN, f64::max);
        let u = d
            .iter()
            .map(|d| (d / dmax.max(1.0)).clamp(0.0, 1.0))
            .collect();
        Wing {
            a,
            t,
            n,
            s,
            d,
            u,
            dmax,
            kmin,
            lag,
            sweep,
        }
    }

    /// Wing points for this beat. k=1 is the logo's rest pose; k<0 folds the
    /// wing below the shoulder. Tips lag the shoulder, so the wing curls.
    fn pose(&self, phase: f64, amp: f64, out: &mut Vec<Pt>) {
        out.clear();
        for i in 0..self.s.len() {
            let wave = 0.5 + 0.5 * (phase - self.lag * self.u[i]).cos();
            let k_raw = self.kmin + (1.0 - self.kmin) * wave;
            let k = 1.0 - amp * (1.0 - k_raw);
            let s = self.s[i] - self.sweep * self.dmax * self.u[i] * (1.0 - k) / (1.0 - self.kmin);
            let d = self.d[i] * k;
            out.push([
                self.a[0] + s * self.t[0] + d * self.n[0],
                self.a[1] + s * self.t[1] + d * self.n[1],
            ]);
        }
    }
}

/// Cubic Bezier sampled by arc length, so the bird moves at a speed we choose.
struct Path {
    xy: Vec<Pt>,
    s: Vec<f64>,
}

impl Path {
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

    fn at(&self, frac: f64) -> Pt {
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

struct Particle {
    pos: Pt,
    vel: Pt,
    prev: Pt,
    age: f64,
}

/// What the compositor needs for one frame.
struct Shot<'a> {
    body: &'a [bool],
    far: &'a [bool],
    bird_color: Option<Rgb>,
    word_band: Option<f64>,
    marks: usize,
    hint: bool,
    landed: bool,
}

struct Film {
    w: usize,
    h: usize,
    wd: usize,
    hd: usize,
    ld: usize,
    f: f64,
    ox: usize,
    final_bird: Vec<bool>,
    final_far: Vec<bool>,
    final_word: Vec<bool>,
    statics: Vec<Pt>,
    near: Wing,
    far: Wing,
    tips: Vec<usize>,
    tail: Vec<Pt>,
    anchor: Pt,
    marks: [(usize, usize); 4],
    vp: Pt,
    path: Path,
    color: ColorMode,
}

/// Decode the run-length label grid: (width, height, labels).
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

/// Lanczos-3 weights for resampling `src` grid cells onto `dst` cells along one axis.
/// `span` is the source length in grid cells that maps onto `dst`.
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

/// Separable Lanczos-3 resize of a `w`x`h` grid to `dw`x`dh`.
fn lanczos_resize(src: &[f64], w: usize, h: usize, dw: usize, dh: usize) -> Vec<f64> {
    let wx = lanczos_weights(w, dw, SRC_W);
    let wy = lanczos_weights(h, dh, SRC_H);
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

impl Film {
    fn new(cols: usize, color: ColorMode) -> Self {
        let (aw, ah, labels) = decode_logo();
        let w = cols;
        let l = (cols - 10).min(64);
        let ld = l * 2;
        let lh = (ld as f64 * SRC_H / SRC_W).round() as usize;
        let f = ld as f64 / SRC_W; // dots per source px
        let ox = (cols * 2 - ld) / 2 / 2 * 2;
        let oy = 16;
        let h = (oy + lh) / 4 + 2;
        let (hd, wd) = (h * 4, w * 2);

        // Final logo: Lanczos-resample each label onto the dot grid, so thin strokes stay crisp.
        let place = |want: u8| {
            let mask: Vec<f64> = labels.iter().map(|&l| (l == want) as u8 as f64).collect();
            let small = lanczos_resize(&mask, aw, ah, ld, lh);
            let mut g = vec![false; hd * wd];
            for ty in 0..lh {
                for tx in 0..ld {
                    g[(oy + ty) * wd + ox + tx] = small[ty * ld + tx] > 0.5;
                }
            }
            g
        };
        let (final_bird, final_far, final_word) = (place(BODY), place(FAR_WING), place(WORD));

        // Bird samples for the in-flight render, in source px.
        let mut pts = Vec::new();
        let mut is_far = Vec::new();
        for y in (0..ah).step_by(SAMPLE_STEP) {
            for x in (0..aw).step_by(SAMPLE_STEP) {
                let label = labels[y * aw + x];
                if label == BODY || label == FAR_WING {
                    pts.push([x as f64, y as f64]);
                    is_far.push(label == FAR_WING);
                }
            }
        }
        let probe = Wing::new(&pts, NEAR_HINGE, -0.55, 0.8, 0.18);
        let (mut statics, mut near_pts, mut far_pts) = (Vec::new(), Vec::new(), Vec::new());
        for (i, p) in pts.iter().enumerate() {
            if is_far[i] {
                far_pts.push(*p);
            } else if probe.d[i] > 0.0 {
                near_pts.push(*p);
            } else {
                statics.push(*p);
            }
        }
        let near = Wing::new(&near_pts, NEAR_HINGE, -0.55, 0.8, 0.18);
        let far = Wing::new(&far_pts, FAR_HINGE, -0.15, 0.7, 0.1);
        let tips = (0..near.u.len()).filter(|&i| near.u[i] > 0.85).collect();
        let tail_x = svg_px([70.0, 0.0])[0];
        let tail = statics.iter().filter(|p| p[0] < tail_x).cloned().collect();
        let anchor = [
            (BIRD_PX[0] + BIRD_PX[2]) / 2.0,
            (BIRD_PX[1] + BIRD_PX[3]) / 2.0,
        ];
        let land = [ox as f64 + anchor[0] * f, oy as f64 + anchor[1] * f];
        let (ml, mr) = (ox / 2 - 3, ox / 2 + l + 2);
        let (wdf, hdf) = (wd as f64, hd as f64);
        Film {
            w,
            h,
            wd,
            hd,
            ld,
            f,
            ox,
            final_bird,
            final_far,
            final_word,
            statics,
            near,
            far,
            tips,
            tail,
            anchor,
            marks: [(ml, 0), (mr, 0), (mr, h - 1), (ml, h - 1)],
            vp: [0.66 * wdf, 0.40 * hdf],
            path: Path::new([
                [-0.22 * wdf, 0.22 * hdf],
                [0.22 * wdf, 1.15 * hdf],
                [0.52 * wdf, -0.35 * hdf],
                land,
            ]),
            color,
        }
    }

    fn project(&self, p: Pt, rot: f64, sc: f64, center: Pt) -> Pt {
        let (c, s) = (rot.cos(), rot.sin());
        let (dx, dy) = (p[0] - self.anchor[0], p[1] - self.anchor[1]);
        let k = sc * self.f;
        [
            (dx * c - dy * s) * k + center[0],
            (dx * s + dy * c) * k + center[1],
        ]
    }

    /// Dot grid where enough bird samples land, so edges stay crisp at any scale.
    fn raster<'a>(
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
        let need = (0.45 * (sc * self.f * SAMPLE_STEP as f64).powi(-2)).max(1.0);
        counts.iter().map(|&c| c as f64 >= need).collect()
    }

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

    /// Youngest particle age per dot, then per cell.
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

    fn compose(&self, shot: &Shot, sparks: &[Particle], streaks: &[Particle]) -> Vec<String> {
        let c = self.color;
        let reset = c.reset();
        let body = self.cells(shot.body);
        let far = self.cells(shot.far);

        // Wordmark: a Violet band sweeps left to right and leaves the letters behind it.
        let (mut word, mut band) = (vec![0u8; self.h * self.w], vec![0u8; self.h * self.w]);
        if let Some(b) = shot.word_band.filter(|b| *b > 0.0) {
            let edge = self.ox as f64 + b * 1.15 * self.ld as f64;
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

        let (spark_bits, spark_age) = self.age_cells(sparks.iter().map(|p| (p.pos, p.age)));
        let (streak_bits, streak_age) = self.age_cells(streaks.iter().flat_map(|p| {
            // Draw each streak as the segment it covered since the last frame.
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
                    (hint.as_bytes()[x - hint_x] as char, Some(c.fg(STONE)))
                } else if bird_bits != 0 {
                    let col = if body[i] != 0 {
                        shot.bird_color.map_or(reset.to_string(), |rgb| c.fg(rgb))
                    } else if word[i] != 0 {
                        if band[i] != 0 {
                            c.fg(VIOLET)
                        } else {
                            reset.to_string()
                        }
                    } else if shot.bird_color.is_some() || !shot.landed {
                        c.fg(STEEL) // the far wing sits back in Steel mid-flight
                    } else {
                        reset.to_string()
                    };
                    (braille(bird_bits), Some(col))
                } else if streak_bits[i] != 0 {
                    (
                        braille(streak_bits[i]),
                        Some(c.fg(pick(&STREAK, streak_age[i]))),
                    )
                } else if spark_bits[i] != 0 {
                    (
                        braille(spark_bits[i]),
                        Some(c.fg(pick(&TRAIL, spark_age[i]))),
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

    fn final_frame(&self) -> Vec<String> {
        let shot = Shot {
            body: &self.final_bird,
            far: &self.final_far,
            bird_color: None,
            word_band: Some(9.0),
            marks: 4,
            hint: false,
            landed: true,
        };
        self.compose(&shot, &[], &[])
    }
}

fn braille(bits: u8) -> char {
    char::from_u32(0x2800 + bits as u32).unwrap_or(' ')
}

/// Moving parts of the film between frames.
struct Reel {
    rng: Rng,
    sparks: Vec<Particle>,
    streaks: Vec<Particle>,
    phase: f64,
    rot: f64,
    amp: f64,
    prev_pos: Option<Pt>,
    near_buf: Vec<Pt>,
    far_buf: Vec<Pt>,
}

impl Reel {
    fn new(seed: u64) -> Self {
        Reel {
            rng: Rng(seed | 1),
            sparks: Vec::new(),
            streaks: Vec::new(),
            phase: 0.0,
            rot: 0.0,
            amp: 1.0,
            prev_pos: None,
            near_buf: Vec::new(),
            far_buf: Vec::new(),
        }
    }

    fn frame_count() -> usize {
        (END / SPEED * FPS) as usize + 1
    }

    /// Render frame `i` of the film. Frames must be rendered in order.
    fn frame(&mut self, film: &Film, i: usize) -> Vec<String> {
        let t = i as f64 / FPS * SPEED;
        let (hd, wd) = (film.hd as f64, film.wd as f64);
        let empty = vec![false; film.hd * film.wd];
        let (mut body, mut far) = (empty.clone(), empty);
        let mut bird_color = None;
        let mut word_band = None;
        let mut landed = false;
        let mut new_streaks = 0;

        if (APPROACH..FLYBY + 0.25).contains(&t) {
            // Shot 1: the bird flies at the camera. z shrinks linearly, so scale grows as 1/z.
            let p = (t - APPROACH) / (FLYBY + 0.25 - APPROACH);
            let sc = 0.24 / (1.0 - 0.945 * p);
            let k = (sc - 0.24) / 4.1;
            let pos = [film.vp[0] - 0.55 * wd * k, film.vp[1] - 0.28 * hd * k];
            self.phase += 2.0 * PI * 2.6 * SPEED / FPS;
            film.near.pose(self.phase, 1.0, &mut self.near_buf);
            film.far.pose(self.phase + 0.25, 1.0, &mut self.far_buf);
            body = film.raster(film.statics.iter().chain(&self.near_buf), 0.0, sc, pos);
            far = film.raster(self.far_buf.iter(), 0.0, sc, pos);
            if sc < 0.55 {
                bird_color = Some(STEEL); // far away reads as distant
            }
            if sc > 3.0 {
                // Passing the camera: the bird breaks up as it leaves the frame.
                for d in body.iter_mut().chain(far.iter_mut()) {
                    *d = *d && self.rng.next() > sc - 3.0;
                }
            }
            new_streaks = (self.rng.next() < 0.5 + p) as usize + (4.0 * p.powi(3)) as usize;
        } else if t >= HERO {
            // Shot 2: a side-on pass along an arc-length path, a flare, and a soft touchdown.
            let u = ((t - HERO) / (LAND - HERO)).min(1.0);
            let dist = 1.0 - (1.0 - u).powf(2.3); // cruise in, slow down for the perch
            let pos = film.path.at(dist);
            let vel = self.prev_pos.map_or([0.0, 0.0], |q| {
                [(pos[0] - q[0]) * FPS / SPEED, (pos[1] - q[1]) * FPS / SPEED]
            });
            self.prev_pos = Some(pos);
            let climb = (-vel[1] / 60.0).clamp(-1.0, 1.0);
            let flare = smooth(0.74, 0.86, u) * (1.0 - smooth(0.93, 1.0, u));
            let settle = smooth(0.9, 1.0, u);

            // Wingbeat follows effort: hard beats to climb, a glide on dives, braking strokes to land.
            let freq = 2.0 + 1.3 * climb.max(0.0) + 1.6 * flare;
            let target =
                (0.45 + 0.55 * climb.max(0.0) - 0.3 * (-climb).max(0.0)) * (1.0 - flare) + flare;
            self.amp = lowpass(self.amp, target.max(0.15), 0.12) * (1.0 - settle);
            self.phase += 2.0 * PI * freq * SPEED / FPS;

            // The body tilts with its heading a beat late; the nose comes up for the flare.
            let heading = vel[1].atan2(vel[0].abs().max(1e-3));
            let rot_target = (0.5 * heading).clamp(-0.35, 0.35) * (1.0 - flare) - 0.26 * flare;
            self.rot = lowpass(self.rot, rot_target, 0.14) * (1.0 - settle);

            let sc = 1.0 + 0.45 * (1.0 - dist);
            let lift = -2.2 * sc * self.phase.sin() * self.amp;
            let td = (t - LAND) / 0.14;
            let dip = if t > LAND {
                1.6 * td * (1.0 - td).exp()
            } else {
                0.0
            };
            let center = [pos[0], pos[1] + lift + dip];

            if t > LAND + 0.45 {
                body = film.final_bird.clone();
                far = film.final_far.clone();
                landed = true;
            } else {
                film.near.pose(self.phase, self.amp, &mut self.near_buf);
                film.far
                    .pose(self.phase + 0.25, self.amp, &mut self.far_buf);
                body = film.raster(
                    film.statics.iter().chain(&self.near_buf),
                    self.rot,
                    sc,
                    center,
                );
                far = film.raster(self.far_buf.iter(), self.rot, sc, center);
                // Sparks off the wingtips on the downstroke, and a thin stream off the tail.
                if self.amp > 0.3 && self.phase.sin() > 0.0 && u < 0.85 {
                    for _ in 0..3 {
                        let tip = self.near_buf[film.tips[self.rng.index(film.tips.len())]];
                        let vel = [-self.rng.range(0.3, 0.7), self.rng.range(0.05, 0.2)];
                        self.spark(film.project(tip, self.rot, sc, center), vel);
                    }
                }
                if u < 0.8 {
                    for _ in 0..2 {
                        let tail = film.tail[self.rng.index(film.tail.len())];
                        let vel = [-self.rng.range(0.1, 0.4), self.rng.range(-0.03, 0.03)];
                        self.spark(film.project(tail, self.rot, sc, center), vel);
                    }
                }
            }
            word_band = Some((t - SWEEP) / 0.95);
        }

        // Speed lines radiate from the vanishing point and speed up.
        for _ in 0..new_streaks {
            let ang = self.rng.range(0.0, 2.0 * PI);
            let r0 = self.rng.range(10.0, 22.0);
            let v = self.rng.range(0.8, 1.6);
            let pos = [
                film.vp[0] + ang.cos() * r0 * 2.0,
                film.vp[1] + ang.sin() * r0,
            ];
            self.streaks.push(Particle {
                pos,
                vel: [ang.cos() * v * 2.0, ang.sin() * v],
                prev: pos,
                age: 0.0,
            });
        }
        for s in &mut self.streaks {
            s.prev = s.pos;
            s.pos = [s.pos[0] + s.vel[0], s.pos[1] + s.vel[1]];
            s.vel = [s.vel[0] * 1.2, s.vel[1] * 1.2];
            s.age += 1.0;
        }
        self.streaks.retain(|s| s.age < STREAK[STREAK.len() - 1].0);
        for s in &mut self.sparks {
            s.pos = [s.pos[0] + s.vel[0], s.pos[1] + s.vel[1]];
            s.age += 1.0;
        }
        self.sparks.retain(|s| s.age < TRAIL[TRAIL.len() - 1].0);

        let shot = Shot {
            body: &body,
            far: &far,
            bird_color,
            word_band,
            marks: (0..4).filter(|k| t >= 0.05 + 0.08 * *k as f64).count(),
            hint: t < LAND,
            landed,
        };
        film.compose(&shot, &self.sparks, &self.streaks)
    }

    fn spark(&mut self, pos: Pt, vel: Pt) {
        self.sparks.push(Particle {
            pos,
            vel,
            prev: pos,
            age: 0.0,
        });
    }
}

/// A line of window chrome that tracks its visible width.
struct Row {
    text: String,
    width: usize,
}

impl Row {
    fn new() -> Self {
        Row {
            text: String::new(),
            width: 0,
        }
    }

    fn push(mut self, s: &str, style: &str, reset: &str) -> Self {
        if style.is_empty() {
            self.text.push_str(s);
        } else {
            self.text.push_str(&format!("{style}{s}{reset}"));
        }
        self.width += s.chars().count();
        self
    }

    fn pad_to(mut self, col: usize) -> Self {
        let pad = col.saturating_sub(self.width);
        self.text.push_str(&" ".repeat(pad));
        self.width += pad;
        self
    }
}

/// Shorten `s` to `max` columns, ending in an ellipsis when cut.
fn fit(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(max.saturating_sub(1)).collect();
        out.push('…');
        out
    }
}

/// The host as people type it: no scheme, no trailing slash.
fn display_host(host: &str) -> String {
    let h = host.split_once("://").map_or(host, |(_, rest)| rest);
    h.trim_end_matches('/').to_string()
}

/// The window around the film: a title bar, the film, the tagline and a
/// get-started panel, in rounded box lines.
struct Window {
    inner: usize,
    color: ColorMode,
    host: String,
}

impl Window {
    /// Rows from the top border down to the film's first row.
    const FILM_TOP: usize = 1;
    /// Rows after the film: blank, tagline, blank, divider, blank, 3 panel rows, bottom.
    const BELOW_FILM: usize = 9;
    /// Columns from the left border to the tagline text.
    const TAGLINE_INDENT: usize = 2;

    fn line(&self, s: &str) -> String {
        format!("{}{s}{}", self.color.fg(STEEL), self.color.reset())
    }

    fn wrap(&self, row: &str) -> String {
        format!("{}{row}{}", self.line("│"), self.line("│"))
    }

    fn close(&self, row: Row) -> String {
        let row = row.pad_to(self.inner);
        self.wrap(&row.text)
    }

    fn top(&self) -> String {
        let c = self.color;
        let reset = c.reset();
        let bold = if c == ColorMode::Off { "" } else { "\x1b[1m" };
        let version = format!("v{}", env!("CARGO_PKG_VERSION"));
        let left = Row::new()
            .push(" ", "", reset)
            .push("fda", bold, reset)
            .push(" ", "", reset)
            .push(&version, &c.fg(STEEL), reset)
            .push(" ", "", reset);
        let site = " feldera.com ";
        let fill = (self.inner + 2).saturating_sub(4 + left.width + site.len());
        format!(
            "{}{}{}{}{site}{reset}{}",
            self.line("╭─"),
            left.text,
            self.line(&"─".repeat(fill)),
            c.fg(STEEL),
            self.line("─╮"),
        )
    }

    fn blank(&self) -> String {
        self.wrap(&" ".repeat(self.inner))
    }

    fn tagline(&self) -> String {
        let c = self.color;
        let band = format!("{}{}", c.bg(LICHEN_TINT), c.fg(INK));
        self.close(
            Row::new()
                .pad_to(Self::TAGLINE_INDENT)
                .push(TAGLINE[0], "", c.reset())
                .push(TAGLINE[1], &band, c.reset())
                .push(TAGLINE[2], "", c.reset()),
        )
    }

    /// GET STARTED and CONNECTION, side by side.
    fn panel(&self) -> [String; 3] {
        let c = self.color;
        let reset = c.reset();
        let (steel, lichen, clay) = (c.fg(STEEL), c.fg(LICHEN), c.fg(CLAY));
        let (left, desc) = (3, 18);
        let right = (self.inner / 2 + 3).max(desc + 16);
        let value = right + 8;
        let host = display_host(&self.host);
        let desc_max = right.saturating_sub(desc + 2);
        let value_max = self.inner.saturating_sub(value + 1);
        [
            self.close(
                Row::new()
                    .pad_to(left)
                    .push("GET STARTED", &steel, reset)
                    .pad_to(right)
                    .push("CONNECTION", &steel, reset),
            ),
            self.close(
                Row::new()
                    .pad_to(left)
                    .push("■", &lichen, reset)
                    .push(" fda --help", "", reset)
                    .pad_to(desc)
                    .push(&fit("every command", desc_max), &steel, reset)
                    .pad_to(right)
                    .push("■", &clay, reset)
                    .push(" Host", "", reset)
                    .pad_to(value)
                    .push(&fit(&host, value_max), "", reset),
            ),
            self.close(
                Row::new()
                    .pad_to(left)
                    .push("■", &lichen, reset)
                    .push(" Web console", "", reset)
                    .pad_to(desc)
                    .push(&fit(&host, desc_max), &steel, reset)
                    .pad_to(right)
                    .push("■", &clay, reset)
                    .push(" Docs", "", reset)
                    .pad_to(value)
                    .push(&fit("docs.feldera.com", value_max), "", reset),
            ),
        ]
    }

    /// Every row of the window, with `film` rows and the tagline shown or blank.
    fn rows(&self, film: &[String], tagline: bool) -> Vec<String> {
        let mut rows = vec![self.top()];
        rows.extend(film.iter().map(|l| self.wrap(l)));
        rows.push(self.blank());
        rows.push(if tagline {
            self.tagline()
        } else {
            self.blank()
        });
        rows.push(self.blank());
        rows.push(self.line(&format!("├{}┤", "─".repeat(self.inner))));
        rows.push(self.blank());
        rows.extend(self.panel());
        rows.push(self.line(&format!("╰{}╯", "─".repeat(self.inner))));
        rows
    }
}

const TAGLINE: [&str; 3] = ["Precisely compute ", "only what changed", "."];

#[cfg(unix)]
fn terminal_columns() -> usize {
    // SAFETY: TIOCGWINSZ fills a winsize struct we own; on error we keep the default.
    let mut ws: libc::winsize = unsafe { std::mem::zeroed() };
    let ok = unsafe { libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &mut ws) } == 0;
    if ok && ws.ws_col > 0 {
        ws.ws_col as usize
    } else {
        80
    }
}

#[cfg(not(unix))]
fn terminal_columns() -> usize {
    80
}

/// Puts stdin in non-canonical, no-echo mode so any key (Ctrl-C too) skips
/// the film. Restores the terminal on drop.
struct Keys {
    #[cfg(unix)]
    saved: Option<libc::termios>,
}

impl Keys {
    #[cfg(unix)]
    fn new() -> Self {
        // SAFETY: tcgetattr/tcsetattr on fd 0 with a termios we own.
        unsafe {
            let mut t: libc::termios = std::mem::zeroed();
            if libc::tcgetattr(libc::STDIN_FILENO, &mut t) != 0 {
                return Keys { saved: None };
            }
            let saved = t;
            t.c_lflag &= !(libc::ICANON | libc::ECHO | libc::ISIG);
            t.c_cc[libc::VMIN] = 0;
            t.c_cc[libc::VTIME] = 0;
            libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &t);
            Keys { saved: Some(saved) }
        }
    }

    #[cfg(not(unix))]
    fn new() -> Self {
        Keys {}
    }

    /// The bytes waiting on stdin, if any.
    #[cfg(unix)]
    fn pressed(&self) -> Option<Vec<u8>> {
        self.saved?;
        let mut buf = [0u8; 64];
        // SAFETY: VMIN=0/VTIME=0 make this read return at once.
        let n = unsafe { libc::read(libc::STDIN_FILENO, buf.as_mut_ptr().cast(), buf.len()) };
        (n > 0).then(|| buf[..n as usize].to_vec())
    }

    #[cfg(not(unix))]
    fn pressed(&self) -> Option<Vec<u8>> {
        None
    }
}

impl Drop for Keys {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Some(t) = self.saved {
            // SAFETY: restores the attributes saved in `new`.
            unsafe { libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &t) };
        }
    }
}

/// Shows the cursor again however the film ends.
struct Cursor;

impl Drop for Cursor {
    fn drop(&mut self) {
        let mut out = stdout().lock();
        let _ = write!(out, "\x1b[0m\x1b[?25h");
        let _ = out.flush();
    }
}

fn can_animate() -> bool {
    stdout().is_terminal()
        && std::io::stdin().is_terminal()
        && std::env::var_os("CI").is_none()
        && std::env::var_os("FDA_NO_ANIMATION").is_none()
        && std::env::var("TERM").map_or(true, |t| t != "dumb")
}

/// Play the film, or print the still logo when animation is off.
/// Play the film in its window, or print the still window when animation is off.
/// `host` is the Feldera host fda talks to, shown in the CONNECTION panel.
pub fn play(host: &str) {
    let color = ColorMode::detect();
    let inner = terminal_columns().saturating_sub(4).clamp(60, 100);
    let film = Film::new(inner, color);
    let window = Window {
        inner,
        color,
        host: host.to_string(),
    };
    let mut out = stdout().lock();
    if !can_animate() {
        for row in window.rows(&film.final_frame(), true) {
            let _ = writeln!(out, "{row}");
        }
        return;
    }

    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(5, |d| d.as_nanos() as u64);
    let mut reel = Reel::new(seed);
    let keys = Keys::new();
    let cursor = Cursor;
    let mut interrupted = false;
    let mut skipped = false;

    // Draw the empty window, then redraw only the film rows in place.
    let total = Window::FILM_TOP + film.h + Window::BELOW_FILM;
    let blank_film = vec![" ".repeat(inner); film.h];
    let _ = write!(out, "\x1b[?25l");
    for row in window.rows(&blank_film, false) {
        let _ = writeln!(out, "{row}");
    }
    let draw = |out: &mut std::io::StdoutLock, frame: &[String]| {
        let rows: Vec<String> = frame.iter().map(|l| window.wrap(l)).collect();
        let back = total - Window::FILM_TOP - film.h;
        let _ = write!(
            out,
            "\x1b[{}F\x1b[2K{}\n\x1b[{back}E",
            total - Window::FILM_TOP,
            rows.join("\n\x1b[2K")
        );
        let _ = out.flush();
    };
    let start = Instant::now();
    for i in 0..Reel::frame_count() {
        if let Some(bytes) = keys.pressed() {
            skipped = true;
            interrupted = bytes.contains(&0x03);
            break;
        }
        draw(&mut out, &reel.frame(&film, i));
        let due = start + Duration::from_secs_f64((i + 1) as f64 / FPS);
        std::thread::sleep(due.saturating_duration_since(Instant::now()));
    }
    if skipped {
        draw(&mut out, &film.final_frame());
    }

    // Type the tagline into its row.
    if !interrupted {
        let up = total - (Window::FILM_TOP + film.h + 1);
        let _ = write!(out, "\x1b[{up}F\x1b[{}C", 1 + Window::TAGLINE_INDENT);
        let band = format!("{}{}", color.bg(LICHEN_TINT), color.fg(INK));
        let reset = color.reset();
        for (i, part) in TAGLINE.iter().enumerate() {
            let style = if i == 1 { band.as_str() } else { "" };
            for ch in part.chars() {
                let _ = write!(out, "{style}{ch}{reset}");
                if !skipped {
                    let _ = out.flush();
                    std::thread::sleep(Duration::from_secs_f64(1.0 / 45.0));
                }
            }
        }
        let _ = write!(out, "\x1b[{up}E");
    }
    let _ = out.flush();
    drop(out);
    drop(cursor);
    drop(keys);
    if interrupted {
        std::process::exit(130);
    }
}

/// Play the film once per machine, on the first command run in a terminal.
pub fn first_run(host: &str) {
    // Too narrow for the window: wait for a wider terminal rather than wrap.
    if !can_animate() || terminal_columns() < 64 {
        return;
    }
    let Some(dirs) = directories::ProjectDirs::from("com", "Feldera", "fda") else {
        return;
    };
    let marker = dirs.config_dir().join("welcome-shown");
    if marker.exists() {
        return;
    }
    // Record it first, so a failed or interrupted run does not play it again.
    if std::fs::create_dir_all(dirs.config_dir()).is_err() || std::fs::write(&marker, b"").is_err()
    {
        return;
    }
    play(host);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strip(line: &str) -> String {
        let mut out = String::new();
        let mut esc = false;
        for ch in line.chars() {
            match (esc, ch) {
                (false, '\x1b') => esc = true,
                (true, c) if c.is_ascii_alphabetic() => esc = false,
                (true, _) => {}
                (false, c) => out.push(c),
            }
        }
        out
    }

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

    #[test]
    fn whole_film_renders_and_ends_on_the_final_frame() {
        let film = Film::new(80, ColorMode::TrueColor);
        let mut reel = Reel::new(7);
        let mut last = Vec::new();
        for i in 0..Reel::frame_count() {
            last = reel.frame(&film, i);
            assert_eq!(last.len(), film.h);
        }
        let plain = |f: &[String]| f.iter().map(|l| strip(l)).collect::<Vec<_>>();
        assert_eq!(plain(&last), plain(&film.final_frame()));
    }

    #[test]
    fn window_rows_line_up_and_show_the_host() {
        for cols in [60, 80, 100] {
            let film = Film::new(cols, ColorMode::TrueColor);
            let window = Window {
                inner: cols,
                color: ColorMode::TrueColor,
                host: "https://try.feldera.com/".to_string(),
            };
            let rows = window.rows(&film.final_frame(), true);
            assert_eq!(rows.len(), Window::FILM_TOP + film.h + Window::BELOW_FILM);
            for row in &rows {
                let plain = strip(row);
                assert_eq!(plain.chars().count(), cols + 2, "ragged row: {plain}");
                let (first, last) = (plain.chars().next().unwrap(), plain.chars().last().unwrap());
                assert!(
                    "╭│├╰".contains(first) && "╮│┤╯".contains(last),
                    "open border: {plain}"
                );
            }
            let all: String = rows.iter().map(|r| strip(r)).collect();
            assert!(
                all.contains("Host  try.feldera.com"),
                "host missing at {cols} columns"
            );
            assert!(!all.contains("https://"));
        }
    }

    #[test]
    fn bird_flaps_during_the_hero_pass() {
        let film = Film::new(80, ColorMode::Off);
        let mut a = Vec::new();
        film.near.pose(0.0, 1.0, &mut a);
        let mut b = Vec::new();
        film.near.pose(PI, 1.0, &mut b);
        // Rest pose (phase 0) matches the logo; the downstroke folds the wing down.
        let rest_y: f64 = a.iter().map(|p| p[1]).sum::<f64>() / a.len() as f64;
        let down_y: f64 = b.iter().map(|p| p[1]).sum::<f64>() / b.len() as f64;
        assert!(
            down_y > rest_y + 50.0,
            "wing did not fold: {rest_y} -> {down_y}"
        );
    }
}
