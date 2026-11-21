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
const EMPTY_LABEL: u8 = 0;
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
/// Landing: how high (dots) the bird hovers over the perch before it lowers
/// onto the words, how hard it flutters there, and how long after touchdown
/// it settles into the logo pose.
const HOVER_HEIGHT: f64 = 2.5;
const HOVER_AMP: f64 = 0.4;
const SETTLED: f64 = 0.55;
/// Hover springs: natural frequency (rad/s) and damping ratio for height and
/// pitch. Damping below 1 overshoots a little, as a body with weight does.
const HEIGHT_W: f64 = 10.0;
const HEIGHT_Z: f64 = 0.5;
const PITCH_W: f64 = 8.0;
const PITCH_Z: f64 = 0.45;
/// How hard a downstroke pushes the body up (dots/s² at full stroke).
const WING_PUSH: f64 = 1200.0;
/// Seconds the wings take to finish their stroke and close after touchdown.
const FOLD: f64 = 0.35;
/// Where the feet touch the words, in the mark's SVG units: the body tilts here.
const FEET: Pt = [360.0, 300.0];
/// How wide the one wingbeat that lowers the bird onto the words is (1 = full).
const DESCEND_AMP: f64 = 0.85;
/// Where along the hero pass (0..1) the bird starts down from its hover.
const DESCEND_FROM: f64 = 0.85;
/// Where the tail joins the body, in the mark's SVG units: the tail bends here.
const TAIL_ROOT: Pt = [235.0, 285.0];
/// Tail spring: natural frequency (rad/s) and damping ratio. On the descent
/// the tail lowers by TAIL_BRAKE (rad, negative is down). After touchdown it
/// eases back to level. Damping 1 stops it from swinging up past level.
const TAIL_W: f64 = 9.0;
const TAIL_Z: f64 = 1.0;
const TAIL_BRAKE: f64 = -0.18;
/// Tail sway in flight. Seen side-on, the tip moves left and right by TAIL_SWAY
/// of the tail's length, TAIL_SWAY_HZ times per second. It stops before the descent.
const TAIL_SWAY: f64 = 0.22;
const TAIL_SWAY_HZ: f64 = 1.4;
/// Height spring after touchdown. Damping 1 stops a bounce. Pitch uses the same
/// damping from the descent on, so the body does not rock.
const SETTLE_W: f64 = 9.0;
const SETTLE_Z: f64 = 1.0;

/// `p` turned by `angle` radians about `pivot`. Positive turns clockwise on screen.
fn turn(p: Pt, pivot: Pt, angle: f64) -> Pt {
    let (s, c) = angle.sin_cos();
    let (dx, dy) = (p[0] - pivot[0], p[1] - pivot[1]);
    [pivot[0] + dx * c - dy * s, pivot[1] + dx * s + dy * c]
}

// Brand palette. The landed logo always ends in the terminal's own text color.
type Rgb = (u8, u8, u8);
const VIOLET: Rgb = (0x62, 0x4E, 0xB3);
const STEEL: Rgb = (0x99, 0xA2, 0xB3);
const LICHEN: Rgb = (0x96, 0x96, 0x64);
const CLAY: Rgb = (0xAE, 0xA6, 0x8C);
const STONE: Rgb = (0xD7, 0xD7, 0xCC);
const LICHEN_TINT: Rgb = (0xDD, 0xDD, 0xC1);
const LICHEN_SHADE: Rgb = (0x63, 0x63, 0x3D);
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

/// Seconds the window takes to draw itself.
const INTRO: f64 = 1.1;
/// The final wingbeat: its length in seconds, and the wing
/// position k over it (1 = the logo pose, below 0 = folded under the shoulder).
const FLAP: f64 = 1.8;
const FLAP_TOP: f64 = 1.15;
const FLAP_LOW: f64 = -0.65;

/// Wing position over the final beat, at `tau` from 0 to 1: wind up past the
/// logo pose, one strong downstroke, then a slow return to the logo pose.
fn flap_k(tau: f64) -> f64 {
    let ease = |x: f64| x * x * (3.0 - 2.0 * x);
    match tau {
        t if t <= 0.0 => 1.0,
        t if t < 0.28 => 1.0 + (FLAP_TOP - 1.0) * ease(t / 0.28),
        t if t < 0.52 => FLAP_TOP + (FLAP_LOW - FLAP_TOP) * ease((t - 0.28) / 0.24),
        t if t < 1.0 => {
            let x = (t - 0.52) / 0.48;
            FLAP_LOW + (1.0 - FLAP_LOW) * (1.0 - (1.0 - x).powi(3))
        }
        _ => 1.0,
    }
}

/// Wing position at `tau` for a point at distance `u` along the wing. The
/// point starts `delay + lag * u` late; the beat is stretched by the same
/// amount, so every point, the last tip too, is back in the logo pose at tau = 1.
/// How far the final beat's tips trail the shoulder, and how late the far wing starts.
const NEAR_LAG: f64 = 0.12;
const FAR_LAG: f64 = 0.1;
const FAR_DELAY: f64 = 0.04;

fn flap_k_at(tau: f64, delay: f64, lag: f64, u: f64) -> f64 {
    flap_k(tau * (1.0 + delay + lag) - delay - lag * u)
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
            ColorMode::Ansi256 => format!("\x1b[{layer};5;{}m", ansi256((r, g, b))),
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

/// The xterm 256-color index nearest to `c`. The 6x6x6 cube's levels are not
/// evenly spaced, and the gray ramp is often nearer for muted colors.
fn ansi256(c: Rgb) -> u8 {
    const LEVELS: [u8; 6] = [0, 95, 135, 175, 215, 255];
    let near = |v: u8| {
        (0..6)
            .min_by_key(|&i| (LEVELS[i] as i32 - v as i32).abs())
            .unwrap()
    };
    let dist = |a: Rgb, b: Rgb| {
        let d = |x: u8, y: u8| (x as i32 - y as i32).pow(2);
        d(a.0, b.0) + d(a.1, b.1) + d(a.2, b.2)
    };
    let (ri, gi, bi) = (near(c.0), near(c.1), near(c.2));
    let cube = (LEVELS[ri], LEVELS[gi], LEVELS[bi]);
    let avg = (c.0 as i32 + c.1 as i32 + c.2 as i32) / 3;
    let k = ((avg - 8 + 5) / 10).clamp(0, 23) as u8;
    let g = 8 + 10 * k;
    if dist(c, (g, g, g)) < dist(c, cube) {
        232 + k
    } else {
        (16 + 36 * ri + 6 * gi + bi) as u8
    }
}

/// Whether the terminal background is dark or light. Secondary text needs a
/// different color on each: no one color reads well on both.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Theme {
    Dark,
    Light,
}

impl Theme {
    /// Color for secondary text (labels, hints, descriptions).
    fn muted(self) -> Rgb {
        match self {
            Theme::Dark => STEEL,
            Theme::Light => LICHEN_SHADE,
        }
    }

    /// From `COLORFGBG` ("fg;bg"), which some terminals set. Background 7 and
    /// 15 are white; the others are dark.
    fn from_colorfgbg(v: &str) -> Option<Theme> {
        let bg: u8 = v.rsplit(';').next()?.trim().parse().ok()?;
        match bg {
            7 | 15 => Some(Theme::Light),
            0..=15 => Some(Theme::Dark),
            _ => None,
        }
    }

    /// From a reply to the OSC 11 background query: `ESC ] 11 ; rgb:RRRR/GGGG/BBBB`.
    fn from_osc11(reply: &[u8]) -> Option<Theme> {
        let text = std::str::from_utf8(reply).ok()?;
        let rgb = &text[text.find("rgb:")? + 4..];
        let end = rgb.find(['\x07', '\x1b']).unwrap_or(rgb.len());
        let parts: Vec<f64> = rgb[..end]
            .split('/')
            .map(|h| {
                let max = 16f64.powi(h.len() as i32) - 1.0;
                u32::from_str_radix(h, 16).ok().map(|v| v as f64 / max)
            })
            .collect::<Option<_>>()?;
        let [r, g, b] = parts[..] else { return None };
        let luminance = 0.2126 * r + 0.7152 * g + 0.0722 * b;
        Some(if luminance > 0.5 {
            Theme::Light
        } else {
            Theme::Dark
        })
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

impl Wing {
    /// Wing points with `k_of_u` giving the wing position k for each point,
    /// by its distance `u` from the shoulder (0) to the tip (1).
    fn pose_k(&self, k_of_u: impl Fn(f64) -> f64, out: &mut Vec<Pt>) {
        out.clear();
        for i in 0..self.s.len() {
            let k = k_of_u(self.u[i]);
            let s = self.s[i] - self.sweep * self.dmax * self.u[i] * (1.0 - k) / (1.0 - FLAP_LOW);
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
    /// A wordmark grid to show whole, in place of the reveal.
    word_full: Option<&'a [bool]>,
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
    /// Top of the logo, in dots.
    oy: usize,
    final_bird: Vec<bool>,
    final_far: Vec<bool>,
    final_word: Vec<bool>,
    statics: Vec<Pt>,
    near: Wing,
    far: Wing,
    tips: Vec<usize>,
    tail: Vec<Pt>,
    /// Per point of `statics`: how much of the tail bend it takes, 0 at the
    /// tail root (and on the rest of the body) to 1 at the tail tip.
    tail_w: Vec<f64>,
    anchor: Pt,
    marks: [(usize, usize); 4],
    vp: Pt,
    path: Path,
    color: ColorMode,
    /// Secondary text color for the terminal's theme.
    muted: Rgb,
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

/// Lanczos-3 resize of each label in `labels`, a `w`x`h` grid, to `dw`x`dh`.
/// Returns the coverage grid of each label, indexed by label.
///
/// Most of the logo is empty, so the horizontal pass adds only the filled px
/// into the few output columns each one feeds, instead of filtering every px.
fn lanczos_labels(labels: &[u8], w: usize, h: usize, dw: usize, dh: usize) -> [Vec<f64>; 4] {
    // For each source column: the output columns it feeds, and with what weight.
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

/// Dot rows above the landed logo, where the bird flies.
const FILM_TOP_DOTS: usize = 16;

impl Film {
    /// Terminal rows the film takes at width `cols`.
    fn rows(cols: usize) -> usize {
        let ld = (cols - 10).min(64) * 2;
        let lh = (ld as f64 * SRC_H / SRC_W).round() as usize;
        (FILM_TOP_DOTS + lh) / 4 + 2
    }

    fn new(cols: usize, color: ColorMode) -> Self {
        let (aw, ah, labels) = decode_logo();
        let w = cols;
        let l = (cols - 10).min(64);
        let ld = l * 2;
        let lh = (ld as f64 * SRC_H / SRC_W).round() as usize;
        let f = ld as f64 / SRC_W; // dots per source px
        let ox = (cols * 2 - ld) / 2 / 2 * 2;
        let oy = FILM_TOP_DOTS;
        let h = Film::rows(cols);
        let (hd, wd) = (h * 4, w * 2);

        // Final logo: Lanczos-resample each label onto the dot grid, so thin strokes stay crisp.
        let coverage = lanczos_labels(&labels, aw, ah, ld, lh);
        let place = |want: u8| {
            let small = &coverage[want as usize];
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
        // The tail is the part of the body below and behind its root.
        let (root, tip_x) = (svg_px(TAIL_ROOT), svg_px([3.0, 0.0])[0]);
        let tail_w = statics
            .iter()
            .map(|p| {
                if p[0] < root[0] && p[1] > root[1] - 40.0 {
                    let w = ((root[0] - p[0]) / (root[0] - tip_x)).clamp(0.0, 1.0);
                    w * w * (3.0 - 2.0 * w)
                } else {
                    0.0
                }
            })
            .collect();
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
            oy,
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
            marks: [(ml, 0), (mr, 0), (mr, h - 1), (ml, h - 1)],
            vp: [0.66 * wdf, 0.40 * hdf],
            path: Path::new([
                [-0.22 * wdf, 0.22 * hdf],
                [0.22 * wdf, 1.15 * hdf],
                [0.52 * wdf, -0.35 * hdf],
                land,
            ]),
            color,
            muted: STEEL,
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
        if let Some(full) = shot.word_full {
            word = self.cells(full);
        } else if let Some(b) = shot.word_band.filter(|b| *b > 0.0) {
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
            word_full: None,
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
    /// Landing springs, from the flare on: height (dots) and its speed, body
    /// pitch (radians) and its speed.
    landing: Option<Springs>,
    /// The descending wingbeat: (phase at its start, phase at its end, start time).
    descend: Option<(f64, f64, f64)>,
    /// The wing stroke at touchdown: (phase then, phase where it closes).
    fold: Option<(f64, f64)>,
    /// The bob from the wingbeat on the last frame of flight, so the springs
    /// take over from where the bird is.
    last_lift: f64,
    /// Body height offset (dots), tilt (rad) and tail bend (rad) of the last
    /// hero-pass frame.
    last_pose: (f64, f64, f64),
    /// Tail sway: its phase (rad), and its swing on the last hero-pass frame.
    sway_phase: f64,
    last_sway: f64,
}

#[derive(Clone, Copy)]
struct Springs {
    y: f64,
    vy: f64,
    pitch: f64,
    vpitch: f64,
    bend: f64,
    vbend: f64,
    /// Height and speed where the descent starts.
    from: Option<(f64, f64)>,
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
            landing: None,
            descend: None,
            fold: None,
            last_lift: 0.0,
            last_pose: (0.0, 0.0, 0.0),
            sway_phase: 0.0,
            last_sway: 0.0,
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
            let flare = smooth(0.74, 0.86, u) * (1.0 - smooth(0.88, 0.94, u));
            // Hover: after the flare the bird holds just above the perch on
            // quick, small beats, then lowers straight down onto the words.
            let hover = smooth(0.88, 0.94, u);
            let hold = smooth(0.76, 0.86, u);
            let touched = t > LAND;
            let dt = SPEED / FPS;

            // Wingbeat follows effort: hard beats to climb, a glide on dives,
            // braking strokes in the flare, a light flutter in the hover.
            let freq = 2.0 + 1.3 * climb.max(0.0) + 1.6 * flare + 1.8 * hover;
            let cruise = 0.45 + 0.55 * climb.max(0.0) - 0.3 * (-climb).max(0.0);
            let target = (cruise * (1.0 - flare) + flare) * (1.0 - hover) + HOVER_AMP * hover;
            if touched {
                // The wings finish the stroke they are in and close into the
                // logo pose: the next whole beat, reached with a soft ease.
                let (p0, p1) = *self.fold.get_or_insert_with(|| {
                    let p1 = (self.phase / (2.0 * PI)).ceil() * 2.0 * PI;
                    (self.phase, p1)
                });
                let k = ((t - LAND) / FOLD).min(1.0);
                self.phase = p0 + (p1 - p0) * (1.0 - (1.0 - k).powi(3));
                self.amp *= (-dt / 0.3).exp();
            } else if u >= DESCEND_FROM {
                // Descent: one slow, full wingbeat lowers the bird onto the
                // words. It is slow at the top, strong through the downstroke,
                // and back in the logo pose as the feet touch.
                let (p0, p1, t0) = *self.descend.get_or_insert_with(|| {
                    let p1 = (self.phase / (2.0 * PI)).ceil() * 2.0 * PI + 2.0 * PI;
                    (self.phase, p1, t)
                });
                let s = ((t - t0) / (LAND - t0)).clamp(0.0, 1.0);
                self.phase = p0 + (p1 - p0) * s * s * (3.0 - 2.0 * s);
                self.amp = lowpass(self.amp, DESCEND_AMP, 0.15);
            } else {
                self.amp = lowpass(self.amp, target.max(0.15), 0.12);
                self.phase += 2.0 * PI * freq * dt;
            }

            // In flight the body tilts with its heading a beat late.
            let heading = vel[1].atan2(vel[0].abs().max(1e-3));
            let travel = (1.0 - flare) * (1.0 - hover);
            let rot_target =
                (0.5 * heading).clamp(-0.35, 0.35) * travel - 0.3 * flare - 0.08 * hover;
            self.rot = lowpass(self.rot, rot_target, 0.18);

            let sc = 1.0 + 0.45 * (1.0 - dist);
            let (y_off, tilt, bend) = if u < 0.74 {
                // Flight: the body bobs with each wingbeat.
                self.last_lift = -2.2 * sc * self.phase.sin() * self.amp;
                (self.last_lift, self.rot, 0.0)
            } else {
                // Landing: springs give the body weight. In the hover each
                // downstroke pushes the body up and it sinks between strokes.
                let s = self.landing.get_or_insert(Springs {
                    y: self.last_lift,
                    vy: 0.0,
                    pitch: self.rot,
                    vpitch: 0.0,
                    bend: 0.0,
                    vbend: 0.0,
                    from: None,
                });
                let push = if touched {
                    0.0
                } else {
                    -WING_PUSH * self.amp * (self.phase.sin().max(0.0) - 1.0 / PI)
                };
                // The tail lowers like an airbrake on the way down, then eases
                // back to level after touchdown.
                let bend_target = if touched { 0.0 } else { TAIL_BRAKE * hover };
                let ab = TAIL_W * TAIL_W * (bend_target - s.bend) - 2.0 * TAIL_Z * TAIL_W * s.vbend;
                s.vbend += ab * dt;
                s.bend += s.vbend * dt;
                let y_target = if self.descend.is_some() {
                    0.0
                } else {
                    -HOVER_HEIGHT * hold
                };
                // From the descent on the body eases level, with damping 1 so
                // it does not rock past level.
                let (pitch_target, pz) = if self.descend.is_some() {
                    (0.0, SETTLE_Z)
                } else {
                    (rot_target, PITCH_Z)
                };
                let (hw, hz) = if touched {
                    (SETTLE_W, SETTLE_Z)
                } else {
                    (HEIGHT_W, HEIGHT_Z)
                };
                let ay = hw * hw * (y_target - s.y) - 2.0 * hz * hw * s.vy;
                let ap =
                    PITCH_W * PITCH_W * (pitch_target - s.pitch) - 2.0 * pz * PITCH_W * s.vpitch;
                match self.descend {
                    Some((_, _, t0)) if !touched => {
                        // Descent: a slow, even glide down to the perch that
                        // starts from still and stops at touchdown.
                        let (y0, v0) = *s.from.get_or_insert((s.y, s.vy.min(0.0)));
                        let span = LAND - t0;
                        let x = ((t - t0) / span).clamp(0.0, 1.0);
                        let m0 = v0 * span;
                        s.y = y0 * (2.0 * x.powi(3) - 3.0 * x * x + 1.0)
                            + m0 * (x.powi(3) - 2.0 * x * x + x);
                        s.vy = (y0 * (6.0 * x * x - 6.0 * x) + m0 * (3.0 * x * x - 4.0 * x + 1.0))
                            / span;
                    }
                    _ => {
                        s.vy += (ay + push) * dt;
                        s.y += s.vy * dt;
                    }
                }
                s.vpitch += ap * dt;
                s.pitch += s.vpitch * dt;
                (s.y, s.pitch, s.bend)
            };

            // Tilt about the feet, not the middle of the picture, once the bird
            // comes in to land; blended in so the switch does not jump.
            let feet = svg_px(FEET);
            let k = sc * film.f;
            let v = [
                (feet[0] - film.anchor[0]) * k,
                (feet[1] - film.anchor[1]) * k,
            ];
            let (s, c) = tilt.sin_cos();
            let w = smooth(0.7, 0.8, u);
            let pivot = [
                (v[0] - (v[0] * c - v[1] * s)) * w,
                (v[1] - (v[0] * s + v[1] * c)) * w,
            ];
            let center = [pos[0] + pivot[0], pos[1] + y_off + pivot[1]];
            self.last_pose = (y_off, tilt, bend);
            // The tail sways in flight and is still by the descent.
            self.sway_phase += 2.0 * PI * TAIL_SWAY_HZ * dt;
            let sway = TAIL_SWAY * (1.0 - smooth(0.7, DESCEND_FROM, u)) * self.sway_phase.sin();
            self.last_sway = sway;

            if t > LAND + SETTLED {
                body = film.final_bird.clone();
                far = film.final_far.clone();
                landed = true;
            } else {
                film.near.pose(self.phase, self.amp, &mut self.near_buf);
                film.far
                    .pose(self.phase + 0.25, self.amp, &mut self.far_buf);
                // The tail bends and sways from its root, more toward its tip.
                let root = svg_px(TAIL_ROOT);
                let statics: Vec<Pt> = if bend == 0.0 && sway == 0.0 {
                    film.statics.clone()
                } else {
                    film.statics
                        .iter()
                        .zip(&film.tail_w)
                        .map(|(p, &w)| {
                            if w > 0.0 {
                                let q = turn(*p, root, bend * w);
                                [root[0] + (q[0] - root[0]) * (1.0 + sway * w), q[1]]
                            } else {
                                *p
                            }
                        })
                        .collect()
                };
                body = film.raster(statics.iter().chain(&self.near_buf), tilt, sc, center);
                far = film.raster(self.far_buf.iter(), tilt, sc, center);
                // Sparks off the wingtips on the downstroke, and a thin stream off the tail.
                if self.amp > 0.3 && self.phase.sin() > 0.0 && u < 0.85 {
                    for _ in 0..3 {
                        let tip = self.near_buf[film.tips[self.rng.index(film.tips.len())]];
                        let vel = [-self.rng.range(0.3, 0.7), self.rng.range(0.05, 0.2)];
                        self.spark(film.project(tip, tilt, sc, center), vel);
                    }
                }
                if u < 0.8 {
                    for _ in 0..2 {
                        let tail = film.tail[self.rng.index(film.tail.len())];
                        let vel = [-self.rng.range(0.1, 0.4), self.rng.range(-0.03, 0.03)];
                        self.spark(film.project(tail, tilt, sc, center), vel);
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
            word_full: None,
            marks: (0..4).filter(|k| t >= 0.05 + 0.08 * *k as f64).count(),
            hint: t < LAND,
            landed,
        };
        film.compose(&shot, &self.sparks, &self.streaks)
    }

    /// One frame of the final wingbeat on the landed logo, at `tau` from 0 to 1.
    fn flap_frame(&mut self, film: &Film, tau: f64) -> Vec<String> {
        // Tips trail the shoulder, so the wing moves like a heavy sail.
        film.near
            .pose_k(|u| flap_k_at(tau, 0.0, NEAR_LAG, u), &mut self.near_buf);
        film.far
            .pose_k(|u| flap_k_at(tau, FAR_DELAY, FAR_LAG, u), &mut self.far_buf);
        // The downstroke lifts the body a little; it settles as the wings return.
        let lift = -3.0 * (PI * ((tau - 0.38) / 0.55).clamp(0.0, 1.0)).sin();
        let center = [
            film.ox as f64 + film.anchor[0] * film.f,
            film.oy as f64 + film.anchor[1] * film.f + lift,
        ];
        let body = film.raster(film.statics.iter().chain(&self.near_buf), 0.0, 1.0, center);
        let far = film.raster(self.far_buf.iter(), 0.0, 1.0, center);

        // The downstroke pushes a stream of dust back out of the tail.
        if (0.3..0.62).contains(&tau) {
            for _ in 0..3 {
                let tail = film.tail[self.rng.index(film.tail.len())];
                let pos = film.project(tail, 0.0, 1.0, center);
                let vel = [-self.rng.range(0.5, 1.3), self.rng.range(-0.05, 0.25)];
                // Start past the Violet ages, so the dust is Steel fading to Stone.
                self.sparks.push(Particle {
                    pos,
                    vel,
                    prev: pos,
                    age: 7.0,
                });
            }
        }
        for s in &mut self.sparks {
            s.pos = [s.pos[0] + s.vel[0], s.pos[1] + s.vel[1]];
            s.vel = [s.vel[0] * 0.96, s.vel[1] * 0.96];
            s.age += 1.0;
        }
        self.sparks.retain(|s| s.age < TRAIL[TRAIL.len() - 1].0);

        let shot = Shot {
            body: &body,
            far: &far,
            bird_color: None,
            word_band: None,
            word_full: Some(&film.final_word),
            marks: 4,
            hint: false,
            landed: true,
        };
        film.compose(&shot, &self.sparks, &[])
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
    /// Secondary text color for the terminal's theme.
    muted: Rgb,
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
            .push(&version, &c.fg(self.muted), reset)
            .push(" ", "", reset);
        let site = " feldera.com ";
        let fill = (self.inner + 2).saturating_sub(4 + left.width + site.len());
        format!(
            "{}{}{}{}{site}{reset}{}",
            self.line("╭─"),
            left.text,
            self.line(&"─".repeat(fill)),
            c.fg(self.muted),
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
        let (steel, lichen, clay) = (c.fg(self.muted), c.fg(LICHEN), c.fg(CLAY));
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

impl Window {
    /// The window part-drawn, at `p` from 0 to 1. Registration
    /// marks land in the four corners, lines run out from them and meet, the
    /// divider crosses, then the title and the panel appear.
    fn intro_rows(&self, film_h: usize, p: f64) -> Vec<String> {
        let c = self.color;
        let reset = c.reset();
        let total = Self::FILM_TOP + film_h + Self::BELOW_FILM;
        let (w, last) = (self.inner + 2, total - 1);
        let divider = Self::FILM_TOP + film_h + 3;
        let lines = smooth(0.16, 0.7, p);
        let cross = smooth(0.55, 0.85, p);
        let corner_at = [
            (0, 0, 0.0),
            (0, w - 1, 0.04),
            (last, w - 1, 0.08),
            (last, 0, 0.12),
        ];
        let half = |n: usize| n as f64 / 2.0;
        let mut rows = Vec::with_capacity(total);
        for r in 0..total {
            if (r == 0 && p >= 0.78) || (r > divider && r < last && p >= 0.92) {
                rows.push(self.rows(&vec![" ".repeat(self.inner); film_h], false)[r].clone());
                continue;
            }
            let mut row = String::new();
            for col in 0..w {
                let edge = col == 0 || col == w - 1;
                let from_side = col.min(w - 1 - col) as f64;
                let from_end = r.min(last - r) as f64;
                let corner = corner_at.iter().find(|(y, x, _)| *y == r && *x == col);
                let cell: Option<(char, String)> = if let Some((_, _, at)) = corner {
                    if p < 0.72 {
                        (p >= *at).then(|| ('+', c.fg(CLAY)))
                    } else {
                        let ch = match (r == 0, col == 0) {
                            (true, true) => '╭',
                            (true, false) => '╮',
                            (false, true) => '╰',
                            (false, false) => '╯',
                        };
                        Some((ch, c.fg(STEEL)))
                    }
                } else if (r == 0 || r == last) && lines * half(w) >= from_side {
                    Some(('─', c.fg(STEEL)))
                } else if edge && r != divider && lines * half(total) >= from_end {
                    Some(('│', c.fg(STEEL)))
                } else if r == divider && cross > 0.0 && (edge || cross * half(w) >= from_side) {
                    let ch = if col == 0 {
                        '├'
                    } else if col == w - 1 {
                        '┤'
                    } else {
                        '─'
                    };
                    Some((ch, c.fg(STEEL)))
                } else {
                    None
                };
                match cell {
                    Some((ch, style)) => row.push_str(&format!("{style}{ch}{reset}")),
                    None => row.push(' '),
                }
            }
            rows.push(row);
        }
        rows
    }
}

const TAGLINE: [&str; 3] = ["Precisely compute ", "only what changed", "."];

/// The terminal's (columns, rows), or `None` when it cannot be read.
#[cfg(unix)]
fn terminal_size() -> Option<(usize, usize)> {
    // SAFETY: `winsize` is a plain C struct of integers, so all zeroes is a valid value.
    let mut ws: libc::winsize = unsafe { std::mem::zeroed() };
    // SAFETY: TIOCGWINSZ writes one `winsize` through the pointer, which points
    // to `ws`, a live, writable `winsize` that we own for the whole call.
    let ok = unsafe { libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &mut ws) } == 0;
    (ok && ws.ws_col > 0 && ws.ws_row > 0).then_some((ws.ws_col as usize, ws.ws_row as usize))
}

/// Not read on other platforms, so the film never plays there.
#[cfg(not(unix))]
fn terminal_size() -> Option<(usize, usize)> {
    None
}

/// Width inside the window's borders for a terminal `cols` wide.
fn inner_width(cols: usize) -> usize {
    cols.saturating_sub(4).clamp(60, 100)
}

/// Whether the animated window fits: the window, plus one row for the cursor
/// under it. In a shorter terminal the cursor moves that redraw the film stop
/// at the top of the screen, and frames land on the wrong rows.
fn fits(cols: usize, rows: usize) -> bool {
    let inner = inner_width(cols);
    cols >= inner + 4 && rows > Window::FILM_TOP + Film::rows(inner) + Window::BELOW_FILM
}

/// Terminal settings from before the film, for the signal handler to restore.
#[cfg(unix)]
static SAVED_TERMIOS: std::sync::OnceLock<libc::termios> = std::sync::OnceLock::new();

/// Signals that end fda while the film plays. SIGKILL cannot be caught.
#[cfg(unix)]
const RESTORE_ON: [libc::c_int; 4] = [libc::SIGTERM, libc::SIGHUP, libc::SIGQUIT, libc::SIGINT];

/// Restores the terminal and the cursor, then lets the signal end fda as usual.
#[cfg(unix)]
extern "C" fn restore_and_reraise(sig: libc::c_int) {
    const SHOW: &[u8] = b"\x1b[0m\x1b[?25h\r\n";
    // SAFETY: tcsetattr, write, signal and raise are async-signal-safe. The
    // termios pointer comes from a static set before this handler was installed,
    // and SHOW is a static buffer valid for SHOW.len() bytes.
    unsafe {
        if let Some(t) = SAVED_TERMIOS.get() {
            libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, t);
        }
        libc::write(libc::STDOUT_FILENO, SHOW.as_ptr().cast(), SHOW.len());
        libc::signal(sig, libc::SIG_DFL);
        libc::raise(sig);
    }
}

/// Puts stdin in non-canonical, no-echo mode so any key (Ctrl-C too) skips
/// the film. Restores the terminal on drop, and on the signals in `RESTORE_ON`.
struct Keys {
    #[cfg(unix)]
    saved: Option<libc::termios>,
    #[cfg(unix)]
    old_handlers: Vec<(libc::c_int, libc::sighandler_t)>,
}

impl Keys {
    #[cfg(unix)]
    fn new() -> Self {
        let off = Keys {
            saved: None,
            old_handlers: Vec::new(),
        };
        // SAFETY: `termios` is a plain C struct of integers, so all zeroes is a valid value.
        let mut t: libc::termios = unsafe { std::mem::zeroed() };
        // SAFETY: tcgetattr writes one `termios` through the pointer, which points to `t`.
        if unsafe { libc::tcgetattr(libc::STDIN_FILENO, &mut t) } != 0 {
            return off;
        }
        let saved = t;
        // The film plays once per process, so this is the only value ever set.
        if SAVED_TERMIOS.set(saved).is_err() {
            return off;
        }
        let handler = restore_and_reraise as extern "C" fn(libc::c_int) as libc::sighandler_t;
        let old_handlers = RESTORE_ON
            .iter()
            // SAFETY: installs a handler that only makes async-signal-safe calls.
            .map(|&sig| (sig, unsafe { libc::signal(sig, handler) }))
            .collect();
        t.c_lflag &= !(libc::ICANON | libc::ECHO | libc::ISIG);
        t.c_cc[libc::VMIN] = 0;
        t.c_cc[libc::VTIME] = 0;
        // SAFETY: tcsetattr reads one `termios` through the pointer, which points to `t`.
        unsafe { libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &t) };
        Keys {
            saved: Some(saved),
            old_handlers,
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
        // SAFETY: `buf` is a live, writable buffer valid for `buf.len()` bytes,
        // and read writes at most that many. VMIN=0 and VTIME=0 make it return at once.
        let n = unsafe { libc::read(libc::STDIN_FILENO, buf.as_mut_ptr().cast(), buf.len()) };
        (n > 0).then(|| buf[..n as usize].to_vec())
    }

    #[cfg(not(unix))]
    fn pressed(&self) -> Option<Vec<u8>> {
        None
    }

    /// Ask the terminal for its background color (OSC 11) and wait up to
    /// 150 ms for the reply. Terminals that do not support it stay silent.
    fn background(&self, out: &mut impl Write) -> Option<Theme> {
        #[cfg(unix)]
        self.saved?;
        #[cfg(not(unix))]
        return None;
        let _ = write!(out, "\x1b]11;?\x1b\\");
        let _ = out.flush();
        let deadline = Instant::now() + Duration::from_millis(150);
        let mut reply = Vec::new();
        while Instant::now() < deadline {
            match self.pressed() {
                Some(bytes) => {
                    reply.extend(bytes);
                    if let Some(theme) = Theme::from_osc11(&reply) {
                        return Some(theme);
                    }
                }
                None => std::thread::sleep(Duration::from_millis(5)),
            }
        }
        None
    }
}

impl Drop for Keys {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Some(t) = self.saved {
            // SAFETY: tcsetattr reads one `termios` through the pointer, which
            // points to `t`, the attributes saved in `new`.
            unsafe { libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &t) };
            for &(sig, old) in &self.old_handlers {
                // SAFETY: puts back the handler that was in place before `new`.
                unsafe { libc::signal(sig, old) };
            }
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

/// Whether the environment allows animation. `env` looks up a variable.
fn animation_allowed(
    stdout_tty: bool,
    stdin_tty: bool,
    env: impl Fn(&str) -> Option<String>,
) -> bool {
    stdout_tty
        && stdin_tty
        && env("CI").is_none()
        && env("FDA_NO_ANIMATION").is_none()
        && env("TERM").is_none_or(|t| t != "dumb")
}

fn can_animate() -> bool {
    animation_allowed(
        stdout().is_terminal(),
        std::io::stdin().is_terminal(),
        |k| std::env::var_os(k).map(|v| v.to_string_lossy().into_owned()),
    )
}

/// Write the first-run marker in `dir`. True only the first time: false when
/// the marker is already there, or when it cannot be written.
fn claim_first_run(dir: &std::path::Path) -> bool {
    let marker = dir.join("welcome-shown");
    !marker.exists() && std::fs::create_dir_all(dir).is_ok() && std::fs::write(&marker, b"").is_ok()
}

/// Play the film in its window, or print the still window when animation is off.
/// `host` is the Feldera host fda talks to, shown in the CONNECTION panel.
pub fn play(host: &str) {
    let color = ColorMode::detect();
    let size = terminal_size();
    let inner = inner_width(size.map_or(80, |(cols, _)| cols));
    let env_theme = std::env::var("COLORFGBG")
        .ok()
        .and_then(|v| Theme::from_colorfgbg(&v));
    let mut film = Film::new(inner, color);
    let mut window = Window {
        inner,
        color,
        host: host.to_string(),
        muted: env_theme.unwrap_or(Theme::Dark).muted(),
    };
    let mut out = stdout().lock();
    // A still print scrolls, so it is safe at any size.
    if !can_animate() || !size.is_some_and(|(cols, rows)| fits(cols, rows)) {
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
    // Without COLORFGBG, ask the terminal for its background. Dark if it does not answer.
    let theme = env_theme
        .or_else(|| keys.background(&mut out))
        .unwrap_or(Theme::Dark);
    film.muted = theme.muted();
    window.muted = theme.muted();
    let mut interrupted = false;
    let mut skipped = false;

    // Draw the empty window, then redraw only the film rows in place.
    let total = Window::FILM_TOP + film.h + Window::BELOW_FILM;
    let blank_film = vec![" ".repeat(inner); film.h];
    let _ = write!(out, "\x1b[?25l");
    // The window draws itself before the film starts.
    let _ = write!(out, "{}", "\n".repeat(total));
    let start = Instant::now();
    let n = (INTRO * FPS) as usize;
    for i in 0..=n {
        if let Some(bytes) = keys.pressed() {
            skipped = true;
            interrupted = bytes.contains(&0x03);
            break;
        }
        let rows = window.intro_rows(film.h, i as f64 / n as f64);
        let _ = writeln!(out, "\x1b[{total}F\x1b[2K{}", rows.join("\n\x1b[2K"));
        let _ = out.flush();
        let due = start + Duration::from_secs_f64((i + 1) as f64 / FPS);
        std::thread::sleep(due.saturating_duration_since(Instant::now()));
    }
    let _ = write!(out, "\x1b[{total}F");
    for row in window.rows(&blank_film, false) {
        let _ = writeln!(out, "\x1b[2K{row}");
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
        if skipped {
            break;
        }
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

    // One final wingbeat once everything is in place.
    if !skipped && !interrupted {
        reel.sparks.clear();
        let n = (FLAP * FPS) as usize;
        let start = Instant::now();
        for i in 0..=n {
            if let Some(bytes) = keys.pressed() {
                interrupted = bytes.contains(&0x03);
                break;
            }
            draw(&mut out, &reel.flap_frame(&film, i as f64 / n as f64));
            let due = start + Duration::from_secs_f64((i + 1) as f64 / FPS);
            std::thread::sleep(due.saturating_duration_since(Instant::now()));
        }
        draw(&mut out, &film.final_frame());
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
/// `reads_stdin` is true when the command reads its input from stdin; the film
/// would take the first keys typed for that input as its skip key.
pub fn first_run(host: &str, reads_stdin: bool) {
    // Too small for the window: wait for a larger terminal rather than garble it.
    let fits_now = terminal_size().is_some_and(|(cols, rows)| fits(cols, rows));
    if reads_stdin || !can_animate() || !fits_now {
        return;
    }
    let Some(dirs) = directories::ProjectDirs::from("com", "Feldera", "fda") else {
        return;
    };
    // Claim it first, so a failed or interrupted run does not play it again.
    if claim_first_run(dirs.config_dir()) {
        play(host);
    }
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
                muted: STEEL,
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
    fn animation_needs_two_terminals_and_a_clear_environment() {
        let env = |vars: &'static [(&'static str, &'static str)]| {
            move |k: &str| {
                vars.iter()
                    .find(|(n, _)| *n == k)
                    .map(|(_, v)| v.to_string())
            }
        };
        assert!(animation_allowed(true, true, env(&[])));
        assert!(animation_allowed(
            true,
            true,
            env(&[("TERM", "xterm-256color")])
        ));
        assert!(!animation_allowed(true, true, env(&[("CI", "true")])));
        assert!(!animation_allowed(
            true,
            true,
            env(&[("FDA_NO_ANIMATION", "1")])
        ));
        assert!(!animation_allowed(true, true, env(&[("TERM", "dumb")])));
        assert!(
            !animation_allowed(false, true, env(&[])),
            "stdout is a pipe"
        );
        assert!(!animation_allowed(true, false, env(&[])), "stdin is a pipe");
    }

    #[test]
    fn window_must_fit_the_terminal() {
        // The window is 30 rows at 80 columns, so a default 80x24 terminal is too short.
        let need = Window::FILM_TOP + Film::rows(inner_width(80)) + Window::BELOW_FILM;
        assert_eq!(need, 30);
        assert!(!fits(80, 24));
        assert!(!fits(80, need), "no row left for the cursor");
        assert!(fits(80, need + 1));
        assert!(fits(80, 40));
        assert!(!fits(63, 60), "too narrow");
        assert!(fits(64, 60));
    }

    #[test]
    fn first_run_marker_is_claimed_once() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("fda");
        assert!(claim_first_run(&config), "first run plays");
        assert!(config.join("welcome-shown").exists());
        assert!(!claim_first_run(&config), "second run does not");

        // A file where the config directory should be: the marker cannot be written.
        let blocked = dir.path().join("blocked");
        std::fs::write(&blocked, b"").unwrap();
        assert!(!claim_first_run(&blocked.join("fda")));
    }

    /// The plain separable filter, to check the fast one against.
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

    /// WCAG contrast ratio between two sRGB colors.
    fn contrast(a: Rgb, b: Rgb) -> f64 {
        let lum = |(r, g, b): Rgb| {
            let ch = |v: u8| {
                let c = v as f64 / 255.0;
                if c <= 0.03928 {
                    c / 12.92
                } else {
                    ((c + 0.055) / 1.055).powf(2.4)
                }
            };
            0.2126 * ch(r) + 0.7152 * ch(g) + 0.0722 * ch(b)
        };
        let (x, y) = (lum(a), lum(b));
        (x.max(y) + 0.05) / (x.min(y) + 0.05)
    }

    /// The RGB value an xterm 256-color index shows.
    fn xterm_rgb(i: u8) -> Rgb {
        const LEVELS: [u8; 6] = [0, 95, 135, 175, 215, 255];
        match i {
            16..=231 => {
                let i = i - 16;
                (
                    LEVELS[(i / 36) as usize],
                    LEVELS[(i / 6 % 6) as usize],
                    LEVELS[(i % 6) as usize],
                )
            }
            232..=255 => {
                let g = 8 + 10 * (i - 232);
                (g, g, g)
            }
            _ => panic!("not a cube or gray index: {i}"),
        }
    }

    #[test]
    fn brand_colors_survive_256_color_terminals() {
        let dist = |a: Rgb, b: Rgb| {
            let d = |x: u8, y: u8| (x as i32 - y as i32).pow(2);
            d(a.0, b.0) + d(a.1, b.1) + d(a.2, b.2)
        };
        let brand = [
            VIOLET,
            STEEL,
            LICHEN,
            CLAY,
            STONE,
            LICHEN_TINT,
            LICHEN_SHADE,
            INK,
        ];
        for c in brand {
            // The pick is the nearest of all 240 cube and gray entries.
            let best = (16..=255u8).map(|i| dist(c, xterm_rgb(i))).min().unwrap();
            assert_eq!(
                dist(c, xterm_rgb(ansi256(c))),
                best,
                "{c:?} is not mapped to its nearest entry"
            );
        }
        let tint = xterm_rgb(ansi256(LICHEN_TINT));
        assert!(tint.2 < tint.0, "Lichen tint turned neutral gray: {tint:?}");
        let violet = xterm_rgb(ansi256(VIOLET));
        assert!(
            violet.2 > violet.0 + 40,
            "Violet turned lavender or gray: {violet:?}"
        );
        // The tagline band keeps its contrast: Ink text on the Lichen tint band.
        let band = contrast(xterm_rgb(ansi256(INK)), tint);
        assert!(band >= 7.0, "tagline band contrast {band:.1}");
    }

    #[test]
    fn secondary_text_reads_on_both_themes() {
        const PAPER: Rgb = (0xFA, 0xF9, 0xF6);
        const ONYX: Rgb = (0x0F, 0x10, 0x0E);
        let light = contrast(Theme::Light.muted(), PAPER);
        let dark = contrast(Theme::Dark.muted(), ONYX);
        assert!(light >= 4.5, "light theme: {light:.1}");
        assert!(dark >= 4.5, "dark theme: {dark:.1}");
    }

    #[test]
    fn terminal_theme_is_read_from_the_terminal() {
        assert_eq!(Theme::from_colorfgbg("15;0"), Some(Theme::Dark));
        assert_eq!(Theme::from_colorfgbg("0;15"), Some(Theme::Light));
        assert_eq!(Theme::from_colorfgbg("0;default;7"), Some(Theme::Light));
        assert_eq!(Theme::from_colorfgbg("nonsense"), None);
        let white = b"\x1b]11;rgb:ffff/ffff/ffff\x1b\\";
        let onyx = b"\x1b]11;rgb:0f0f/1010/0e0e\x07";
        assert_eq!(Theme::from_osc11(white), Some(Theme::Light));
        assert_eq!(Theme::from_osc11(onyx), Some(Theme::Dark));
        assert_eq!(Theme::from_osc11(b"\x1b]11;rgb:ff"), None, "cut-off reply");
        assert_eq!(Theme::from_osc11(b"x"), None);
    }

    #[test]
    fn final_wingbeat_starts_and_ends_on_the_logo() {
        assert_eq!(flap_k(0.0), 1.0);
        assert_eq!(flap_k(1.0), 1.0);
        let ks: Vec<f64> = (0..=100).map(|i| flap_k(i as f64 / 100.0)).collect();
        let top = ks.iter().cloned().fold(f64::MIN, f64::max);
        let low = ks.iter().cloned().fold(f64::MAX, f64::min);
        assert!((top - FLAP_TOP).abs() < 1e-9, "no wind-up: {top}");
        assert!((low - FLAP_LOW).abs() < 1e-9, "no downstroke: {low}");
        // Smooth: no jump between frames bigger than a fifth of the stroke.
        let jump = ks
            .windows(2)
            .map(|w| (w[1] - w[0]).abs())
            .fold(0.0, f64::max);
        assert!(jump < 0.2 * (FLAP_TOP - FLAP_LOW), "jerky beat: {jump}");

        // Every point of both wings, shoulder to tip, starts and ends in the logo pose.
        for (delay, lag) in [(0.0, NEAR_LAG), (FAR_DELAY, FAR_LAG)] {
            for u in [0.0, 0.25, 0.5, 0.75, 1.0] {
                assert_eq!(flap_k_at(0.0, delay, lag, u), 1.0);
                assert_eq!(
                    flap_k_at(1.0, delay, lag, u),
                    1.0,
                    "u={u} not at rest at the end"
                );
            }
        }

        // The last frame of the beat draws the logo exactly.
        let film = Film::new(80, ColorMode::Off);
        let mut reel = Reel::new(3);
        let last = reel.flap_frame(&film, 1.0);
        let plain = |f: &[String]| f.iter().map(|l| strip(l)).collect::<Vec<_>>();
        let end = plain(&last);
        let still = plain(&film.final_frame());
        let differ: usize = end
            .iter()
            .zip(&still)
            .map(|(a, b)| a.chars().zip(b.chars()).filter(|(x, y)| x != y).count())
            .sum();
        // The moving bird is drawn by sampling, the still one by Lanczos: a few
        // edge cells may differ, nothing more.
        assert!(differ <= 12, "{differ} cells differ from the still logo");
    }

    #[test]
    fn bird_hovers_then_settles_onto_the_words() {
        let film = Film::new(80, ColorMode::Off);
        let mut reel = Reel::new(11);
        // Body height (dots, negative is up), pitch (rad, negative is nose up)
        // and tail bend (rad, negative is down) through the landing.
        let (mut high, mut nose_up) = (0.0f64, 0.0f64);
        // From the descent on: the most nose-down pitch, and after touchdown
        // the lowest the body sinks below the perch.
        let (mut nose_down, mut dip) = (f64::MIN, f64::MIN);
        let (mut tail_down, mut tail_flick) = (0.0f64, f64::MIN);
        // The descent: its frames' heights, in order.
        let mut glide = Vec::new();
        let mut at_rest = None;
        let mut closed = None;
        // The wing phase where the descent starts and at touchdown.
        let (mut descent_from, mut at_touch) = (None, 0.0);
        let mut descent_amp = f64::MAX;
        for i in 0..Reel::frame_count() {
            let t = i as f64 / FPS * SPEED;
            reel.frame(&film, i);
            let (y, pitch, bend) = reel.last_pose;
            let u = (t - HERO) / (LAND - HERO);
            if (0.86..1.0).contains(&u) {
                high = high.min(y);
                nose_up = nose_up.min(pitch);
                tail_down = tail_down.min(bend);
            }
            if u >= DESCEND_FROM && t <= LAND + SETTLED {
                nose_down = nose_down.max(pitch);
            }
            if u >= DESCEND_FROM && t <= LAND {
                glide.push(y);
                descent_from.get_or_insert(reel.phase);
                at_touch = reel.phase;
                if u >= 0.92 {
                    descent_amp = descent_amp.min(reel.amp);
                }
            }
            if t > LAND && t <= LAND + SETTLED {
                dip = dip.max(y);
                tail_flick = tail_flick.max(bend);
                at_rest = Some((y, pitch, bend));
            }
            if t > LAND + FOLD && closed.is_none() {
                closed = Some(reel.phase);
            }
        }
        assert!(high <= -1.5, "no hover above the perch: highest {high:.2}");
        assert!(
            nose_up >= -0.05,
            "body tips back on the descent: {nose_up:.2}"
        );
        // A gentle landing: the body eases level and never rocks forward
        // past it, and does not sink below the perch.
        assert!(nose_down <= 0.02, "body rocks forward: {nose_down:.3}");
        assert!(dip <= 0.05, "body dips on touchdown: {dip:.2}");
        // The bird lowers slowly and evenly: from the top of the hover, down
        // every frame by no more than half a dot, to the perch.
        let top = (0..glide.len())
            .min_by(|&a, &b| glide[a].total_cmp(&glide[b]))
            .unwrap();
        assert!(glide[top] <= -2.0, "descent starts low: {:.2}", glide[top]);
        assert!(top <= 2, "descent rises for {top} frames");
        let steps: Vec<f64> = glide[top..].windows(2).map(|w| w[1] - w[0]).collect();
        assert!(
            steps.iter().all(|d| (-1e-9..=0.5).contains(d)),
            "descent is not a slow glide: {steps:.2?}"
        );
        // One full, wide wingbeat lowers the bird: about one turn of the
        // stroke, not the quick flutter of the hover.
        let beats = (at_touch - descent_from.unwrap()) / (2.0 * PI);
        assert!(
            (0.9..2.0).contains(&beats),
            "descent is {beats:.2} beats, not one"
        );
        assert!(
            descent_amp >= 0.7,
            "descent beat too shallow: {descent_amp:.2}"
        );
        // The tail lowers on the way down and eases back without a flick up.
        assert!(tail_down <= -0.1, "tail does not lower: {tail_down:.3}");
        assert!(
            tail_flick <= 1e-3,
            "tail flicks up on touchdown: {tail_flick:.3}"
        );
        let (y, pitch, bend) = at_rest.unwrap();
        assert!(
            y.abs() < 0.3 && pitch.abs() < 0.03 && bend.abs() < 0.03,
            "not settled: y {y:.2}, pitch {pitch:.3}, tail {bend:.3}"
        );
        // The wings close on a whole stroke: phase is a multiple of 2 pi, the logo pose.
        let turns = closed.unwrap() / (2.0 * PI);
        assert!(
            (turns - turns.round()).abs() < 1e-9,
            "wings stop mid-stroke: {turns}"
        );
    }

    #[test]
    fn tail_sways_in_flight_and_is_still_on_landing() {
        let film = Film::new(80, ColorMode::Off);
        let mut reel = Reel::new(5);
        let (mut left, mut right, mut landing) = (0.0f64, 0.0f64, 0.0f64);
        for i in 0..Reel::frame_count() {
            let t = i as f64 / FPS * SPEED;
            reel.frame(&film, i);
            let u = (t - HERO) / (LAND - HERO);
            if (0.0..0.7).contains(&u) {
                left = left.min(reel.last_sway);
                right = right.max(reel.last_sway);
            }
            if u >= DESCEND_FROM && t <= LAND + SETTLED {
                landing = landing.max(reel.last_sway.abs());
            }
        }
        // The tip moves both ways along the tail, by about TAIL_SWAY.
        assert!(
            left <= -0.8 * TAIL_SWAY && right >= 0.8 * TAIL_SWAY,
            "tail does not sway: {left:.3}..{right:.3}"
        );
        assert!(landing < 1e-9, "tail still sways on landing: {landing:.3}");
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
