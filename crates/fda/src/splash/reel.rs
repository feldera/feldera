//! The motion of the bird. This file computes each frame of the film and of
//! the final wingbeat.
//!
//! The film has these steps, in seconds from its first frame:
//!
//! | From | Step |
//! |---|---|
//! | `APPROACH_START` | The approach. The bird flies at the camera until `APPROACH_END`. |
//! | `PASS_START` | The side-on pass. The bird flies along the flight path to the perch. |
//! | `SWEEP_START` | The Violet band starts to reveal the wordmark. |
//! | `TOUCHDOWN` | The feet touch the wordmark. The body settles. |
//! | `FILM_END` | The last frame, which shows the landed logo. |

use std::f64::consts::PI;

use super::color::{Rgb, STEEL};
use super::film::{FEET, Film, Particle, SPARK_COLORS, STREAK_COLORS, Shot, Word, svg_px};
use super::{FPS, Pt, smooth};

// The timeline of the film, in seconds from its first frame. The table at the
// top of this file describes each step.
const APPROACH_START: f64 = 0.35;
const APPROACH_END: f64 = 2.3;
const PASS_START: f64 = 2.55;
/// The Violet band starts to cross the wordmark. The band takes `SWEEP_SECS`
/// to cross.
const SWEEP_START: f64 = 5.15;
const SWEEP_SECS: f64 = 0.95;
const TOUCHDOWN: f64 = 5.35;
const FILM_END: f64 = 6.6;

// The landing. The progress of the side-on pass goes from 0 at its start to 1 at touchdown.
/// The progress at which the flare starts. In the flare the bird tilts back to
/// brake. From this progress on, `Springs` moves the body.
const LANDING_FROM: f64 = 0.74;
/// The progress at which the bird starts to descend from its hover.
const DESCEND_FROM: f64 = 0.85;
/// The height of the hover above the perch, in dots, and the width of the
/// wingbeat in the hover.
const HOVER_HEIGHT: f64 = 2.5;
const HOVER_AMP: f64 = 0.4;
/// The width of the one wingbeat of the descent, where 1 is a full wingbeat.
const DESCEND_AMP: f64 = 0.85;
/// The time after touchdown in which the wings complete their wingbeat.
const FOLD_SECS: f64 = 0.35;
/// The time after touchdown at which the frames start to show the landed logo.
const SETTLED_SECS: f64 = 0.55;
/// The springs of the hover for the height and the pitch, each as a natural frequency in
/// rad/s (`_W`) and a damping ratio (`_Z`). A ratio below 1 makes the body overshoot a little.
const HEIGHT_W: f64 = 10.0;
const HEIGHT_Z: f64 = 0.5;
const PITCH_W: f64 = 8.0;
const PITCH_Z: f64 = 0.45;
/// The height spring after touchdown. A damping ratio of 1 prevents a bounce. From the start
/// of the descent, the pitch spring has the same ratio, so that the body does not rock.
const SETTLE_W: f64 = 9.0;
const SETTLE_Z: f64 = 1.0;
/// The upward acceleration of the body from the downstroke of a full wingbeat,
/// in dots/s².
const WING_PUSH: f64 = 1200.0;
/// The tail spring. In the descent the tail bends by `TAIL_BRAKE` radians, where negative is
/// down. A damping ratio of 1 prevents a swing above level after touchdown.
const TAIL_W: f64 = 9.0;
const TAIL_Z: f64 = 1.0;
const TAIL_BRAKE: f64 = -0.18;
/// The tail sway in flight. In the side view, the tip moves left and right by the fraction
/// `TAIL_SWAY` of the tail length, `TAIL_SWAY_HZ` times per second.
const TAIL_SWAY: f64 = 0.22;
const TAIL_SWAY_HZ: f64 = 1.4;

// The steps of the timeline are in order. The Violet band is past the wordmark
// and the bird is the landed logo before the film ends.
const _: () = assert!(
    APPROACH_START < APPROACH_END
        && APPROACH_END < PASS_START
        && PASS_START < SWEEP_START
        && SWEEP_START < TOUCHDOWN
        && TOUCHDOWN + SETTLED_SECS < FILM_END
        && SWEEP_START + SWEEP_SECS <= FILM_END
);
// The flare, the descent and touchdown are in order. The wings stop before the
// frames show the landed logo.
const _: () =
    assert!(LANDING_FROM < DESCEND_FROM && DESCEND_FROM < 1.0 && FOLD_SECS < SETTLED_SECS);

/// The highest and the lowest wing position k of the final wingbeat. `Wing`
/// defines k.
const FLAP_TOP: f64 = 1.15;
const FLAP_LOW: f64 = -0.65;
/// The delays in the final wingbeat, as fractions of its duration. The tip of each wing is
/// behind its hinge by a `_LAG` value, and the far wing starts `FAR_DELAY` late.
const NEAR_LAG: f64 = 0.12;
const FAR_LAG: f64 = 0.1;
const FAR_DELAY: f64 = 0.04;
/// The phase, in radians, by which the far wing is behind the near wing in
/// flight.
const FAR_PHASE: f64 = 0.25;

/// Returns `cur` moved by the fraction `k` of the distance to `target`.
fn lowpass(cur: f64, target: f64, k: f64) -> f64 {
    cur + (target - cur) * k
}

/// Returns the acceleration of a damped spring with the natural frequency `w` in rad/s and
/// the damping ratio `z`.
fn spring(pos: f64, vel: f64, target: f64, w: f64, z: f64) -> f64 {
    w * w * (target - pos) - 2.0 * z * w * vel
}

/// Evaluates a cubic curve at `x`, from 0 to 1. The curve starts at `y0` with
/// the slope `m0`, and ends at 0 with the slope 0. Returns (value, slope).
fn ease_to_zero(y0: f64, m0: f64, x: f64) -> (f64, f64) {
    (
        y0 * (2.0 * x.powi(3) - 3.0 * x * x + 1.0) + m0 * (x.powi(3) - 2.0 * x * x + x),
        y0 * (6.0 * x * x - 6.0 * x) + m0 * (3.0 * x * x - 4.0 * x + 1.0),
    )
}

/// Returns the wing position k of the final wingbeat at the progress `tau`, from 0 to 1. The
/// wing rises to `FLAP_TOP`, moves down fast to `FLAP_LOW`, and returns slowly to the logo pose.
fn flap_k(tau: f64) -> f64 {
    match tau {
        t if t <= 0.0 => 1.0,
        t if t < 0.28 => 1.0 + (FLAP_TOP - 1.0) * smooth(0.0, 1.0, t / 0.28),
        t if t < 0.52 => FLAP_TOP + (FLAP_LOW - FLAP_TOP) * smooth(0.0, 1.0, (t - 0.28) / 0.24),
        t if t < 1.0 => {
            let x = (t - 0.52) / 0.48;
            FLAP_LOW + (1.0 - FLAP_LOW) * (1.0 - (1.0 - x).powi(3))
        }
        _ => 1.0,
    }
}

/// Returns the wing position k at `tau` for a wing point with the tip fraction `u`. The point
/// starts `delay + lag * u` late. `tau` is scaled by `1 + delay + lag`, so that each point is
/// in the logo pose again at `tau` = 1.
fn flap_k_at(tau: f64, delay: f64, lag: f64, u: f64) -> f64 {
    flap_k(tau * (1.0 + delay + lag) - delay - lag * u)
}

/// A xorshift random number generator. The particles need only noise, so a
/// cryptographic generator is not necessary.
struct Rng(u64);

impl Rng {
    /// Returns a random number in [0, 1).
    fn next(&mut self) -> f64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Returns a random number in [lo, hi).
    fn range(&mut self, lo: f64, hi: f64) -> f64 {
        lo + (hi - lo) * self.next()
    }

    /// Returns a random index below `len`.
    fn index(&mut self, len: usize) -> usize {
        ((self.next() * len as f64) as usize).min(len - 1)
    }
}

/// The dots of the bird in one frame. The fields have the same meaning as in
/// `Shot`.
struct Bird {
    body: Vec<bool>,
    far: Vec<bool>,
    color: Option<Rgb>,
    landed: bool,
}

/// The pose of the body in one frame of the side-on pass.
#[derive(Clone, Copy, Default)]
struct Pose {
    /// The offset of the body from the flight path, in dots. Negative is up.
    lift: f64,
    /// The pitch of the body, in radians. Negative is nose up.
    tilt: f64,
    /// The bend of the tail, in radians. Negative is down.
    bend: f64,
    /// The sway of the tail, as a fraction of the tail length.
    sway: f64,
}

/// A scripted change of the wingbeat phase from `from` to `to`. The change
/// starts at the time `start`, in seconds.
#[derive(Clone, Copy)]
struct PhaseRamp {
    from: f64,
    to: f64,
    start: f64,
}

/// The state of the body in the landing. Springs move the height, the pitch
/// and the tail bend.
struct Springs {
    /// The height above the perch, in dots, and its velocity. Negative is up.
    y: f64,
    vy: f64,
    /// The pitch, in radians, and its velocity. Negative is nose up.
    pitch: f64,
    vpitch: f64,
    /// The tail bend, in radians, and its velocity. Negative is down.
    bend: f64,
    vbend: f64,
    /// The height and the velocity at the start of the descent.
    glide_from: Option<(f64, f64)>,
}

/// The state of the animation that changes from frame to frame.
pub(super) struct Reel {
    rng: Rng,
    /// The particles. Sparks and dust start at the bird. Streaks start near the
    /// vanishing point.
    sparks: Vec<Particle>,
    streaks: Vec<Particle>,
    /// The phase of the wingbeat, in radians, and the width of the wingbeat,
    /// from 0 to 1. A phase that is a multiple of 2 pi is the top of a wingbeat.
    phase: f64,
    amp: f64,
    /// The tilt of the body in flight, in radians. The tilt follows the heading
    /// of the bird with a delay.
    rot: f64,
    /// The position of the bird in the frame before, in the side-on pass. The
    /// velocity of the bird comes from this position.
    prev_pos: Option<Pt>,
    /// The phase of the tail sway, in radians.
    sway_phase: f64,
    /// The pose of the body in the frame before, in the side-on pass.
    pose: Pose,
    /// The state of the landing. `None` before the flare.
    landing: Option<Springs>,
    /// The wingbeat of the descent, and the completion of the wingbeat after
    /// touchdown. Each is `None` until it starts.
    descent: Option<PhaseRamp>,
    fold: Option<PhaseRamp>,
    /// The wing points of the current frame. The buffers avoid an allocation per frame.
    near_buf: Vec<Pt>,
    far_buf: Vec<Pt>,
}

/// The duration of a frame, in seconds.
const DT: f64 = 1.0 / FPS;

/// Returns the first phase at or after `phase` that is the top of a wingbeat.
fn next_rest(phase: f64) -> f64 {
    (phase / (2.0 * PI)).ceil() * 2.0 * PI
}

impl Reel {
    pub(super) fn new(seed: u64) -> Self {
        Reel {
            rng: Rng(seed | 1),
            sparks: Vec::new(),
            streaks: Vec::new(),
            phase: 0.0,
            amp: 1.0,
            rot: 0.0,
            prev_pos: None,
            sway_phase: 0.0,
            pose: Pose::default(),
            landing: None,
            descent: None,
            fold: None,
            near_buf: Vec::new(),
            far_buf: Vec::new(),
        }
    }

    pub(super) fn frame_count() -> usize {
        (FILM_END * FPS) as usize + 1
    }

    /// Returns frame `i` of the film. Call it for each frame in order, because
    /// each call advances the state.
    pub(super) fn frame(&mut self, film: &Film, i: usize) -> Vec<String> {
        let t = i as f64 / FPS;
        let (bird, word) = if (APPROACH_START..APPROACH_END).contains(&t) {
            (self.approach(film, t), Word::Hidden)
        } else if t >= PASS_START {
            let sweep = (t - SWEEP_START) / SWEEP_SECS;
            (self.side_pass(film, t), Word::Sweep(sweep))
        } else {
            let empty = vec![false; film.hd * film.wd];
            let bird = Bird {
                body: empty.clone(),
                far: empty,
                color: None,
                landed: false,
            };
            (bird, Word::Hidden)
        };
        self.step_streaks();
        self.step_sparks(1.0);
        let shot = Shot {
            body: &bird.body,
            far: &bird.far,
            bird_color: bird.color,
            landed: bird.landed,
            word,
            // The registration marks appear one after the other, 0.08 s apart.
            marks: (0..4).filter(|k| t >= 0.05 + 0.08 * *k as f64).count(),
            hint: t < TOUCHDOWN,
        };
        film.compose(&shot, &self.sparks, &self.streaks)
    }

    /// Returns the bird for a frame of the approach. The bird flies from the vanishing point at
    /// the camera and breaks into dots as it passes. This function also starts the streaks.
    fn approach(&mut self, film: &Film, t: f64) -> Bird {
        let (hd, wd) = (film.hd as f64, film.wd as f64);
        // The distance to the bird decreases at a constant rate, so the scale of
        // the bird grows as 1/distance.
        let p = (t - APPROACH_START) / (APPROACH_END - APPROACH_START);
        let sc = 0.24 / (1.0 - 0.945 * p);
        let k = (sc - 0.24) / 4.1;
        let pos = [film.vp[0] - 0.55 * wd * k, film.vp[1] - 0.28 * hd * k];
        self.phase += 2.0 * PI * 2.6 / FPS;
        film.near.pose(self.phase, 1.0, &mut self.near_buf);
        film.far
            .pose(self.phase + FAR_PHASE, 1.0, &mut self.far_buf);
        let mut body = film.raster(film.statics.iter().chain(&self.near_buf), 0.0, sc, pos);
        let mut far = film.raster(self.far_buf.iter(), 0.0, sc, pos);
        if sc > 3.0 {
            // The bird passes the camera. Remove more of its dots at random as
            // it grows.
            for d in body.iter_mut().chain(far.iter_mut()) {
                *d = *d && self.rng.next() > sc - 3.0;
            }
        }

        // More streaks start as the bird comes near. Each streak starts on an
        // ellipse around the vanishing point and moves away from that point.
        let new_streaks = (self.rng.next() < 0.5 + p) as usize + (4.0 * p.powi(3)) as usize;
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
        Bird {
            body,
            far,
            // The far bird is Steel, which makes it look distant.
            color: (sc < 0.55).then_some(STEEL),
            landed: false,
        }
    }

    /// Returns the bird for a frame of the side-on pass. The bird flies along the flight path,
    /// flares, hovers above the perch and descends to the wordmark.
    fn side_pass(&mut self, film: &Film, t: f64) -> Bird {
        let u = ((t - PASS_START) / (TOUCHDOWN - PASS_START)).min(1.0);
        // The bird covers distance fast at the start and slows down near the perch.
        let dist = 1.0 - (1.0 - u).powf(2.3);
        let pos = film.path.at(dist);
        let vel = self.prev_pos.map_or([0.0, 0.0], |q| {
            [(pos[0] - q[0]) * FPS, (pos[1] - q[1]) * FPS]
        });
        self.prev_pos = Some(pos);
        let climb = (-vel[1] / 60.0).clamp(-1.0, 1.0);
        // The weight of the flare and the weight of the hover, each from 0 to 1.
        // The hover replaces the flare.
        let hover = smooth(0.88, 0.94, u);
        let flare = smooth(LANDING_FROM, 0.86, u) * (1.0 - hover);

        self.beat_wings(t, u, climb, flare, hover);

        // In flight the body tilts toward its heading, with a delay.
        let heading = vel[1].atan2(vel[0].abs().max(1e-3));
        let travel = (1.0 - flare) * (1.0 - hover);
        let rot_target = (0.5 * heading).clamp(-0.35, 0.35) * travel - 0.3 * flare - 0.08 * hover;
        self.rot = lowpass(self.rot, rot_target, 0.18);

        let sc = 1.0 + 0.45 * (1.0 - dist);
        let (lift, tilt, bend) = if u < LANDING_FROM {
            // In flight the body moves up and down with each wingbeat.
            (-2.2 * sc * self.phase.sin() * self.amp, self.rot, 0.0)
        } else {
            self.land(t, u, hover, rot_target)
        };
        // The tail sways in flight. The sway fades before the descent starts.
        self.sway_phase += 2.0 * PI * TAIL_SWAY_HZ * DT;
        let sway = TAIL_SWAY * (1.0 - smooth(0.7, DESCEND_FROM, u)) * self.sway_phase.sin();
        let pose = Pose {
            lift,
            tilt,
            bend,
            sway,
        };
        self.pose = pose;
        if t > TOUCHDOWN + SETTLED_SECS {
            return Bird {
                body: film.final_bird.clone(),
                far: film.final_far.clone(),
                color: None,
                landed: true,
            };
        }

        // In flight the body tilts about `anchor`. In the landing it tilts about
        // its feet. `w` blends the two pivots, so that the bird does not jump.
        let feet = svg_px(FEET);
        let k = sc * film.scale;
        let v = [
            (feet[0] - film.anchor[0]) * k,
            (feet[1] - film.anchor[1]) * k,
        ];
        let (s, c) = pose.tilt.sin_cos();
        let w = smooth(0.7, 0.8, u);
        let pivot = [
            (v[0] - (v[0] * c - v[1] * s)) * w,
            (v[1] - (v[0] * s + v[1] * c)) * w,
        ];
        let center = [pos[0] + pivot[0], pos[1] + pose.lift + pivot[1]];

        film.near.pose(self.phase, self.amp, &mut self.near_buf);
        film.far
            .pose(self.phase + FAR_PHASE, self.amp, &mut self.far_buf);
        let statics = film.body_points(pose.bend, pose.sway);
        let body = film.raster(statics.iter().chain(&self.near_buf), pose.tilt, sc, center);
        let far = film.raster(self.far_buf.iter(), pose.tilt, sc, center);
        // Sparks start at the wingtips in each downstroke. Fewer sparks start at
        // the tail.
        if self.amp > 0.3 && self.phase.sin() > 0.0 && u < 0.85 {
            for _ in 0..3 {
                let tip = self.near_buf[film.tips[self.rng.index(film.tips.len())]];
                let vel = [-self.rng.range(0.3, 0.7), self.rng.range(0.05, 0.2)];
                self.spark(film.project(tip, pose.tilt, sc, center), vel, 0.0);
            }
        }
        if u < 0.8 {
            for _ in 0..2 {
                let tail = film.tail[self.rng.index(film.tail.len())];
                let vel = [-self.rng.range(0.1, 0.4), self.rng.range(-0.03, 0.03)];
                self.spark(film.project(tail, pose.tilt, sc, center), vel, 0.0);
            }
        }
        Bird {
            body,
            far,
            color: None,
            landed: false,
        }
    }

    /// Advances the wingbeat by one frame of the side-on pass.
    fn beat_wings(&mut self, t: f64, u: f64, climb: f64, flare: f64, hover: f64) {
        let phase = self.phase;
        if t > TOUCHDOWN {
            // After touchdown the wings complete their wingbeat and stop at its
            // top. The phase slows down toward the end.
            let fold = *self.fold.get_or_insert(PhaseRamp {
                from: phase,
                to: next_rest(phase),
                start: TOUCHDOWN,
            });
            let k = ((t - fold.start) / FOLD_SECS).min(1.0);
            self.phase = fold.from + (fold.to - fold.from) * (1.0 - (1.0 - k).powi(3));
            self.amp *= (-DT / 0.3).exp();
        } else if u >= DESCEND_FROM {
            // The descent has one full wingbeat. The wingbeat starts slowly, is
            // fastest in the downstroke, and ends at its top at touchdown.
            let beat = *self.descent.get_or_insert(PhaseRamp {
                from: phase,
                to: next_rest(phase) + 2.0 * PI,
                start: t,
            });
            let s = ((t - beat.start) / (TOUCHDOWN - beat.start)).clamp(0.0, 1.0);
            self.phase = beat.from + (beat.to - beat.from) * s * s * (3.0 - 2.0 * s);
            self.amp = lowpass(self.amp, DESCEND_AMP, 0.15);
        } else {
            // The wingbeat follows the effort of the bird. It is fast and wide in
            // a climb, narrow in a dive, wide in the flare, and fast and narrow in
            // the hover.
            let freq = 2.0 + 1.3 * climb.max(0.0) + 1.6 * flare + 1.8 * hover;
            let cruise = 0.45 + 0.55 * climb.max(0.0) - 0.3 * (-climb).max(0.0);
            let target = (cruise * (1.0 - flare) + flare) * (1.0 - hover) + HOVER_AMP * hover;
            self.amp = lowpass(self.amp, target.max(0.15), 0.12);
            self.phase += 2.0 * PI * freq * DT;
        }
    }

    /// Advances the landing by one frame. Returns the height, the pitch and the
    /// tail bend of the body.
    fn land(&mut self, t: f64, u: f64, hover: f64, rot_target: f64) -> (f64, f64, f64) {
        let touched = t > TOUCHDOWN;
        // The springs start from the current pose of the body.
        let s = self.landing.get_or_insert(Springs {
            y: self.pose.lift,
            vy: 0.0,
            pitch: self.rot,
            vpitch: 0.0,
            bend: 0.0,
            vbend: 0.0,
            glide_from: None,
        });

        // The tail bends down in the hover and the descent. After touchdown the
        // tail returns to level.
        let bend_target = if touched { 0.0 } else { TAIL_BRAKE * hover };
        s.vbend += spring(s.bend, s.vbend, bend_target, TAIL_W, TAIL_Z) * DT;
        s.bend += s.vbend * DT;

        // From the start of the descent, the pitch returns to level with a
        // damping ratio of 1, so that the body does not rock.
        let (pitch_target, pitch_z) = if self.descent.is_some() {
            (0.0, SETTLE_Z)
        } else {
            (rot_target, PITCH_Z)
        };
        s.vpitch += spring(s.pitch, s.vpitch, pitch_target, PITCH_W, pitch_z) * DT;
        s.pitch += s.vpitch * DT;

        if touched {
            // After touchdown the height settles at the perch without a bounce.
            s.vy += spring(s.y, s.vy, 0.0, SETTLE_W, SETTLE_Z) * DT;
            s.y += s.vy * DT;
        } else if let Some(beat) = self.descent {
            // In the descent the body follows a cubic curve down to the perch. The
            // curve starts with no downward velocity and ends with no velocity.
            let (y0, v0) = *s.glide_from.get_or_insert((s.y, s.vy.min(0.0)));
            let span = TOUCHDOWN - beat.start;
            let x = ((t - beat.start) / span).clamp(0.0, 1.0);
            let (y, slope) = ease_to_zero(y0, v0 * span, x);
            s.y = y;
            s.vy = slope / span;
        } else {
            // In the hover each downstroke pushes the body up, and the body sinks
            // between downstrokes.
            let push = -WING_PUSH * self.amp * (self.phase.sin().max(0.0) - 1.0 / PI);
            let target = -HOVER_HEIGHT * smooth(0.76, 0.86, u);
            s.vy += (spring(s.y, s.vy, target, HEIGHT_W, HEIGHT_Z) + push) * DT;
            s.y += s.vy * DT;
        }
        (s.y, s.pitch, s.bend)
    }

    /// Returns the frame of the final wingbeat at the progress `tau`, from 0
    /// to 1.
    pub(super) fn flap_frame(&mut self, film: &Film, tau: f64) -> Vec<String> {
        // The tip of each wing is behind its hinge, so that the wing bends as it moves.
        film.near.pose_with(
            FLAP_LOW,
            |u| flap_k_at(tau, 0.0, NEAR_LAG, u),
            &mut self.near_buf,
        );
        film.far.pose_with(
            FLAP_LOW,
            |u| flap_k_at(tau, FAR_DELAY, FAR_LAG, u),
            &mut self.far_buf,
        );
        // The downstroke lifts the body by up to 3 dots. The body returns as the
        // wings return.
        let lift = -3.0 * (PI * ((tau - 0.38) / 0.55).clamp(0.0, 1.0)).sin();
        let center = [film.perch[0], film.perch[1] + lift];
        let body = film.raster(film.statics.iter().chain(&self.near_buf), 0.0, 1.0, center);
        let far = film.raster(self.far_buf.iter(), 0.0, 1.0, center);

        // In the downstroke, dust starts at the tail and moves back.
        if (0.3..0.62).contains(&tau) {
            for _ in 0..3 {
                let tail = film.tail[self.rng.index(film.tail.len())];
                let pos = film.project(tail, 0.0, 1.0, center);
                let vel = [-self.rng.range(0.5, 1.3), self.rng.range(-0.05, 0.25)];
                // Dust starts at the age 7, after the Violet range of
                // `SPARK_COLORS`. Its colors go from Steel to Stone.
                self.spark(pos, vel, 7.0);
            }
        }
        self.step_sparks(0.96);

        let shot = Shot {
            body: &body,
            far: &far,
            bird_color: None,
            landed: true,
            word: Word::Full,
            marks: 4,
            hint: false,
        };
        film.compose(&shot, &self.sparks, &[])
    }

    fn spark(&mut self, pos: Pt, vel: Pt, age: f64) {
        self.sparks.push(Particle {
            pos,
            vel,
            prev: pos,
            age,
        });
    }

    /// Advances the sparks by one frame and removes the sparks that are too
    /// old. Each spark keeps the fraction `drag` of its velocity.
    fn step_sparks(&mut self, drag: f64) {
        for s in &mut self.sparks {
            s.pos = [s.pos[0] + s.vel[0], s.pos[1] + s.vel[1]];
            s.vel = [s.vel[0] * drag, s.vel[1] * drag];
            s.age += 1.0;
        }
        self.sparks
            .retain(|s| s.age < SPARK_COLORS[SPARK_COLORS.len() - 1].0);
    }

    /// Advances the streaks by one frame and removes the streaks that are too
    /// old. Each streak accelerates.
    fn step_streaks(&mut self) {
        for s in &mut self.streaks {
            s.prev = s.pos;
            s.pos = [s.pos[0] + s.vel[0], s.pos[1] + s.vel[1]];
            s.vel = [s.vel[0] * 1.2, s.vel[1] * 1.2];
            s.age += 1.0;
        }
        self.streaks
            .retain(|s| s.age < STREAK_COLORS[STREAK_COLORS.len() - 1].0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::splash::color::ColorMode;
    use crate::splash::test_util::plain;

    #[test]
    fn whole_film_renders_and_ends_on_the_final_frame() {
        let film = Film::new(80, ColorMode::TrueColor);
        let mut reel = Reel::new(7);
        let mut last = Vec::new();
        for i in 0..Reel::frame_count() {
            last = reel.frame(&film, i);
            assert_eq!(last.len(), film.h);
        }
        assert_eq!(plain(&last), plain(&film.final_frame()));
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
        // The wing position changes by less than a fifth of its range between two steps.
        let jump = ks
            .windows(2)
            .map(|w| (w[1] - w[0]).abs())
            .fold(0.0, f64::max);
        assert!(jump < 0.2 * (FLAP_TOP - FLAP_LOW), "jerky beat: {jump}");

        // Each point of each wing, from the hinge to the tip, starts and ends in the logo pose.
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

        // The last frame of the final wingbeat shows the landed logo.
        let film = Film::new(80, ColorMode::Off);
        let mut reel = Reel::new(3);
        let last = reel.flap_frame(&film, 1.0);
        let end = plain(&last);
        let still = plain(&film.final_frame());
        let differ: usize = end
            .iter()
            .zip(&still)
            .map(|(a, b)| a.chars().zip(b.chars()).filter(|(x, y)| x != y).count())
            .sum();
        // The moving bird comes from sample points and the landed logo comes from
        // the Lanczos filter, so a few cells at the edges can differ.
        assert!(differ <= 12, "{differ} cells differ from the still logo");
    }

    #[test]
    fn bird_hovers_then_settles_onto_the_words() {
        let film = Film::new(80, ColorMode::Off);
        let mut reel = Reel::new(11);
        // The extremes of the body height, the pitch and the tail bend in the
        // landing. `Pose` gives the units and the signs.
        let (mut high, mut nose_up) = (0.0f64, 0.0f64);
        // The largest nose-down pitch from the start of the descent, and the
        // largest depth below the perch after touchdown.
        let (mut nose_down, mut dip) = (f64::MIN, f64::MIN);
        let (mut tail_down, mut tail_flick) = (0.0f64, f64::MIN);
        // The body height in each frame of the descent.
        let mut glide = Vec::new();
        let mut at_rest = None;
        let mut closed = None;
        // The wingbeat phase at the start of the descent and at touchdown.
        let (mut descent_from, mut at_touch) = (None, 0.0);
        let mut descent_amp = f64::MAX;
        for i in 0..Reel::frame_count() {
            let t = i as f64 / FPS;
            reel.frame(&film, i);
            let (y, pitch, bend) = (reel.pose.lift, reel.pose.tilt, reel.pose.bend);
            let u = (t - PASS_START) / (TOUCHDOWN - PASS_START);
            if (0.86..1.0).contains(&u) {
                high = high.min(y);
                nose_up = nose_up.min(pitch);
                tail_down = tail_down.min(bend);
            }
            if u >= DESCEND_FROM && t <= TOUCHDOWN + SETTLED_SECS {
                nose_down = nose_down.max(pitch);
            }
            if u >= DESCEND_FROM && t <= TOUCHDOWN {
                glide.push(y);
                descent_from.get_or_insert(reel.phase);
                at_touch = reel.phase;
                if u >= 0.92 {
                    descent_amp = descent_amp.min(reel.amp);
                }
            }
            if t > TOUCHDOWN && t <= TOUCHDOWN + SETTLED_SECS {
                dip = dip.max(y);
                tail_flick = tail_flick.max(bend);
                at_rest = Some((y, pitch, bend));
            }
            if t > TOUCHDOWN + FOLD_SECS && closed.is_none() {
                closed = Some(reel.phase);
            }
        }
        assert!(high <= -1.5, "no hover above the perch: highest {high:.2}");
        assert!(
            nose_up >= -0.05,
            "body tips back on the descent: {nose_up:.2}"
        );
        // The body returns to level without a rock forward, and does not sink
        // below the perch.
        assert!(nose_down <= 0.02, "body rocks forward: {nose_down:.3}");
        assert!(dip <= 0.05, "body dips on touchdown: {dip:.2}");
        // From the top of the hover, the bird moves down in each frame, by half a
        // dot at most.
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
        // The descent has about one wingbeat, and the wingbeat is wide.
        let beats = (at_touch - descent_from.unwrap()) / (2.0 * PI);
        assert!(
            (0.9..2.0).contains(&beats),
            "descent is {beats:.2} beats, not one"
        );
        assert!(
            descent_amp >= 0.7,
            "descent beat too shallow: {descent_amp:.2}"
        );
        // The tail bends down in the descent and returns to level without a swing
        // above level.
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
        // The wings stop at a phase that is a multiple of 2 pi, the top of a wingbeat.
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
            let t = i as f64 / FPS;
            reel.frame(&film, i);
            let u = (t - PASS_START) / (TOUCHDOWN - PASS_START);
            if (0.0..0.7).contains(&u) {
                left = left.min(reel.pose.sway);
                right = right.max(reel.pose.sway);
            }
            if u >= DESCEND_FROM && t <= TOUCHDOWN + SETTLED_SECS {
                landing = landing.max(reel.pose.sway.abs());
            }
        }
        // The tip moves in both directions, by about `TAIL_SWAY`.
        assert!(
            left <= -0.8 * TAIL_SWAY && right >= 0.8 * TAIL_SWAY,
            "tail does not sway: {left:.3}..{right:.3}"
        );
        assert!(landing < 1e-9, "tail still sways on landing: {landing:.3}");
    }

    #[test]
    fn the_same_seed_gives_the_same_film() {
        let film = Film::new(80, ColorMode::Off);
        let render = |seed| {
            let mut reel = Reel::new(seed);
            let frames = (0..Reel::frame_count()).map(|i| reel.frame(&film, i));
            frames.collect::<Vec<_>>()
        };
        let first = render(5);
        assert!(first == render(5), "two films with one seed differ");
        // Seed 0 is valid. Its particles differ from the particles of seed 5.
        assert!(first != render(0), "two seeds give one film");
    }

    #[test]
    fn each_frame_fills_the_frame_rows() {
        for cols in [60, 100] {
            let film = Film::new(cols, ColorMode::Off);
            let mut reel = Reel::new(9);
            let check = |frame: Vec<String>| {
                assert_eq!(frame.len(), film.h);
                assert!(
                    frame.iter().all(|row| row.chars().count() == cols),
                    "ragged frame at {cols} columns"
                );
            };
            for i in 0..Reel::frame_count() {
                check(reel.frame(&film, i));
            }
            for step in 0..=10 {
                check(reel.flap_frame(&film, step as f64 / 10.0));
            }
        }
    }

    #[test]
    fn random_numbers_stay_in_their_ranges() {
        let mut rng = Rng(1);
        for _ in 0..10_000 {
            let (x, y, i) = (rng.next(), rng.range(2.0, 5.0), rng.index(7));
            assert!(
                (0.0..1.0).contains(&x) && (2.0..5.0).contains(&y) && i < 7,
                "{x} {y} {i}"
            );
        }
        assert_eq!(rng.index(1), 0);
    }

    #[test]
    fn landing_math_matches_its_definitions() {
        assert_eq!(lowpass(0.0, 10.0, 0.25), 2.5);
        // A spring on its target with no velocity has no acceleration.
        assert_eq!(spring(1.0, 0.0, 1.0, 9.0, 1.0), 0.0);
        assert!(
            spring(0.0, 0.0, 1.0, 9.0, 1.0) > 0.0,
            "no pull to the target"
        );
        assert!(
            spring(1.0, 2.0, 1.0, 9.0, 1.0) < 0.0,
            "damping does not oppose the velocity"
        );
        assert_eq!(ease_to_zero(-2.5, 4.0, 0.0), (-2.5, 4.0));
        assert_eq!(ease_to_zero(-2.5, 4.0, 1.0), (0.0, 0.0));
        for (phase, top) in [(0.0, 0.0), (1.0, 2.0 * PI), (2.0 * PI, 2.0 * PI)] {
            assert_eq!(next_rest(phase), top, "phase {phase}");
        }
    }

    #[test]
    fn skip_hint_shows_until_touchdown() {
        let film = Film::new(80, ColorMode::Off);
        let mut reel = Reel::new(1);
        for i in 0..Reel::frame_count() {
            let frame = reel.frame(&film, i).concat();
            let before_touchdown = (i as f64 / FPS) < TOUCHDOWN;
            assert_eq!(
                frame.contains("press any key to skip"),
                before_touchdown,
                "frame {i}"
            );
        }
    }

    #[test]
    fn registration_marks_appear_one_after_the_other() {
        let film = Film::new(80, ColorMode::Off);
        let mut reel = Reel::new(1);
        let marks: Vec<usize> = (0..12)
            .map(|i| reel.frame(&film, i).concat().matches('+').count())
            .collect();
        assert_eq!((marks[0], marks[11]), (0, 4), "{marks:?}");
        assert!(
            marks.windows(2).all(|w| w[1] == w[0] || w[1] == w[0] + 1),
            "{marks:?}"
        );
    }
}
