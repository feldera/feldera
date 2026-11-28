//! Reproduce the ARM64 HotSpot Math.sin and Math.cos used by the Janino oracle.
//! Its fused multiply-adds and its choice of cosine correction from the
//! original angle are observable at the S1 literal boundary.

// Adapted from fdlibm k_sin.c, k_cos.c, e_rem_pio2.c and k_rem_pio2.c.
// Copyright (C) 1993 by Sun Microsystems, Inc. All rights reserved.
// Developed at SunSoft, a Sun Microsystems, Inc. business.
// Permission to use, copy, modify, and distribute this software is freely
// granted, provided that this notice is preserved.

use num_bigint::BigUint;
use num_traits::ToPrimitive;

pub(crate) fn sin(x: f64) -> f64 {
    evaluate(x, false)
}

pub(crate) fn cos(x: f64) -> f64 {
    evaluate(x, true)
}

fn evaluate(x: f64, cosine: bool) -> f64 {
    if !x.is_finite() {
        return f64::NAN;
    }
    // fdlibm selects its primary interval by the high word of the argument.
    let word = x.abs().to_bits() >> 32;
    if word < 0x3e40_0000 {
        return if cosine { 1.0 } else { x };
    }
    if word <= 0x3fe9_21fb {
        return if cosine {
            kernel_cos(x, 0.0, word)
        } else {
            kernel_sin(x, None)
        };
    }
    let (quadrant, high, low) = reduce(x.abs());
    let value = match (quadrant + u8::from(cosine)) % 4 {
        0 => kernel_sin(high, Some(low)),
        1 => kernel_cos(high, low, word),
        2 => -kernel_sin(high, Some(low)),
        _ => -kernel_cos(high, low, word),
    };
    if x.is_sign_negative() && !cosine {
        -value
    } else {
        value
    }
}

// fdlibm reduces a positive angle to x - n*pi/2. The kernel uses the
// quadrant and both parts of the remainder to round its final polynomial.
fn reduce(x: f64) -> (u8, f64, f64) {
    const PI_PARTS: [(f64, f64); 3] = [
        (1.5707963267341256, 6.077100506506192e-11),
        (6.077100506303966e-11, 2.0222662487959506e-21),
        (2.0222662487111665e-21, 8.4784276603689e-32),
    ];
    let word = x.to_bits() >> 32;
    if word > 0x4139_21fb {
        return reduce_large(x);
    }
    let n = x.mul_add(std::f64::consts::FRAC_2_PI, 0.5).trunc();
    let (first, tail) = PI_PARTS[0];
    let mut head = (-n).mul_add(first, x);
    let mut correction = n * tail;
    let mut high = head - correction;
    for ((part, tail), lost_bits) in PI_PARTS[1..].iter().zip([16, 49]) {
        if (word >> 20) <= ((high.abs().to_bits() >> 52) + lost_bits) {
            break;
        }
        let product = n * part;
        let next = head - product;
        correction = n.mul_add(*tail, product - (head - next));
        head = next;
        high = head - correction;
    }
    ((n as u32 % 4) as u8, high, (head - high) - correction)
}

const TWO_OVER_PI: [u32; 66] = [
    0xA2F983, 0x6E4E44, 0x1529FC, 0x2757D1, 0xF534DD, 0xC0DB62, 0x95993C, 0x439041, 0xFE5163,
    0xABDEBB, 0xC561B7, 0x246E3A, 0x424DD2, 0xE00649, 0x2EEA09, 0xD1921C, 0xFE1DEB, 0x1CB129,
    0xA73EE8, 0x8235F5, 0x2EBB44, 0x84E99C, 0x7026B4, 0x5F7E41, 0x3991D6, 0x398353, 0x39F49C,
    0x845F8B, 0xBDF928, 0x3B1FF8, 0x97FFDE, 0x05980F, 0xEF2F11, 0x8B5A0A, 0x6D1F6D, 0x367ECF,
    0x27CB09, 0xB74F46, 0x3F669E, 0x5FEA2D, 0x7527BA, 0xC7EBE5, 0xF17B3D, 0x0739F7, 0x8A5292,
    0xEA6BFB, 0x5FB11F, 0x8D5D08, 0x560330, 0x46FC7B, 0x6BABF0, 0xCFBC20, 0x9AF436, 0x1DA9E3,
    0x91615E, 0xE61B08, 0x659985, 0x5F14A0, 0x68408D, 0xFFD880, 0x4D7327, 0x310606, 0x1556CA,
    0x73A8C9, 0x60E27B, 0xC08C6B,
];
const PI_OVER_TWO_PARTS: [f64; 5] = [
    1.570796251296997,
    7.549789415861596e-8,
    5.390302529957765e-15,
    3.282003415807913e-22,
    1.270655753080676e-29,
];

// Payne–Hanek reduction. Form the relevant digits of x*2/pi in radix 2^24.
// Earlier digits contribute only multiples of four. Five significant digits
// of the fractional remainder suffice for fdlibm's double-precision mode.
fn reduce_large(x: f64) -> (u8, f64, f64) {
    let exponent = (x.to_bits() >> 52) as i32 - 1023 - 23;
    let first = ((exponent - 3) / 24).max(0);
    let top_exponent = exponent - 24 * (first + 1);
    let mantissa = (x.to_bits() & ((1 << 52) - 1)) | (1 << 52);
    let mask = (1_u64 << 24) - 1;
    let digits = [
        mantissa >> 29,
        (mantissa >> 5) & mask,
        (mantissa << 19) & mask,
    ];
    let mut product = BigUint::ZERO;
    for i in 0.. {
        let mut digit = 0;
        for (j, x_digit) in digits.iter().enumerate() {
            let offset = first + i - j as i32;
            if offset >= 0 {
                digit += x_digit * u64::from(TWO_OVER_PI[offset as usize]);
            }
        }
        product = (product << 24) + digit;
        if i < 4 {
            continue;
        }
        let scale = (24 * i - top_exponent) as usize;
        let unit = BigUint::from(1u8) << scale;
        let whole = &product >> scale;
        let fraction = &product & (&unit - 1u8);
        let round_up = fraction.bit(scale as u64 - 1);
        let remainder = if round_up { unit - fraction } else { fraction };
        if remainder.bits() <= 4 * 24 {
            continue;
        }
        let quadrant = ((whole + u8::from(round_up)) & BigUint::from(3u8))
            .to_u8()
            .expect("quadrant");
        let (hi, lo) = radians(remainder, scale);
        return if round_up {
            (quadrant, -hi, -lo)
        } else {
            (quadrant, hi, lo)
        };
    }
    unreachable!("a finite nonzero binary float is not a multiple of pi/2")
}

fn radians(mut fraction: BigUint, scale: usize) -> (f64, f64) {
    let mut digits = Vec::new();
    let mask = BigUint::from(0xff_ffffu32);
    while fraction != BigUint::ZERO {
        let digit = (&fraction & &mask).to_u32().expect("24-bit digit");
        digits.push(libm::scalbn(
            f64::from(digit),
            24 * digits.len() as i32 - scale as i32,
        ));
        fraction >>= 24;
    }
    digits.reverse();
    let mut terms = Vec::with_capacity(digits.len());
    for end in 0..digits.len() {
        let term = PI_OVER_TWO_PARTS
            .iter()
            .zip(digits[..=end].iter().rev())
            .fold(0.0, |sum, (pi, digit)| pi.mul_add(*digit, sum));
        terms.push(term);
    }
    let high = terms.iter().rev().sum();
    let low = terms[1..]
        .iter()
        .fold(terms[0] - high, |sum, term| sum + term);
    (high, low)
}

fn kernel_sin(x: f64, tail: Option<f64>) -> f64 {
    let z = x * x;
    let v = z * x;
    let [s1, s2, s3, s4, s5, s6] = [
        -0.16666666666666632,
        0.00833333333332249,
        -0.0001984126982985795,
        2.7557313707070068e-06,
        -2.5050760253406863e-08,
        1.58969099521155e-10,
    ];
    let r = [s5, s4, s3, s2]
        .into_iter()
        .fold(s6, |r, c| z.mul_add(r, c));
    match tail {
        None => v.mul_add(z.mul_add(r, s1), x),
        Some(y) => {
            let correction = (-v).mul_add(r, 0.5 * y);
            x + v.mul_add(s1, (-z).mul_add(correction, y))
        }
    }
}

fn kernel_cos(x: f64, y: f64, original_word: u64) -> f64 {
    let z = x * x;
    let [c1, c2, c3, c4, c5, c6] = [
        0.0416666666666666,
        -0.001388888888887411,
        2.480158728947673e-05,
        -2.7557314351390663e-07,
        2.087572321298175e-09,
        -1.1359647557788195e-11,
    ];
    let r = [c5, c4, c3, c2, c1]
        .into_iter()
        .fold(c6, |r, c| z.mul_add(r, c));
    let correction = (z * z).mul_add(r, -(x * y));
    if original_word <= 0x3fd3_3333 {
        1.0 - z.mul_add(0.5, -correction)
    } else {
        let quarter = if original_word > 0x3fe9_0000 {
            0.28125
        } else {
            f64::from_bits((original_word - 0x0020_0000) << 32)
        };
        (1.0 - quarter) - (z.mul_add(0.5, -quarter) - correction)
    }
}
