use dbsp::algebra::{F32, F64};
use num_traits::{CheckedMul, CheckedSub};
use rust_decimal::Decimal;

const OVERFLOW: &str = "arithmetic overflow";

pub trait SQLArithmetic {
    fn plus(&self, other: &Self) -> Self;
    fn minus(&self, other: &Self) -> Self;
    fn mul(&self, other: &Self) -> Self;
}

impl SQLArithmetic for i8 {
    fn plus(&self, other: &Self) -> Self {
        self.checked_add(*other).expect(OVERFLOW)
    }

    fn minus(&self, other: &Self) -> Self {
        self.checked_sub(other).expect(OVERFLOW)
    }

    fn mul(&self, other: &Self) -> Self {
        self.checked_mul(other).expect(OVERFLOW)
    }
}

impl SQLArithmetic for i16 {
    fn plus(&self, other: &Self) -> Self {
        self.checked_add(*other).expect(OVERFLOW)
    }

    fn minus(&self, other: &Self) -> Self {
        self.checked_sub(other).expect(OVERFLOW)
    }

    fn mul(&self, other: &Self) -> Self {
        self.checked_mul(other).expect(OVERFLOW)
    }
}

impl SQLArithmetic for i32 {
    fn plus(&self, other: &Self) -> Self {
        self.checked_add(*other).expect(OVERFLOW)
    }

    fn minus(&self, other: &Self) -> Self {
        self.checked_sub(other).expect(OVERFLOW)
    }

    fn mul(&self, other: &Self) -> Self {
        self.checked_mul(other).expect(OVERFLOW)
    }
}

impl SQLArithmetic for i64 {
    fn plus(&self, other: &Self) -> Self {
        self.checked_add(*other).expect(OVERFLOW)
    }

    fn minus(&self, other: &Self) -> Self {
        self.checked_sub(other).expect(OVERFLOW)
    }

    fn mul(&self, other: &Self) -> Self {
        self.checked_mul(other).expect(OVERFLOW)
    }
}

impl SQLArithmetic for F32 {
    fn plus(&self, other: &Self) -> Self {
        self + other
    }

    fn minus(&self, other: &Self) -> Self {
        self - other
    }

    fn mul(&self, other: &Self) -> Self {
        self * other
    }
}

impl SQLArithmetic for F64 {
    fn plus(&self, other: &Self) -> Self {
        self + other
    }

    fn minus(&self, other: &Self) -> Self {
        self - other
    }

    fn mul(&self, other: &Self) -> Self {
        self * other
    }
}

impl SQLArithmetic for Decimal {
    fn plus(&self, other: &Self) -> Self {
        self + other
    }

    fn minus(&self, other: &Self) -> Self {
        self - other
    }

    fn mul(&self, other: &Self) -> Self {
        self * other
    }
}
