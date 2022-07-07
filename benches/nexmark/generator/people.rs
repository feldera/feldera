//! Generates people for the Nexmark streaming data source.
//!
//! API based on the equivalent [Nexmark Flink PersonGenerator API](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/model/PersonGenerator.java).

use super::NexmarkGenerator;
use crate::config;
use crate::model::{DateTime, Id, Person};
use rand::{seq::SliceRandom, Rng};
use std::cmp::min;
use std::time::Duration;

// Keep the number of states small so that the example queries will find
// results even with a small batch of events.
const US_STATES: &[&str] = &["AZ", "CA", "ID", "OR", "WA", "WY"];

const US_CITIES: &[&str] = &[
    "Phoenix",
    "Los Angeles",
    "San Francisco",
    "Boise",
    "Portland",
    "Bend",
    "Redmond",
    "Seattle",
    "Kent",
    "Cheyenne",
];

const FIRST_NAMES: &[&str] = &[
    "Peter", "Paul", "Luke", "John", "Saul", "Vicky", "Kate", "Julie", "Sarah", "Deiter", "Walter",
];

const LAST_NAMES: &[&str] = &[
    "Shultz", "Abrams", "Spencer", "White", "Bartels", "Walton", "Smith", "Jones", "Noris",
];

impl<R: Rng> NexmarkGenerator<R> {
    // Generate and return a random person with next available id.
    pub fn next_person(&mut self, next_event_id: Id, timestamp: u64) -> Person {
        // TODO(absoludity): Figure out the purpose of the extra field - appears to be
        // aiming to adjust the number of bytes for the record to be an average, which will
        // need slightly different handling in Rust.
        // int currentSize =
        //     8 + name.length() + email.length() + creditCard.length() + city.length() + state.length();
        // String extra = nextExtra(random, currentSize, config.getAvgPersonByteSize());

        Person {
            id: self.last_base0_person_id(next_event_id) + config::FIRST_PERSON_ID,
            name: self.next_person_name(),
            email_address: self.next_email(),
            credit_card: self.next_credit_card(),
            city: self.next_us_city(),
            state: self.next_us_state(),
            date_time: DateTime::UNIX_EPOCH + Duration::from_millis(timestamp),
            extra: String::new(),
        }
    }

    /// Return a random person id (base 0).
    ///
    /// Choose a random person from any of the 'active' people, plus a few 'leads'.
    /// By limiting to 'active' we ensure the density of bids or auctions per person
    /// does not decrease over time for long running jobs.  By choosing a person id
    /// ahead of the last valid person id we will make newPerson and newAuction
    /// events appear to have been swapped in time.
    ///
    /// NOTE: The above is the original comment from the Java implementation. The
    /// "base 0" is referring to the fact that the returned Id is not including the
    /// FIRST_PERSON_ID offset, and should really be "offset 0".
    pub fn next_base0_person_id(&mut self, event_id: Id) -> Id {
        let num_people = self.last_base0_person_id(event_id) + 1;
        let active_people = min(num_people, config::NUM_ACTIVE_PEOPLE);
        let n = self
            .rng
            .gen_range(0..(active_people + config::PERSON_ID_LEAD));
        num_people - active_people + n
    }

    /// Return the last valid person id (ignoring FIRST_PERSON_ID). Will be the
    /// current person id if due to generate a person.
    pub fn last_base0_person_id(&self, event_id: Id) -> Id {
        let epoch = event_id / self.config.total_proportion();
        let mut offset = event_id % self.config.total_proportion();

        if offset >= self.config.person_proportion {
            // About to generate an auction or bid.
            // Go back to the last person generated in this epoch.
            offset = self.config.person_proportion - 1;
        }
        // About to generate a person.
        epoch * self.config.person_proportion + offset
    }

    // Return a random US state.
    fn next_us_state(&mut self) -> String {
        US_STATES.choose(&mut self.rng).unwrap().to_string()
    }

    // Return a random US city.
    fn next_us_city(&mut self) -> String {
        US_CITIES.choose(&mut self.rng).unwrap().to_string()
    }

    // Return a random person name.
    fn next_person_name(&mut self) -> String {
        format!(
            "{} {}",
            FIRST_NAMES.choose(&mut self.rng).unwrap(),
            LAST_NAMES.choose(&mut self.rng).unwrap()
        )
    }

    // Return a random email address.
    fn next_email(&mut self) -> String {
        format!("{}@{}.com", self.next_string(7), self.next_string(5))
    }

    // Return a random credit card number.
    fn next_credit_card(&mut self) -> String {
        format!(
            "{:04} {:04} {:04} {:04}",
            &mut self.rng.gen_range(0..10_000),
            &mut self.rng.gen_range(0..10_000),
            &mut self.rng.gen_range(0..10_000),
            &mut self.rng.gen_range(0..10_000)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use rand::rngs::mock::StepRng;

    fn make_default_config() -> Config {
        Config {
            person_proportion: 1,
            auction_proportion: 3,
            bid_proportion: 46,
        }
    }

    #[test]
    fn test_next_person() {
        let mut ng = NexmarkGenerator {
            rng: StepRng::new(0, 5),
            config: make_default_config(),
        };

        let p = ng.next_person(105, 1_000_000_000_000);

        assert_eq!(
            p,
            Person {
                id: 1002,
                name: "Peter Shultz".into(),
                email_address: "AAA@AAA.com".into(),
                credit_card: "0000 0000 0000 0000".into(),
                city: "Phoenix".into(),
                state: "AZ".into(),
                date_time: DateTime::UNIX_EPOCH + Duration::from_millis(1_000_000_000_000),
                extra: String::new(),
            }
        );
    }

    #[test]
    fn test_next_base0_person_id() {
        let mut ng = NexmarkGenerator {
            rng: StepRng::new(0, 5),
            config: make_default_config(),
        };

        // When one more than the last person id is less than the configured
        // active people (1000), the id returned is a random id from one of
        // the currently active people plus the 'lead' people.
        // Note: the mock rng is always returning zero for the random addition
        // in the range (0..active_people).
        assert_eq!(ng.next_base0_person_id(50 * 998), 0);

        // Even when one more than the last person id is equal to the configured
        // active people, the id returned is a random id from one of the
        // active people plus the 'lead' people.
        assert_eq!(ng.next_base0_person_id(50 * 999), 0);

        // When one more than the last person id is one greater than the
        // configured active people, we consider the most recent
        // NUM_ACTIVE_PEOPLE to be the active ones, and return a random id from
        // those plus the 'lead'people.
        assert_eq!(ng.next_base0_person_id(50 * 1000), 1);

        // When one more than the last person id is 501 greater than the
        // configured active people, we consider the most recent
        // NUM_ACTIVE_PEOPLE to be the active ones, and return a random id from
        // those plus the 'lead' people.
        assert_eq!(ng.next_base0_person_id(50 * 1500), 501);
    }

    #[test]
    fn test_last_base0_person_id_default() {
        let ng = NexmarkGenerator {
            rng: StepRng::new(0, 5),
            config: make_default_config(),
        };

        // With the default config, the first 50 events will only include one
        // person
        assert_eq!(ng.last_base0_person_id(25), 0);

        // The 50th event will correspond to the next...
        assert_eq!(ng.last_base0_person_id(50), 1);
        assert_eq!(ng.last_base0_person_id(75), 1);

        // And so on...
        assert_eq!(ng.last_base0_person_id(100), 2);
    }

    #[test]
    fn test_last_base0_person_id_custom() {
        // Set the configured bid proportion to 21,
        // which together with the other defaults for person and auction
        // proportion, makes the total 25.
        let ng = NexmarkGenerator {
            rng: StepRng::new(0, 5),
            config: Config {
                bid_proportion: 21,
                ..make_default_config()
            },
        };

        // With the total proportion at 25, there will be a new person
        // at every 25th event.
        assert_eq!(ng.last_base0_person_id(25), 1);
        assert_eq!(ng.last_base0_person_id(50), 2);
        assert_eq!(ng.last_base0_person_id(75), 3);
        assert_eq!(ng.last_base0_person_id(100), 4);
    }

    #[test]
    fn test_next_us_state() {
        let mut ng = NexmarkGenerator {
            rng: StepRng::new(0, 5),
            config: make_default_config(),
        };

        let s = ng.next_us_state();

        assert_eq!(s, "AZ");
    }

    #[test]
    fn test_next_us_city() {
        let mut ng = NexmarkGenerator {
            rng: StepRng::new(0, 5),
            config: make_default_config(),
        };

        let c = ng.next_us_city();

        assert_eq!(c, "Phoenix");
    }

    #[test]
    fn test_next_person_name() {
        let mut ng = NexmarkGenerator {
            rng: StepRng::new(0, 5),
            config: make_default_config(),
        };

        let n = ng.next_person_name();

        assert_eq!(n, "Peter Shultz");
    }

    #[test]
    fn test_next_email() {
        let mut ng = NexmarkGenerator {
            rng: StepRng::new(0, 5),
            config: make_default_config(),
        };

        let e = ng.next_email();

        assert_eq!(e, "AAA@AAA.com");
    }

    #[test]
    fn test_next_credit_card() {
        let mut ng = NexmarkGenerator {
            rng: StepRng::new(0, 5),
            config: make_default_config(),
        };

        let e = ng.next_credit_card();

        assert_eq!(e, "0000 0000 0000 0000");
    }
}
